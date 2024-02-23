// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <sqltypes.h>

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

#include "common/logging.h"
#include "concurrentqueue.h"
#include "gutil/integral_types.h"
#include "pipeline/exec/data_queue.h"
#include "pipeline/exec/multi_cast_data_streamer.h"
#include "pipeline/exec/operator.h"
#include "vec/common/hash_table/hash_map_context_creator.h"
#include "vec/common/sort/partition_sorter.h"
#include "vec/common/sort/sorter.h"
#include "vec/core/types.h"
#include "vec/exec/join/process_hash_table_probe.h"
#include "vec/exec/join/vhash_join_node.h"
#include "vec/exec/vaggregation_node.h"
#include "vec/exec/vanalytic_eval_node.h"
#include "vec/exec/vpartition_sort_node.h"
#include "vec/exec/vset_operation_node.h"
#include "vec/spill/spill_stream.h"

namespace doris::pipeline {

class Dependency;
class AnalyticSourceDependency;
class AnalyticSinkDependency;
class PipelineXTask;
struct BasicSharedState;
using DependencySPtr = std::shared_ptr<Dependency>;
using DependencyMap = std::map<int, std::vector<DependencySPtr>>;

static constexpr auto SLOW_DEPENDENCY_THRESHOLD = 60 * 1000L * 1000L * 1000L;
static constexpr auto TIME_UNIT_DEPENDENCY_LOG = 30 * 1000L * 1000L * 1000L;
static_assert(TIME_UNIT_DEPENDENCY_LOG < SLOW_DEPENDENCY_THRESHOLD);

struct BasicSharedState {
    template <class TARGET>
    TARGET* cast() {
        DCHECK(dynamic_cast<TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<TARGET*>(this);
    }
    template <class TARGET>
    const TARGET* cast() const {
        DCHECK(dynamic_cast<const TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<const TARGET*>(this);
    }
    DependencySPtr source_dep = nullptr;
    DependencySPtr sink_dep = nullptr;
    virtual ~BasicSharedState() = default;
};

class Dependency : public std::enable_shared_from_this<Dependency> {
    ENABLE_FACTORY_CREATOR(Dependency);

public:
    Dependency(int id, int node_id, std::string name, QueryContext* query_ctx)
            : _id(id),
              _node_id(node_id),
              _name(std::move(name)),
              _is_write_dependency(false),
              _ready(false),
              _query_ctx(query_ctx) {}
    Dependency(int id, int node_id, std::string name, bool ready, QueryContext* query_ctx)
            : _id(id),
              _node_id(node_id),
              _name(std::move(name)),
              _is_write_dependency(true),
              _ready(ready),
              _query_ctx(query_ctx) {}
    virtual ~Dependency() = default;

    [[nodiscard]] int id() const { return _id; }
    [[nodiscard]] virtual std::string name() const { return _name; }
    void add_child(std::shared_ptr<Dependency> child) { _children.push_back(child); }
    BasicSharedState* shared_state() { return _shared_state; }
    void set_shared_state(BasicSharedState* shared_state) { _shared_state = shared_state; }
    virtual std::string debug_string(int indentation_level = 0);

    // Start the watcher. We use it to count how long this dependency block the current pipeline task.
    void start_watcher() {
        for (auto& child : _children) {
            child->start_watcher();
        }
        _watcher.start();
    }
    [[nodiscard]] int64_t watcher_elapse_time() { return _watcher.elapsed_time(); }

    // Which dependency current pipeline task is blocked by. `nullptr` if this dependency is ready.
    [[nodiscard]] virtual Dependency* is_blocked_by(PipelineXTask* task = nullptr);
    // Notify downstream pipeline tasks this dependency is ready.
    void set_ready();
    void set_ready_to_read() {
        DCHECK(_is_write_dependency) << debug_string();
        DCHECK(_shared_state->source_dep != nullptr) << debug_string();
        _shared_state->source_dep->set_ready();
    }
    void set_block_to_read() {
        DCHECK(_is_write_dependency) << debug_string();
        DCHECK(_shared_state->source_dep != nullptr) << debug_string();
        _shared_state->source_dep->block();
    }
    void set_ready_to_write() {
        DCHECK(_shared_state->sink_dep != nullptr) << debug_string();
        _shared_state->sink_dep->set_ready();
    }
    void set_block_to_write() {
        DCHECK(_shared_state->sink_dep != nullptr) << debug_string();
        _shared_state->sink_dep->block();
    }

    // Notify downstream pipeline tasks this dependency is blocked.
    virtual void block() { _ready = false; }

protected:
    void _add_block_task(PipelineXTask* task);
    bool _is_cancelled() const { return _query_ctx->is_cancelled(); }

    const int _id;
    const int _node_id;
    const std::string _name;
    const bool _is_write_dependency;
    std::atomic<bool> _ready;
    const QueryContext* _query_ctx = nullptr;

    BasicSharedState* _shared_state = nullptr;
    MonotonicStopWatch _watcher;
    std::list<std::shared_ptr<Dependency>> _children;

    std::mutex _task_lock;
    std::vector<PipelineXTask*> _blocked_task;
};

struct FakeSharedState : public BasicSharedState {};

struct FakeDependency final : public Dependency {
public:
    using SharedState = FakeSharedState;
    FakeDependency(int id, int node_id, QueryContext* query_ctx)
            : Dependency(id, node_id, "FakeDependency", query_ctx) {}

    [[nodiscard]] Dependency* is_blocked_by(PipelineXTask* task) override { return nullptr; }
};

struct FinishDependency final : public Dependency {
public:
    using SharedState = FakeSharedState;
    FinishDependency(int id, int node_id, std::string name, QueryContext* query_ctx)
            : Dependency(id, node_id, name, true, query_ctx) {}

    [[nodiscard]] Dependency* is_blocked_by(PipelineXTask* task) override;
};

class RuntimeFilterDependency;
class RuntimeFilterTimer {
public:
    RuntimeFilterTimer(int64_t registration_time, int32_t wait_time_ms,
                       std::shared_ptr<RuntimeFilterDependency> parent)
            : _parent(std::move(parent)),
              _registration_time(registration_time),
              _wait_time_ms(wait_time_ms) {}

    void call_ready();

    void call_timeout();

    void call_has_ready();

    void call_has_release();

    bool has_ready();

    int64_t registration_time() const { return _registration_time; }
    int32_t wait_time_ms() const { return _wait_time_ms; }

private:
    bool _call_ready {};
    bool _call_timeout {};
    std::shared_ptr<RuntimeFilterDependency> _parent;
    std::mutex _lock;
    const int64_t _registration_time;
    const int32_t _wait_time_ms;
    bool _is_ready = false;
};

struct RuntimeFilterTimerQueue {
    constexpr static int64_t interval = 10;
    void run() { _thread.detach(); }
    void start() {
        while (!_stop) {
            std::unique_lock<std::mutex> lk(cv_m);

            cv.wait(lk, [this] { return !_que.empty() || _stop; });
            if (_stop) {
                break;
            }
            {
                std::unique_lock<std::mutex> lc(_que_lock);
                std::list<std::shared_ptr<pipeline::RuntimeFilterTimer>> new_que;
                for (auto& it : _que) {
                    if (it.use_count() == 1) {
                        it->call_has_release();
                    } else if (it->has_ready()) {
                        it->call_has_ready();
                    } else {
                        int64_t ms_since_registration = MonotonicMillis() - it->registration_time();
                        if (ms_since_registration > it->wait_time_ms()) {
                            it->call_timeout();
                        } else {
                            new_que.push_back(std::move(it));
                        }
                    }
                }
                new_que.swap(_que);
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(interval));
        }
        _shutdown = true;
    }

    void stop() {
        _stop = true;
        cv.notify_all();
    }

    void wait_for_shutdown() const {
        while (!_shutdown) {
            std::this_thread::sleep_for(std::chrono::milliseconds(interval));
        }
    }

    ~RuntimeFilterTimerQueue() { wait_for_shutdown(); }
    RuntimeFilterTimerQueue() { _thread = std::thread(&RuntimeFilterTimerQueue::start, this); }
    void push_filter_timer(std::shared_ptr<pipeline::RuntimeFilterTimer> filter) { push(filter); }

    void push(std::shared_ptr<pipeline::RuntimeFilterTimer> filter) {
        std::unique_lock<std::mutex> lc(_que_lock);
        _que.push_back(filter);
        cv.notify_all();
    }

    std::thread _thread;
    std::condition_variable cv;
    std::mutex cv_m;
    std::mutex _que_lock;
    std::atomic_bool _stop = false;
    std::atomic_bool _shutdown = false;
    std::list<std::shared_ptr<pipeline::RuntimeFilterTimer>> _que;
};

class RuntimeFilterDependency final : public Dependency {
public:
    RuntimeFilterDependency(int id, int node_id, std::string name, QueryContext* query_ctx)
            : Dependency(id, node_id, name, query_ctx) {}
    Dependency* is_blocked_by(PipelineXTask* task) override;
    void add_filters(IRuntimeFilter* runtime_filter);
    void sub_filters();
    void set_blocked_by_rf(std::shared_ptr<std::atomic_bool> blocked_by_rf) {
        _blocked_by_rf = blocked_by_rf;
    }

    std::string debug_string(int indentation_level = 0) override;

protected:
    std::atomic_int _filters;
    std::shared_ptr<std::atomic_bool> _blocked_by_rf;
};

class AndDependency final : public Dependency {
public:
    using SharedState = FakeSharedState;
    ENABLE_FACTORY_CREATOR(AndDependency);
    AndDependency(int id, int node_id, QueryContext* query_ctx)
            : Dependency(id, node_id, "AndDependency", query_ctx) {}

    std::string debug_string(int indentation_level = 0) override;

    [[nodiscard]] Dependency* is_blocked_by(PipelineXTask* task) override {
        for (auto& child : Dependency::_children) {
            if (auto* dep = child->is_blocked_by(task)) {
                return dep;
            }
        }
        return nullptr;
    }
};

struct AggSharedState : public BasicSharedState {
public:
    AggSharedState() {
        agg_data = std::make_unique<vectorized::AggregatedDataVariants>();
        agg_arena_pool = std::make_unique<vectorized::Arena>();
    }
    ~AggSharedState() override {
        if (probe_expr_ctxs.empty()) {
            _close_without_key();
        } else {
            _close_with_serialized_key();
        }
    }

    Status reset_hash_table() {
        return std::visit(
                [&](auto&& agg_method) {
                    auto& hash_table = *agg_method.hash_table;
                    using HashTableType = std::decay_t<decltype(hash_table)>;

                    agg_method.reset();

                    hash_table.for_each_mapped([&](auto& mapped) {
                        if (mapped) {
                            static_cast<void>(_destroy_agg_status(mapped));
                            mapped = nullptr;
                        }
                    });

                    aggregate_data_container.reset(new vectorized::AggregateDataContainer(
                            sizeof(typename HashTableType::key_type),
                            ((total_size_of_aggregate_states + align_aggregate_states - 1) /
                             align_aggregate_states) *
                                    align_aggregate_states));
                    agg_method.hash_table.reset(new HashTableType());
                    agg_arena_pool.reset(new vectorized::Arena);
                    return Status::OK();
                },
                agg_data->method_variant);
    }

    static int _get_slot_column_id(const vectorized::AggFnEvaluator* evaluator) {
        auto ctxs = evaluator->input_exprs_ctxs();
        CHECK(ctxs.size() == 1 && ctxs[0]->root()->is_slot_ref())
                << "input_exprs_ctxs is invalid, input_exprs_ctx[0]="
                << ctxs[0]->root()->debug_string();
        return ((vectorized::VSlotRef*)ctxs[0]->root().get())->column_id();
    }

    template <bool limit, bool for_spill = false>
    Status merge_with_serialized_key_helper(vectorized::Block* block) {
        size_t key_size = probe_expr_ctxs.size();
        vectorized::ColumnRawPtrs key_columns(key_size);

        for (size_t i = 0; i < key_size; ++i) {
            if constexpr (for_spill) {
                key_columns[i] = block->get_by_position(i).column.get();
            } else {
                int result_column_id = -1;
                RETURN_IF_ERROR(probe_expr_ctxs[i]->execute(block, &result_column_id));
                block->replace_by_position_if_const(result_column_id);
                key_columns[i] = block->get_by_position(result_column_id).column.get();
            }
        }

        int rows = block->rows();
        if (_places.size() < rows) {
            _places.resize(rows);
        }

        if constexpr (limit) {
            _find_in_hash_table(_places.data(), key_columns, rows);

            for (int i = 0; i < aggregate_evaluators.size(); ++i) {
                if (aggregate_evaluators[i]->is_merge()) {
                    int col_id = _get_slot_column_id(aggregate_evaluators[i]);
                    auto column = block->get_by_position(col_id).column;
                    if (column->is_nullable()) {
                        column = ((vectorized::ColumnNullable*)column.get())
                                         ->get_nested_column_ptr();
                    }

                    size_t buffer_size = aggregate_evaluators[i]->function()->size_of_data() * rows;
                    if (_deserialize_buffer.size() < buffer_size) {
                        _deserialize_buffer.resize(buffer_size);
                    }

                    {
                        // SCOPED_TIMER(_deserialize_data_timer);
                        aggregate_evaluators[i]->function()->deserialize_and_merge_vec_selected(
                                _places.data(), offsets_of_aggregate_states[i],
                                _deserialize_buffer.data(),
                                (vectorized::ColumnString*)(column.get()), agg_arena_pool.get(),
                                rows);
                    }
                } else {
                    RETURN_IF_ERROR(aggregate_evaluators[i]->execute_batch_add_selected(
                            block, offsets_of_aggregate_states[i], _places.data(),
                            agg_arena_pool.get()));
                }
            }
        } else {
            _emplace_into_hash_table(_places.data(), key_columns, rows);

            for (int i = 0; i < aggregate_evaluators.size(); ++i) {
                if (aggregate_evaluators[i]->is_merge() || for_spill) {
                    int col_id = 0;
                    if constexpr (for_spill) {
                        col_id = probe_expr_ctxs.size() + i;
                    } else {
                        col_id = _get_slot_column_id(aggregate_evaluators[i]);
                    }
                    auto column = block->get_by_position(col_id).column;
                    if (column->is_nullable()) {
                        column = ((vectorized::ColumnNullable*)column.get())
                                         ->get_nested_column_ptr();
                    }

                    size_t buffer_size = aggregate_evaluators[i]->function()->size_of_data() * rows;
                    if (_deserialize_buffer.size() < buffer_size) {
                        _deserialize_buffer.resize(buffer_size);
                    }

                    {
                        // SCOPED_TIMER(_deserialize_data_timer);
                        aggregate_evaluators[i]->function()->deserialize_and_merge_vec(
                                _places.data(), offsets_of_aggregate_states[i],
                                _deserialize_buffer.data(),
                                (vectorized::ColumnString*)(column.get()), agg_arena_pool.get(),
                                rows);
                    }
                } else {
                    RETURN_IF_ERROR(aggregate_evaluators[i]->execute_batch_add(
                            block, offsets_of_aggregate_states[i], _places.data(),
                            agg_arena_pool.get()));
                }
            }

            if (_should_limit_output) {
                _reach_limit = _get_hash_table_size() >= _limit;
            }
        }

        return Status::OK();
    }

    void clear();

    size_t _get_hash_table_size() const;

    vectorized::PODArray<vectorized::AggregateDataPtr> _places;
    vectorized::AggregatedDataVariantsUPtr agg_data = nullptr;
    std::unique_ptr<vectorized::AggregateDataContainer> aggregate_data_container;
    vectorized::ArenaUPtr agg_arena_pool;
    std::vector<vectorized::AggFnEvaluator*> aggregate_evaluators;
    // group by k1,k2
    vectorized::VExprContextSPtrs probe_expr_ctxs;
    size_t input_num_rows = 0;
    std::vector<vectorized::AggregateDataPtr> values;
    std::unique_ptr<vectorized::Arena> agg_profile_arena;
    std::unique_ptr<DataQueue> data_queue = std::make_unique<DataQueue>(1, true);
    /// The total size of the row from the aggregate functions.
    size_t total_size_of_aggregate_states = 0;
    size_t align_aggregate_states = 1;
    /// The offset to the n-th aggregate function in a row of aggregate functions.
    vectorized::Sizes offsets_of_aggregate_states;
    std::vector<size_t> make_nullable_keys;

    struct MemoryRecord {
        MemoryRecord() : used_in_arena(0), used_in_state(0) {}
        int64_t used_in_arena;
        int64_t used_in_state;
    };
    MemoryRecord mem_usage_record;
    bool agg_data_created_without_key = false;

    std::vector<char> _deserialize_buffer;

    int64_t _limit = -1; // -1: no limit
    bool _should_limit_output = false;
    bool _reach_limit = false;

private:
    Status _create_agg_status(vectorized::AggregateDataPtr data);
    void _find_in_hash_table(vectorized::AggregateDataPtr* places,
                             vectorized::ColumnRawPtrs& key_columns, size_t num_rows);
    void _emplace_into_hash_table(vectorized::AggregateDataPtr* places,
                                  vectorized::ColumnRawPtrs& key_columns, const size_t num_rows);
    void _close_with_serialized_key() {
        std::visit(
                [&](auto&& agg_method) -> void {
                    auto& data = *agg_method.hash_table;
                    data.for_each_mapped([&](auto& mapped) {
                        if (mapped) {
                            static_cast<void>(_destroy_agg_status(mapped));
                            mapped = nullptr;
                        }
                    });
                    if (data.has_null_key_data()) {
                        auto st = _destroy_agg_status(
                                data.template get_null_key_data<vectorized::AggregateDataPtr>());
                        if (!st) {
                            throw Exception(st.code(), st.to_string());
                        }
                    }
                },
                agg_data->method_variant);
    }
    void _close_without_key() {
        //because prepare maybe failed, and couldn't create agg data.
        //but finally call close to destory agg data, if agg data has bitmapValue
        //will be core dump, it's not initialized
        if (agg_data_created_without_key) {
            static_cast<void>(_destroy_agg_status(agg_data->without_key));
            agg_data_created_without_key = false;
        }
    }
    Status _destroy_agg_status(vectorized::AggregateDataPtr data) {
        for (int i = 0; i < aggregate_evaluators.size(); ++i) {
            aggregate_evaluators[i]->function()->destroy(data + offsets_of_aggregate_states[i]);
        }
        return Status::OK();
    }
};

struct AggSpillPartition;
struct PartitionedAggSharedState : public BasicSharedState {
public:
    PartitionedAggSharedState() {}
    ~PartitionedAggSharedState() override = default;

    void init_spill_params(size_t spill_partition_count_bits, AggSharedState* agg_shared_state);

    Status prepare_merge_partition_aggregation_data() {
        // return shared_state_->prepare_merge_partition_aggregation_data();
        return Status::OK();
    }

    Status merge_spilt_partition_aggregation_data(vectorized::Block* block) {
        // return shared_state_->merge_with_serialized_key_helper<false, true>(block);
        return Status::OK();
    }
    void clear();

    AggSharedState* _agg_shared_state = nullptr;
    std::map<int, std::shared_ptr<BasicSharedState>> _shared_states;
    std::vector<DependencySPtr> _upstream_deps;
    std::vector<DependencySPtr> _downstream_deps;

    size_t partition_count_bits_;
    size_t partition_count_;
    size_t max_partition_index_;
    Status sink_status_;
    std::deque<std::shared_ptr<AggSpillPartition>> spill_partitions_;

    size_t get_partition_index(size_t hash_value) const {
        return (hash_value >> (32 - partition_count_bits_)) & max_partition_index_;
    }
};

struct AggSpillPartition {
    AggSpillPartition(PartitionedAggSharedState* parent) : _parent(parent) {}

    void init();

    Status prepare_spill_stream(RuntimeState* state, int operator_id, RuntimeProfile* profile);

    template <typename HashTableCtxType, typename KeyType>
    Status to_block(HashTableCtxType& context, std::vector<KeyType>& keys,
                    std::vector<vectorized::AggregateDataPtr>& values,
                    const vectorized::AggregateDataPtr null_key_data) {
        context.insert_keys_into_columns(keys, key_columns_, keys.size());

        if (null_key_data) {
            // only one key of group by support wrap null key
            // here need additional processing logic on the null key / value
            CHECK(key_columns_.size() == 1);
            CHECK(key_columns_[0]->is_nullable());
            key_columns_[0]->insert_data(nullptr, 0);

            values.emplace_back(null_key_data);
        }

        for (size_t i = 0; i < _parent->_agg_shared_state->aggregate_evaluators.size(); ++i) {
            _parent->_agg_shared_state->aggregate_evaluators[i]->function()->serialize_to_column(
                    values, _parent->_agg_shared_state->offsets_of_aggregate_states[i],
                    value_columns_[i], values.size());
        }

        vectorized::ColumnsWithTypeAndName key_columns_with_schema;
        for (int i = 0; i < key_columns_.size(); ++i) {
            key_columns_with_schema.emplace_back(
                    std::move(key_columns_[i]),
                    _parent->_agg_shared_state->probe_expr_ctxs[i]->root()->data_type(),
                    _parent->_agg_shared_state->probe_expr_ctxs[i]->root()->expr_name());
        }
        key_block_ = key_columns_with_schema;

        vectorized::ColumnsWithTypeAndName value_columns_with_schema;
        for (int i = 0; i < value_columns_.size(); ++i) {
            value_columns_with_schema.emplace_back(
                    std::move(value_columns_[i]), value_data_types_[i],
                    _parent->_agg_shared_state->aggregate_evaluators[i]->function()->get_name());
        }
        value_block_ = value_columns_with_schema;

        for (const auto& column : key_block_.get_columns_with_type_and_name()) {
            block_.insert(column);
        }
        for (const auto& column : value_block_.get_columns_with_type_and_name()) {
            block_.insert(column);
        }
        return Status::OK();
    }

    Status wait_spill() {
        DCHECK(spilling_stream_);
        auto status = spilling_stream_->wait_spill();
        _reset_tmp_data();
        return status;
    }

    void _reset_tmp_data() {
        block_.clear();
        key_columns_.clear();
        value_columns_.clear();
        key_block_.clear_column_data();
        value_block_.clear_column_data();
        key_columns_ = key_block_.mutate_columns();
        value_columns_ = value_block_.mutate_columns();
    }
    void close();

    Status reset_spilling() {
        _reset_tmp_data();
        key_columns_.clear();
        value_columns_.clear();
        init();
        if (spilling_stream_) {
            auto status = spilling_stream_->spill_eof();
            spilling_stream_.reset();
            return status;
        }
        return Status::OK();
    }

    vectorized::SpillStreamSPtr spilling_stream() const { return spilling_stream_; }
    const vectorized::Block& get_spill_block() const { return block_; }

    PartitionedAggSharedState* _parent = nullptr;

    std::deque<vectorized::SpillStreamSPtr> spill_streams_;

    // tmp members during spilling
    vectorized::SpillStreamSPtr spilling_stream_;
    vectorized::MutableColumns key_columns_;
    vectorized::MutableColumns value_columns_;
    vectorized::DataTypes value_data_types_;
    vectorized::Block block_;
    vectorized::Block key_block_;
    vectorized::Block value_block_;
};
using AggSpillPartitionSPtr = std::shared_ptr<AggSpillPartition>;
struct SortSharedState : public BasicSharedState {
public:
    void update_spill_block_batch_size(const vectorized::Block* block) {
        auto rows = block->rows();
        if (rows > 0 && 0 == avg_row_bytes_) {
            avg_row_bytes_ = std::max((std::size_t)1, block->bytes() / rows);
            spill_block_batch_size_ =
                    (SORT_BLOCK_SPILL_BATCH_BYTES + avg_row_bytes_ - 1) / avg_row_bytes_;
        }
    }

    void clear();

    // This number specifies the maximum size of sub blocks
    static constexpr int SORT_BLOCK_SPILL_BATCH_BYTES = 8 * 1024 * 1024;

    std::unique_ptr<vectorized::Sorter> sorter;
    Status sink_status_;
    bool enable_spill_ = false;
    size_t avg_row_bytes_ = 0;
    int spill_block_batch_size_;
    std::deque<vectorized::SpillStreamSPtr> sorted_streams_;
};

struct UnionSharedState : public BasicSharedState {
public:
    UnionSharedState(int child_count = 1)
            : data_queue(child_count, false), _child_count(child_count) {};
    int child_count() const { return _child_count; }
    DataQueue data_queue;
    const int _child_count;
};

struct MultiCastSharedState : public BasicSharedState {
public:
    MultiCastSharedState(const RowDescriptor& row_desc, ObjectPool* pool, int cast_sender_count)
            : multi_cast_data_streamer(row_desc, pool, cast_sender_count, true) {}
    pipeline::MultiCastDataStreamer multi_cast_data_streamer;
};

struct AnalyticSharedState : public BasicSharedState {
public:
    AnalyticSharedState() = default;

    int64_t current_row_position = 0;
    vectorized::BlockRowPos partition_by_end;
    vectorized::VExprContextSPtrs partition_by_eq_expr_ctxs;
    int64_t input_total_rows = 0;
    vectorized::BlockRowPos all_block_end;
    std::vector<vectorized::Block> input_blocks;
    bool input_eos = false;
    vectorized::BlockRowPos found_partition_end;
    std::vector<int64_t> origin_cols;
    vectorized::VExprContextSPtrs order_by_eq_expr_ctxs;
    std::vector<int64_t> input_block_first_row_positions;
    std::vector<std::vector<vectorized::MutableColumnPtr>> agg_input_columns;

    // TODO: maybe global?
    std::vector<int64_t> partition_by_column_idxs;
    std::vector<int64_t> ordey_by_column_idxs;
};

struct JoinSharedState : public BasicSharedState {
    // For some join case, we can apply a short circuit strategy
    // 1. _has_null_in_build_side = true
    // 2. build side rows is empty, Join op is: inner join/right outer join/left semi/right semi/right anti
    bool _has_null_in_build_side = false;
    bool short_circuit_for_probe = false;
    // for some join, when build side rows is empty, we could return directly by add some additional null data in probe table.
    bool empty_right_table_need_probe_dispose = false;
    vectorized::JoinOpVariants join_op_variants;
};

struct HashJoinSharedState : public JoinSharedState {
    // mark the join column whether support null eq
    std::vector<bool> is_null_safe_eq_join;
    // mark the build hash table whether it needs to store null value
    std::vector<bool> store_null_in_hash_table;
    std::shared_ptr<vectorized::Arena> arena = std::make_shared<vectorized::Arena>();

    // maybe share hash table with other fragment instances
    std::shared_ptr<vectorized::HashTableVariants> hash_table_variants =
            std::make_shared<vectorized::HashTableVariants>();
    const std::vector<TupleDescriptor*> build_side_child_desc;
    size_t build_exprs_size = 0;
    std::shared_ptr<vectorized::Block> build_block;
    std::shared_ptr<std::vector<uint32_t>> build_indexes_null;
    bool probe_ignore_null = false;
};

struct PartitionedHashJoinSharedState : public HashJoinSharedState {
    std::vector<std::unique_ptr<vectorized::MutableBlock>> partitioned_build_blocks;
};

struct NestedLoopJoinSharedState : public JoinSharedState {
    // if true, left child has no more rows to process
    bool left_side_eos = false;
    // Visited flags for each row in build side.
    vectorized::MutableColumns build_side_visited_flags;
    // List of build blocks, constructed in prepare()
    vectorized::Blocks build_blocks;
};

struct PartitionSortNodeSharedState : public BasicSharedState {
public:
    std::queue<vectorized::Block> blocks_buffer;
    std::mutex buffer_mutex;
    std::vector<std::unique_ptr<vectorized::PartitionSorter>> partition_sorts;
    bool sink_eos = false;
    std::mutex sink_eos_lock;
};

class AsyncWriterDependency final : public Dependency {
public:
    using SharedState = FakeSharedState;
    ENABLE_FACTORY_CREATOR(AsyncWriterDependency);
    AsyncWriterDependency(int id, int node_id, QueryContext* query_ctx)
            : Dependency(id, node_id, "AsyncWriterDependency", true, query_ctx) {}
    ~AsyncWriterDependency() override = default;
};

struct SetSharedState : public BasicSharedState {
public:
    /// default init
    vectorized::Block build_block; // build to source
    //record element size in hashtable
    int64_t valid_element_in_hash_tbl = 0;
    //first:column_id, could point to origin column or cast column
    //second:idx mapped to column types
    std::unordered_map<int, int> build_col_idx;

    //// shared static states (shared, decided in prepare/open...)

    /// init in setup_local_state
    std::unique_ptr<vectorized::SetHashTableVariants> hash_table_variants =
            nullptr; // the real data HERE.
    std::vector<bool> build_not_ignore_null;

    /// init in both upstream side.
    //The i-th result expr list refers to the i-th child.
    std::vector<vectorized::VExprContextSPtrs> child_exprs_lists;

    /// init in build side
    int child_quantity;
    vectorized::VExprContextSPtrs build_child_exprs;
    std::vector<Dependency*> probe_finished_children_dependency;

    /// init in probe side
    std::vector<vectorized::VExprContextSPtrs> probe_child_exprs_lists;

    std::atomic<bool> ready_for_read = false;

    /// called in setup_local_state
    void hash_table_init() {
        using namespace vectorized;
        if (child_exprs_lists[0].size() == 1 && (!build_not_ignore_null[0])) {
            // Single column optimization
            switch (child_exprs_lists[0][0]->root()->result_type()) {
            case TYPE_BOOLEAN:
            case TYPE_TINYINT:
                hash_table_variants->emplace<SetPrimaryTypeHashTableContext<UInt8>>();
                break;
            case TYPE_SMALLINT:
                hash_table_variants->emplace<SetPrimaryTypeHashTableContext<UInt16>>();
                break;
            case TYPE_INT:
            case TYPE_FLOAT:
            case TYPE_DATEV2:
            case TYPE_DECIMAL32:
                hash_table_variants->emplace<SetPrimaryTypeHashTableContext<UInt32>>();
                break;
            case TYPE_BIGINT:
            case TYPE_DOUBLE:
            case TYPE_DATETIME:
            case TYPE_DATE:
            case TYPE_DECIMAL64:
            case TYPE_DATETIMEV2:
                hash_table_variants->emplace<SetPrimaryTypeHashTableContext<UInt64>>();
                break;
            case TYPE_LARGEINT:
            case TYPE_DECIMALV2:
            case TYPE_DECIMAL128I:
                hash_table_variants->emplace<SetPrimaryTypeHashTableContext<UInt128>>();
                break;
            default:
                hash_table_variants->emplace<SetSerializedHashTableContext>();
            }
            return;
        }
        if (!try_get_hash_map_context_fixed<NormalHashMap, HashCRC32, RowRefListWithFlags>(
                    *hash_table_variants, child_exprs_lists[0])) {
            hash_table_variants->emplace<SetSerializedHashTableContext>();
        }
    }
};

enum class ExchangeType : uint8_t {
    NOOP = 0,
    // Shuffle data by Crc32HashPartitioner<LocalExchangeChannelIds>.
    HASH_SHUFFLE = 1,
    // Round-robin passthrough data blocks.
    PASSTHROUGH = 2,
    // Shuffle data by Crc32HashPartitioner<ShuffleChannelIds> (e.g. same as storage engine).
    BUCKET_HASH_SHUFFLE = 3,
    // Passthrough data blocks to all channels.
    BROADCAST = 4,
    // Passthrough data to channels evenly in an adaptive way.
    ADAPTIVE_PASSTHROUGH = 5,
    // Send all data to the first channel.
    PASS_TO_ONE = 6,
};

inline std::string get_exchange_type_name(ExchangeType idx) {
    switch (idx) {
    case ExchangeType::NOOP:
        return "NOOP";
    case ExchangeType::HASH_SHUFFLE:
        return "HASH_SHUFFLE";
    case ExchangeType::PASSTHROUGH:
        return "PASSTHROUGH";
    case ExchangeType::BUCKET_HASH_SHUFFLE:
        return "BUCKET_HASH_SHUFFLE";
    case ExchangeType::BROADCAST:
        return "BROADCAST";
    case ExchangeType::ADAPTIVE_PASSTHROUGH:
        return "ADAPTIVE_PASSTHROUGH";
    case ExchangeType::PASS_TO_ONE:
        return "PASS_TO_ONE";
    }
    LOG(FATAL) << "__builtin_unreachable";
    __builtin_unreachable();
}

struct DataDistribution {
    DataDistribution(ExchangeType type) : distribution_type(type) {}
    DataDistribution(ExchangeType type, const std::vector<TExpr>& partition_exprs_)
            : distribution_type(type), partition_exprs(partition_exprs_) {}
    DataDistribution(const DataDistribution& other)
            : distribution_type(other.distribution_type), partition_exprs(other.partition_exprs) {}
    bool need_local_exchange() const { return distribution_type != ExchangeType::NOOP; }
    DataDistribution& operator=(const DataDistribution& other) {
        distribution_type = other.distribution_type;
        partition_exprs = other.partition_exprs;
        return *this;
    }
    ExchangeType distribution_type;
    std::vector<TExpr> partition_exprs;
};

class Exchanger;

struct LocalExchangeSharedState : public BasicSharedState {
public:
    ENABLE_FACTORY_CREATOR(LocalExchangeSharedState);
    LocalExchangeSharedState(int num_instances);
    std::unique_ptr<Exchanger> exchanger {};
    std::vector<DependencySPtr> source_dependencies;
    DependencySPtr sink_dependency;
    std::vector<MemTracker*> mem_trackers;
    std::atomic<size_t> mem_usage = 0;
    std::mutex le_lock;
    void sub_running_sink_operators();
    void _set_ready_for_read() {
        for (auto& dep : source_dependencies) {
            DCHECK(dep);
            dep->set_ready();
        }
    }

    void set_dep_by_channel_id(DependencySPtr dep, int channel_id) {
        source_dependencies[channel_id] = dep;
    }

    void set_ready_to_read(int channel_id) {
        auto& dep = source_dependencies[channel_id];
        DCHECK(dep) << channel_id;
        dep->set_ready();
    }

    void add_mem_usage(int channel_id, size_t delta, bool update_total_mem_usage = true) {
        mem_trackers[channel_id]->consume(delta);
        if (update_total_mem_usage) {
            add_total_mem_usage(delta);
        }
    }

    void sub_mem_usage(int channel_id, size_t delta, bool update_total_mem_usage = true) {
        mem_trackers[channel_id]->release(delta);
        if (update_total_mem_usage) {
            sub_total_mem_usage(delta);
        }
    }

    void add_total_mem_usage(size_t delta) {
        if (mem_usage.fetch_add(delta) > config::local_exchange_buffer_mem_limit) {
            sink_dependency->block();
        }
    }

    void sub_total_mem_usage(size_t delta) {
        if (mem_usage.fetch_sub(delta) <= config::local_exchange_buffer_mem_limit) {
            sink_dependency->set_ready();
        }
    }
};

} // namespace doris::pipeline
