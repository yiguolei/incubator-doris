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
#include "aggregation_sink_operator.h"
#include "pipeline/pipeline_x/operator.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris {
class ExecNode;

namespace pipeline {
class PartitionedAggSinkLocalState;
template <typename LocalStateType = PartitionedAggSinkLocalState>
class PartitionedAggSinkOperatorX;

class PartitionedAggSinkDependency final : public Dependency {
public:
    using SharedState = PartitionedAggSharedState;
    PartitionedAggSinkDependency(int id, int node_id, QueryContext* query_ctx)
            : Dependency(id, node_id, "PartitionedAggSinkDependency", true, query_ctx) {}
    ~PartitionedAggSinkDependency() override = default;

    friend class PartitionedAggSinkOperatorX<>;
    friend class PartitionedAggSinkLocalState;
};

class BlockingAggSinkLocalState;
class PartitionedAggSinkLocalState : public PipelineXSinkLocalState<PartitionedAggSinkDependency> {
public:
    ENABLE_FACTORY_CREATOR(PartitionedAggSinkLocalState);
    using Base = PipelineXSinkLocalState<PartitionedAggSinkDependency>;
    using Parent = PartitionedAggSinkOperatorX<PartitionedAggSinkLocalState>;

    PartitionedAggSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state);
    ~PartitionedAggSinkLocalState() override = default;

    template <typename LocalStateType>
    friend class PartitionedAggSinkOperatorX;

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;
    Dependency* finishdependency() override { return _finish_dependency.get(); }

    Status revoke_memory(RuntimeState* state);

    Status setup_in_memory_agg_op(RuntimeState* state);

    template <typename KeyType>
    struct TmpSpillInfo {
        std::vector<KeyType> keys_;
        std::vector<vectorized::AggregateDataPtr> values_;
    };
    template <typename HashTableCtxType, typename HashTableType>
    Status spill_hash_table2(RuntimeState* state, HashTableCtxType& context,
                             HashTableType& hash_table) {
        Status status;
        Defer defer {[&]() {
            if (!status.ok()) {
                Base::_shared_state->clear();
            }
        }};
        // static constexpr int AGG_SPILL_BLOCK_MEM_SIZE = 1024 * 1024 * 8; // 8M

        context.init_iterator();
        _in_memory_agg_op_shared_state->aggregate_data_container->init_once();

        static int spill_batch_rows = 4096;
        int row_count = 0;

        std::vector<TmpSpillInfo<typename HashTableType::key_type>> spill_infos(
                Base::_shared_state->partition_count_);
        auto& iter = _in_memory_agg_op_shared_state->aggregate_data_container->iterator;
        while (iter != _in_memory_agg_op_shared_state->aggregate_data_container->end()) {
            const auto& key = iter.template get_key<typename HashTableType::key_type>();
            auto partition_index = Base::_shared_state->get_partition_index(hash_table.hash(key));
            spill_infos[partition_index].keys_.emplace_back(key);
            spill_infos[partition_index].values_.emplace_back(iter.get_aggregate_data());

            if (++row_count == spill_batch_rows) {
                row_count = 0;
                std::vector<AggSpillPartitionSPtr> spill_partitions;
                for (int i = 0; i < Base::_shared_state->partition_count_; ++i) {
                    if (spill_infos[i].keys_.size() >= spill_batch_rows) {
                        status = _async_spill(
                                state, context, Base::_shared_state->spill_partitions_[i],
                                spill_infos[i].keys_, spill_infos[i].values_, nullptr);
                        RETURN_IF_ERROR(status);
                        spill_partitions.emplace_back(Base::_shared_state->spill_partitions_[i]);
                        spill_infos[i].keys_.clear();
                        spill_infos[i].values_.clear();
                    }
                }
                for (auto& partition : spill_partitions) {
                    status = partition->wait_spill();
                    RETURN_IF_ERROR(status);
                }
            }

            ++iter;
        }
        auto hash_null_key_data = hash_table.has_null_key_data();
        if (row_count > 0 || hash_null_key_data) {
            std::vector<AggSpillPartitionSPtr> spill_partitions;
            for (int i = 0; i < Base::_shared_state->partition_count_; ++i) {
                auto spill_null_key_data =
                        (hash_null_key_data && i == Base::_shared_state->partition_count_ - 1);
                if (spill_infos[i].keys_.size() > 0 || spill_null_key_data) {
                    status = _async_spill(state, context, Base::_shared_state->spill_partitions_[i],
                                          spill_infos[i].keys_, spill_infos[i].values_,
                                          spill_null_key_data
                                                  ? hash_table.template get_null_key_data<
                                                            vectorized::AggregateDataPtr>()
                                                  : nullptr);
                    RETURN_IF_ERROR(status);
                    spill_partitions.emplace_back(Base::_shared_state->spill_partitions_[i]);
                    spill_infos[i].keys_.clear();
                    spill_infos[i].values_.clear();
                }
            }
            for (auto& partition : spill_partitions) {
                status = partition->wait_spill();
                RETURN_IF_ERROR(status);
            }
        }

        for (auto& partition : Base::_shared_state->spill_partitions_) {
            status = partition->reset_spilling();
            RETURN_IF_ERROR(status);
        }
        return Status::OK();
    }

    template <typename HashTableCtxType, typename KeyType>
    Status _async_spill(RuntimeState* state, HashTableCtxType& context,
                        AggSpillPartitionSPtr& spill_partition, std::vector<KeyType>& keys,
                        std::vector<vectorized::AggregateDataPtr>& values,
                        const vectorized::AggregateDataPtr null_key_data) {
        RETURN_IF_ERROR(spill_partition->prepare_spill_stream(state, Base::_parent->operator_id(),
                                                              Base::profile()));
        RETURN_IF_ERROR(spill_partition->to_block(context, keys, values, null_key_data));
        return ExecEnv::GetInstance()
                ->spill_stream_mgr()
                ->get_spill_io_thread_pool(spill_partition->spilling_stream()->get_spill_root_dir())
                ->submit_func([spill_partition] {
                    Status status;
                    auto spilling_stream = spill_partition->spilling_stream();
                    Defer defer {[&]() { spilling_stream->end_spill(status); }};
                    status =
                            spilling_stream->spill_block(spill_partition->get_spill_block(), false);
                });
    }

    // volatile bool _need_setup_in_memory_agg_op = true;
    AggSharedState* _in_memory_agg_op_shared_state = nullptr;
    std::unique_ptr<RuntimeState> _runtime_state;
    // std::map<int, std::shared_ptr<BasicSharedState>> _shared_states;
    // std::vector<DependencySPtr> _upstream_deps;
    // std::vector<DependencySPtr> _downstream_deps;

    SourceState _source_state = SourceState::DEPEND_ON_SOURCE;
    std::shared_ptr<Dependency> _finish_dependency;
    bool is_spilling_ = false;
    std::mutex spill_lock_;
    std::condition_variable spill_cv_;
};

template <typename LocalStateType>
class PartitionedAggSinkOperatorX : public DataSinkOperatorX<LocalStateType> {
public:
    PartitionedAggSinkOperatorX(ObjectPool* pool, int operator_id, const TPlanNode& tnode,
                                const DescriptorTbl& descs, bool is_streaming = false);
    ~PartitionedAggSinkOperatorX() override = default;
    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TPlanNode",
                                     DataSinkOperatorX<LocalStateType>::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override;

    DataDistribution required_data_distribution() const override {
        if (!_has_probe_expr) {
            return _needs_finalize || DataSinkOperatorX<LocalStateType>::_child_x
                                              ->ignore_data_distribution()
                           ? DataDistribution(ExchangeType::PASSTHROUGH)
                           : DataSinkOperatorX<LocalStateType>::required_data_distribution();
        }
        return _is_colocate ? DataDistribution(ExchangeType::BUCKET_HASH_SHUFFLE, _partition_exprs)
                            : DataDistribution(ExchangeType::HASH_SHUFFLE, _partition_exprs);
    }

    Status set_child(OperatorXPtr child) override {
        RETURN_IF_ERROR(DataSinkOperatorX<LocalStateType>::set_child(child));
        return _agg_sink_operator->set_child(child);
    }
    size_t revocable_mem_size(RuntimeState* state) const override;

    Status revoke_memory(RuntimeState* state) override;

    using DataSinkOperatorX<LocalStateType>::id;
    using DataSinkOperatorX<LocalStateType>::operator_id;
    using DataSinkOperatorX<LocalStateType>::get_local_state;

private:
    friend class PartitionedAggSinkLocalState;
    std::unique_ptr<AggSinkOperatorX<>> _agg_sink_operator;
    const std::vector<TExpr> _partition_exprs;
    bool _has_probe_expr;
    bool _needs_finalize;
    bool _is_colocate;

    size_t _spill_partition_count_bits = 4;

    // RuntimeProfile* runtime_profile = nullptr;
};
} // namespace pipeline
} // namespace doris