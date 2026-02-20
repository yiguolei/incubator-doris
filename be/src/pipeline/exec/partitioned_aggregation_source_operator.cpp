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

#include "partitioned_aggregation_source_operator.h"

#include <glog/logging.h>

#include <limits>
#include <string>

#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "pipeline/exec/aggregation_source_operator.h"
#include "pipeline/exec/operator.h"
#include "pipeline/exec/spill_utils.h"
#include "pipeline/pipeline_task.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "util/pretty_printer.h"
#include "util/runtime_profile.h"
#include "util/uid_util.h"
#include "vec/spill/spill_repartitioner.h"
#include "vec/spill/spill_stream.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

PartitionedAggLocalState::PartitionedAggLocalState(RuntimeState* state, OperatorXBase* parent)
        : Base(state, parent) {}

Status PartitionedAggLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _internal_runtime_profile = std::make_unique<RuntimeProfile>("internal_profile");
    return Status::OK();
}

Status PartitionedAggLocalState::open(RuntimeState* state) {
    RETURN_IF_ERROR(Base::open(state));
    SCOPED_TIMER(_open_timer);
    if (_opened) {
        return Status::OK();
    }
    _opened = true;
    RETURN_IF_ERROR(setup_in_memory_agg_op(state));

    return Status::OK();
}

#define UPDATE_COUNTER_FROM_INNER(name) \
    update_profile_from_inner_profile<spilled>(name, custom_profile(), child_profile)

template <bool spilled>
void PartitionedAggLocalState::update_profile(RuntimeProfile* child_profile) {
    UPDATE_COUNTER_FROM_INNER("GetResultsTime");
    UPDATE_COUNTER_FROM_INNER("HashTableIterateTime");
    UPDATE_COUNTER_FROM_INNER("InsertKeysToColumnTime");
    UPDATE_COUNTER_FROM_INNER("InsertValuesToColumnTime");
    UPDATE_COUNTER_FROM_INNER("MergeTime");
    UPDATE_COUNTER_FROM_INNER("DeserializeAndMergeTime");
    UPDATE_COUNTER_FROM_INNER("HashTableComputeTime");
    UPDATE_COUNTER_FROM_INNER("HashTableEmplaceTime");
    UPDATE_COUNTER_FROM_INNER("HashTableInputCount");
    UPDATE_COUNTER_FROM_INNER("MemoryUsageHashTable");
    UPDATE_COUNTER_FROM_INNER("HashTableSize");
    UPDATE_COUNTER_FROM_INNER("MemoryUsageContainer");
    UPDATE_COUNTER_FROM_INNER("MemoryUsageArena");
}

#undef UPDATE_COUNTER_FROM_INNER

Status PartitionedAggLocalState::close(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);
    if (_closed) {
        return Status::OK();
    }

    // Clean up multi-level spill queue resources.
    for (auto& partition : _spill_partition_queue) {
        for (auto& stream : partition.streams) {
            if (stream) {
                ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
            }
        }
    }
    _spill_partition_queue.clear();
    for (auto& stream : _current_queue_partition.streams) {
        if (stream) {
            ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
        }
    }
    _current_queue_partition.streams.clear();

    return Base::close(state);
}
PartitionedAggSourceOperatorX::PartitionedAggSourceOperatorX(ObjectPool* pool,
                                                             const TPlanNode& tnode,
                                                             int operator_id,
                                                             const DescriptorTbl& descs)
        : Base(pool, tnode, operator_id, descs) {
    _agg_source_operator = std::make_unique<AggSourceOperatorX>(pool, tnode, operator_id, descs);
}

Status PartitionedAggSourceOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(OperatorXBase::init(tnode, state));
    _op_name = "PARTITIONED_AGGREGATION_OPERATOR";
    return _agg_source_operator->init(tnode, state);
}

Status PartitionedAggSourceOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorXBase::prepare(state));
    return _agg_source_operator->prepare(state);
}

Status PartitionedAggSourceOperatorX::close(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorXBase::close(state));
    return _agg_source_operator->close(state);
}

bool PartitionedAggSourceOperatorX::is_serial_operator() const {
    return _agg_source_operator->is_serial_operator();
}

void PartitionedAggSourceOperatorX::update_operator(const TPlanNode& tnode,
                                                    bool followed_by_shuffled_operator,
                                                    bool require_bucket_distribution) {
    _agg_source_operator->update_operator(tnode, followed_by_shuffled_operator,
                                          require_bucket_distribution);
}

DataDistribution PartitionedAggSourceOperatorX::required_data_distribution(
        RuntimeState* state) const {
    return _agg_source_operator->required_data_distribution(state);
}

bool PartitionedAggSourceOperatorX::is_colocated_operator() const {
    return _agg_source_operator->is_colocated_operator();
}
bool PartitionedAggSourceOperatorX::is_shuffled_operator() const {
    return _agg_source_operator->is_shuffled_operator();
}

Status PartitionedAggSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                                bool* eos) {
    auto& local_state = get_local_state(state);
    local_state.copy_shared_spill_profile();
    Status status;
    Defer defer {[&]() {
        if (!status.ok() || *eos) {
            local_state._shared_state->close();
        }
    }};

    SCOPED_TIMER(local_state.exec_time_counter());

    // If we are already processing the multi-level spill queue, delegate to it.
    if (local_state._processing_spill_queue) {
        return _pull_from_spill_queue(local_state, state, block, eos);
    }

    if (local_state._shared_state->is_spilled &&
        local_state._need_to_merge_data_for_current_partition) {
        // Pre-check: when starting a new partition (no blocks loaded yet, not started reading),
        // check if the partition data is too large for available memory.
        if (local_state._blocks.empty() && local_state._current_partition_eos &&
            !local_state._shared_state->spill_partitions.empty()) {
            static constexpr int64_t HASH_TABLE_OVERHEAD_FACTOR = 3;
            auto& front_partition = local_state._shared_state->spill_partitions[0];
            int64_t partition_bytes = 0;
            for (auto& stream : front_partition->spill_streams_) {
                if (stream) {
                    partition_bytes += stream->get_written_bytes();
                }
            }
            int64_t estimated_memory = partition_bytes * HASH_TABLE_OVERHEAD_FACTOR;

            auto query_mem_tracker = state->get_query_ctx()->query_mem_tracker();
            int64_t available_memory =
                    query_mem_tracker->limit() > 0
                            ? (query_mem_tracker->limit() - query_mem_tracker->consumption())
                            : std::numeric_limits<int64_t>::max();

            if (estimated_memory > available_memory && partition_bytes > 0) {
                VLOG_DEBUG << fmt::format(
                        "Query:{}, agg source:{}, task:{}, partition data too large for memory"
                        " (estimated:{}, available:{}), repartitioning directly",
                        print_id(state->query_id()), node_id(), state->task_id(),
                        PrettyPrinter::print_bytes(estimated_memory),
                        PrettyPrinter::print_bytes(available_memory));

                // Convert the original AggSpillPartition to an AggSpillPartitionInfo
                AggSpillPartitionInfo partition_info(std::move(front_partition->spill_streams_),
                                                     /*level=*/0);
                local_state._shared_state->spill_partitions.pop_front();

                status = local_state.repartition_agg_partition(state, partition_info);
                RETURN_IF_ERROR(status);

                // If all original partitions are done, switch to queue processing
                if (local_state._shared_state->spill_partitions.empty()) {
                    if (!local_state._spill_partition_queue.empty()) {
                        local_state._processing_spill_queue = true;
                        local_state._need_to_setup_queue_partition = true;
                        *eos = false;
                        return Status::OK();
                    }
                }

                *eos = false;
                return Status::OK();
            }
        }

        if (local_state._blocks.empty() && !local_state._current_partition_eos) {
            bool has_recovering_data = false;
            status = local_state.recover_blocks_from_disk(state, has_recovering_data);
            RETURN_IF_ERROR(status);
            if (!has_recovering_data) {
                // All original partitions may be exhausted. Check if we have
                // queue entries to process before declaring eos.
                if (!local_state._spill_partition_queue.empty()) {
                    local_state._processing_spill_queue = true;
                    local_state._need_to_setup_queue_partition = true;
                    status = _agg_source_operator->reset_hash_table(
                            local_state._runtime_state.get());
                    RETURN_IF_ERROR(status);
                    *eos = false;
                    return Status::OK();
                }
                *eos = true;
            }
            return Status::OK();
        } else if (!local_state._blocks.empty()) {
            size_t merged_rows = 0;
            while (!local_state._blocks.empty()) {
                auto block_ = std::move(local_state._blocks.front());
                merged_rows += block_.rows();
                local_state._blocks.erase(local_state._blocks.begin());
                status = _agg_source_operator->merge_with_serialized_key_helper(
                        local_state._runtime_state.get(), &block_);
                RETURN_IF_ERROR(status);
            }
            local_state._estimate_memory_usage +=
                    _agg_source_operator->get_estimated_memory_size_for_merging(
                            local_state._runtime_state.get(), merged_rows);

            // Mid-merge memory check: if the hash table is growing too large and
            // there's still data left to read, repartition.
            if (!local_state._current_partition_eos) {
                auto query_mem_tracker = state->get_query_ctx()->query_mem_tracker();
                int64_t available_memory =
                        query_mem_tracker->limit() > 0
                                ? (query_mem_tracker->limit() - query_mem_tracker->consumption())
                                : std::numeric_limits<int64_t>::max();

                // If available memory drops below a threshold, we need to repartition
                // the current hash table + remaining stream data.
                static constexpr int64_t MIN_AVAILABLE_MEMORY_FOR_MERGE = 32 * 1024 * 1024; // 32MB
                if (available_memory < MIN_AVAILABLE_MEMORY_FOR_MERGE) {
                    VLOG_DEBUG << fmt::format(
                            "Query:{}, agg source:{}, task:{}, mid-merge OOM detected"
                            " (available:{}), flushing hash table and repartitioning remaining",
                            print_id(state->query_id()), node_id(), state->task_id(),
                            PrettyPrinter::print_bytes(available_memory));

                    // Create output sub-streams
                    std::vector<vectorized::SpillStreamSPtr> output_streams;
                    RETURN_IF_ERROR(SpillRepartitioner::create_output_streams(
                            state, node_id(), "agg_repart_midmerge_l1",
                            local_state.operator_profile(), output_streams));

                    // Collect profile before flushing (flush resets the hash table).
                    {
                        auto* source_local_state =
                                local_state._runtime_state->get_local_state(
                                        _agg_source_operator->operator_id());
                        local_state.update_profile<true>(source_local_state->custom_profile());
                    }

                    // Flush the hash table data to sub-streams
                    status = local_state.flush_hash_table_to_sub_streams(state, output_streams);
                    RETURN_IF_ERROR(status);

                    // Repartition remaining unread streams from the current original partition
                    auto& front_partition = local_state._shared_state->spill_partitions[0];

                    auto* in_mem_state = local_state._shared_state->in_mem_shared_state;
                    size_t num_keys = in_mem_state->probe_expr_ctxs.size();
                    std::vector<size_t> key_indices(num_keys);
                    std::vector<vectorized::DataTypePtr> key_types(num_keys);
                    for (size_t i = 0; i < num_keys; ++i) {
                        key_indices[i] = i;
                        key_types[i] = in_mem_state->probe_expr_ctxs[i]->root()->data_type();
                    }
                    local_state._repartitioner.init_with_key_columns(
                            std::move(key_indices), std::move(key_types),
                            local_state.operator_profile());

                    for (auto& stream : front_partition->spill_streams_) {
                        if (!stream) {
                            continue;
                        }
                        if (stream->get_written_bytes() == 0) {
                            ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(
                                    stream);
                            stream.reset();
                            continue;
                        }
                        stream->set_read_counters(local_state.operator_profile());
                        bool done = false;
                        while (!done && !state->is_cancelled()) {
                            RETURN_IF_ERROR(local_state._repartitioner.repartition(
                                    state, stream, output_streams, &done));
                        }
                        ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
                        stream.reset();
                    }
                    front_partition->spill_streams_.clear();
                    local_state._shared_state->spill_partitions.pop_front();

                    RETURN_IF_ERROR(SpillRepartitioner::finalize(output_streams));

                    // Push non-empty sub-partitions into the queue
                    for (int i = 0; i < SpillRepartitioner::FANOUT; ++i) {
                        if (output_streams[i] && output_streams[i]->get_written_bytes() > 0) {
                            std::deque<vectorized::SpillStreamSPtr> sub_streams;
                            sub_streams.push_back(std::move(output_streams[i]));
                            local_state._spill_partition_queue.emplace_back(std::move(sub_streams),
                                                                            1);
                        } else if (output_streams[i]) {
                            ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(
                                    output_streams[i]);
                        }
                    }

                    local_state._need_to_merge_data_for_current_partition = true;
                    local_state._current_partition_eos = true;

                    // If all original partitions done, switch to queue
                    if (local_state._shared_state->spill_partitions.empty()) {
                        if (!local_state._spill_partition_queue.empty()) {
                            local_state._processing_spill_queue = true;
                            local_state._need_to_setup_queue_partition = true;
                        }
                    }

                    *eos = false;
                    return Status::OK();
                }

                return Status::OK();
            }
        }

        local_state._need_to_merge_data_for_current_partition = false;
    }

    // not spilled in sink or current partition still has data
    auto* runtime_state = local_state._runtime_state.get();
    local_state._shared_state->in_mem_shared_state->aggregate_data_container->init_once();
    status = _agg_source_operator->get_block(runtime_state, block, eos);
    if (!local_state._shared_state->is_spilled) {
        auto* source_local_state =
                local_state._runtime_state->get_local_state(_agg_source_operator->operator_id());
        local_state.update_profile<false>(source_local_state->custom_profile());
    }

    RETURN_IF_ERROR(status);
    if (*eos) {
        if (local_state._shared_state->is_spilled) {
            auto* source_local_state = local_state._runtime_state->get_local_state(
                    _agg_source_operator->operator_id());
            local_state.update_profile<true>(source_local_state->custom_profile());

            if (!local_state._shared_state->spill_partitions.empty()) {
                local_state._current_partition_eos = true;
                local_state._need_to_merge_data_for_current_partition = true;
                status = _agg_source_operator->reset_hash_table(runtime_state);
                RETURN_IF_ERROR(status);
                *eos = false;
            } else if (!local_state._spill_partition_queue.empty()) {
                // All original partitions done, switch to queue processing.
                local_state._processing_spill_queue = true;
                local_state._need_to_setup_queue_partition = true;
                status = _agg_source_operator->reset_hash_table(runtime_state);
                RETURN_IF_ERROR(status);
                *eos = false;
            }
        }
    }
    local_state.reached_limit(block, eos);
    return Status::OK();
}

Status PartitionedAggLocalState::setup_in_memory_agg_op(RuntimeState* state) {
    _runtime_state = RuntimeState::create_unique(
            state->fragment_instance_id(), state->query_id(), state->fragment_id(),
            state->query_options(), TQueryGlobals {}, state->exec_env(), state->get_query_ctx());
    _runtime_state->set_task_execution_context(state->get_task_execution_context().lock());
    _runtime_state->set_be_number(state->be_number());

    _runtime_state->set_desc_tbl(&state->desc_tbl());
    _runtime_state->resize_op_id_to_local_state(state->max_operator_id());
    _runtime_state->set_runtime_filter_mgr(state->local_runtime_filter_mgr());

    auto& parent = Base::_parent->template cast<Parent>();

    DCHECK(Base::_shared_state->in_mem_shared_state);
    LocalStateInfo state_info {.parent_profile = _internal_runtime_profile.get(),
                               .scan_ranges = {},
                               .shared_state = Base::_shared_state->in_mem_shared_state,
                               .shared_state_map = {},
                               .task_idx = 0};

    RETURN_IF_ERROR(
            parent._agg_source_operator->setup_local_state(_runtime_state.get(), state_info));

    auto* source_local_state =
            _runtime_state->get_local_state(parent._agg_source_operator->operator_id());
    DCHECK(source_local_state != nullptr);
    return source_local_state->open(state);
}

Status PartitionedAggLocalState::_recover_spill_data_from_disk(RuntimeState* state,
                                                               const UniqueId& query_id) {
    Status status;
    Defer defer {[&]() {
        if (!status.ok() || state->is_cancelled()) {
            if (!status.ok()) {
                LOG(WARNING) << fmt::format(
                        "Query:{}, agg probe:{}, task:{}, recover agg data error:{}",
                        print_id(query_id), _parent->node_id(), state->task_id(), status);
            }
            _shared_state->close();
        }
    }};
    bool has_agg_data = false;
    size_t accumulated_blocks_size = 0;
    while (!state->is_cancelled() && !has_agg_data && !_shared_state->spill_partitions.empty()) {
        while (!_shared_state->spill_partitions[0]->spill_streams_.empty() &&
               !state->is_cancelled() && !has_agg_data) {
            auto& stream = _shared_state->spill_partitions[0]->spill_streams_[0];
            stream->set_read_counters(operator_profile());
            vectorized::Block block;
            bool eos = false;
            while (!eos && !state->is_cancelled()) {
                {
                    DBUG_EXECUTE_IF("fault_inject::partitioned_agg_source::recover_spill_data", {
                        status = Status::Error<INTERNAL_ERROR>(
                                "fault_inject partitioned_agg_source "
                                "recover_spill_data failed");
                    });
                    if (status.ok()) {
                        status = stream->read_next_block_sync(&block, &eos);
                    }
                }
                RETURN_IF_ERROR(status);

                if (!block.empty()) {
                    has_agg_data = true;
                    accumulated_blocks_size += block.allocated_bytes();
                    _blocks.emplace_back(std::move(block));

                    if (accumulated_blocks_size >=
                        vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM) {
                        break;
                    }
                }
            }

            _current_partition_eos = eos;

            if (_current_partition_eos) {
                ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
                _shared_state->spill_partitions[0]->spill_streams_.pop_front();
            }
        }

        if (_shared_state->spill_partitions[0]->spill_streams_.empty()) {
            _shared_state->spill_partitions.pop_front();
        }
    }

    VLOG_DEBUG << fmt::format(
            "Query:{}, agg probe:{}, task:{}, recover partitioned finished, partitions "
            "left:{}, bytes read:{}",
            print_id(query_id), _parent->node_id(), state->task_id(),
            _shared_state->spill_partitions.size(), accumulated_blocks_size);
    return status;
}

Status PartitionedAggLocalState::recover_blocks_from_disk(RuntimeState* state, bool& has_data) {
    const auto query_id = state->query_id();

    if (_shared_state->spill_partitions.empty()) {
        _shared_state->close();
        has_data = false;
        return Status::OK();
    }

    has_data = true;
    auto exception_catch_func = [this, state, query_id]() {
        DBUG_EXECUTE_IF("fault_inject::partitioned_agg_source::merge_spill_data_cancel", {
            auto st = Status::InternalError(
                    "fault_inject partitioned_agg_source "
                    "merge spill data canceled");
            state->get_query_ctx()->cancel(st);
            return st;
        });

        auto status = [&]() {
            RETURN_IF_CATCH_EXCEPTION({ return _recover_spill_data_from_disk(state, query_id); });
        }();
        LOG_IF(INFO, !status.ok()) << fmt::format(
                "Query:{}, agg probe:{}, task:{}, recover exception:{}", print_id(query_id),
                _parent->node_id(), state->task_id(), status.to_string());
        return status;
    };

    DBUG_EXECUTE_IF("fault_inject::partitioned_agg_source::submit_func", {
        return Status::Error<INTERNAL_ERROR>(
                "fault_inject partitioned_agg_source submit_func failed");
    });

    VLOG_DEBUG << fmt::format(
            "Query:{}, agg probe:{}, task:{}, begin to recover, partitions left:{}, ",
            print_id(query_id), _parent->node_id(), state->task_id(),
            _shared_state->spill_partitions.size());
    return SpillRecoverRunnable(state, operator_profile(), exception_catch_func).run();
}

Status PartitionedAggLocalState::recover_blocks_from_queue_partition(
        RuntimeState* state, AggSpillPartitionInfo& partition, bool& has_data) {
    has_data = false;
    size_t accumulated_blocks_size = 0;

    while (!partition.streams.empty() && !state->is_cancelled()) {
        auto& stream = partition.streams.front();
        stream->set_read_counters(operator_profile());
        bool eos = false;

        while (!eos && !state->is_cancelled()) {
            vectorized::Block block;
            RETURN_IF_ERROR(stream->read_next_block_sync(&block, &eos));

            if (!block.empty()) {
                has_data = true;
                accumulated_blocks_size += block.allocated_bytes();
                _blocks.emplace_back(std::move(block));

                if (accumulated_blocks_size >= vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM) {
                    _current_queue_partition_eos = false;
                    return Status::OK();
                }
            }
        }

        if (eos) {
            ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
            partition.streams.pop_front();
        }
    }

    // All streams consumed.
    _current_queue_partition_eos = true;
    if (!has_data && _blocks.empty()) {
        has_data = false;
    }
    return Status::OK();
}

Status PartitionedAggLocalState::repartition_agg_partition(RuntimeState* state,
                                                           AggSpillPartitionInfo& partition) {
    auto& p = _parent->cast<PartitionedAggSourceOperatorX>();
    const int new_level = partition.level + 1;

    if (new_level >= SpillRepartitioner::MAX_DEPTH) {
        return Status::InternalError(
                "Agg spill repartition exceeded max depth {}. "
                "Likely due to extreme data skew.",
                SpillRepartitioner::MAX_DEPTH);
    }

    VLOG_DEBUG << fmt::format(
            "Query:{}, agg source:{}, task:{}, repartitioning agg partition at level {} to "
            "level {}, streams: {}, total bytes: {}",
            print_id(state->query_id()), p.node_id(), state->task_id(), partition.level, new_level,
            partition.streams.size(), PrettyPrinter::print_bytes(partition.total_bytes()));

    // Determine key column info from the in-memory shared state.
    // Spill block format: [key_col_0, key_col_1, ..., value_col_0, value_col_1, ...]
    auto* in_mem_state = _shared_state->in_mem_shared_state;
    size_t num_keys = in_mem_state->probe_expr_ctxs.size();
    std::vector<size_t> key_column_indices(num_keys);
    std::vector<vectorized::DataTypePtr> key_data_types(num_keys);
    for (size_t i = 0; i < num_keys; ++i) {
        key_column_indices[i] = i;
        key_data_types[i] = in_mem_state->probe_expr_ctxs[i]->root()->data_type();
    }

    _repartitioner.init_with_key_columns(std::move(key_column_indices), std::move(key_data_types),
                                         operator_profile());

    // Create FANOUT output streams
    std::vector<vectorized::SpillStreamSPtr> output_streams;
    RETURN_IF_ERROR(SpillRepartitioner::create_output_streams(
            state, p.node_id(), fmt::format("agg_repart_l{}", new_level), operator_profile(),
            output_streams));

    // Repartition all streams in the partition
    for (auto& stream : partition.streams) {
        if (!stream) {
            continue;
        }
        if (stream->get_written_bytes() == 0) {
            ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
            stream.reset();
            continue;
        }
        stream->set_read_counters(operator_profile());
        bool done = false;
        while (!done && !state->is_cancelled()) {
            RETURN_IF_ERROR(_repartitioner.repartition(state, stream, output_streams, &done));
        }
        ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
        stream.reset();
    }
    partition.streams.clear();

    RETURN_IF_ERROR(SpillRepartitioner::finalize(output_streams));

    // Push non-empty sub-partitions into the queue
    for (int i = 0; i < SpillRepartitioner::FANOUT; ++i) {
        if (output_streams[i] && output_streams[i]->get_written_bytes() > 0) {
            std::deque<vectorized::SpillStreamSPtr> sub_streams;
            sub_streams.push_back(std::move(output_streams[i]));
            _spill_partition_queue.emplace_back(std::move(sub_streams), new_level);
        } else if (output_streams[i]) {
            ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(output_streams[i]);
        }
    }

    VLOG_DEBUG << fmt::format("Query:{}, agg source:{}, task:{}, repartition done, queue size: {}",
                              print_id(state->query_id()), p.node_id(), state->task_id(),
                              _spill_partition_queue.size());
    return Status::OK();
}

Status PartitionedAggLocalState::flush_hash_table_to_sub_streams(
        RuntimeState* state, std::vector<vectorized::SpillStreamSPtr>& output_streams) {
    auto& p = _parent->cast<PartitionedAggSourceOperatorX>();
    auto* runtime_state = _runtime_state.get();

    // Determine key column info for routing.
    auto* in_mem_state = _shared_state->in_mem_shared_state;
    size_t num_keys = in_mem_state->probe_expr_ctxs.size();
    std::vector<size_t> key_column_indices(num_keys);
    std::vector<vectorized::DataTypePtr> key_data_types(num_keys);
    for (size_t i = 0; i < num_keys; ++i) {
        key_column_indices[i] = i;
        key_data_types[i] = in_mem_state->probe_expr_ctxs[i]->root()->data_type();
    }

    // Reuse the repartitioner in column-index mode.
    _repartitioner.init_with_key_columns(std::move(key_column_indices), std::move(key_data_types),
                                         operator_profile());

    // Drain the hash table by calling get_serialized_block() on the inner agg operator.
    // This outputs blocks in the serialized intermediate format (key cols + serialized agg
    // state cols), which is the same format as the spill block and can be re-merged later.
    in_mem_state->aggregate_data_container->init_once();
    bool inner_eos = false;
    while (!inner_eos && !state->is_cancelled()) {
        vectorized::Block block;
        RETURN_IF_ERROR(
                p._agg_source_operator->get_serialized_block(runtime_state, &block, &inner_eos));
        if (!block.empty()) {
            RETURN_IF_ERROR(_repartitioner.route_block(state, block, output_streams));
        }
    }

    // Reset the hash table for the next partition.
    RETURN_IF_ERROR(p._agg_source_operator->reset_hash_table(runtime_state));

    return Status::OK();
}

bool PartitionedAggLocalState::is_blockable() const {
    return _shared_state->is_spilled;
}

Status PartitionedAggSourceOperatorX::_pull_from_spill_queue(PartitionedAggLocalState& local_state,
                                                             RuntimeState* state,
                                                             vectorized::Block* block, bool* eos) {
    *eos = false;

    if (local_state._need_to_setup_queue_partition) {
        // If the queue is empty, we're done.
        if (local_state._spill_partition_queue.empty()) {
            *eos = true;
            return Status::OK();
        }

        // Pop next partition to process.
        local_state._current_queue_partition =
                std::move(local_state._spill_partition_queue.front());
        local_state._spill_partition_queue.pop_front();
        local_state._blocks.clear();
        local_state._current_queue_partition_eos = false;
        local_state._need_to_merge_for_queue_partition = true;

        VLOG_DEBUG << fmt::format(
                "Query:{}, agg source:{}, task:{},"
                " processing queue partition at level:{}, queue remaining:{},"
                " partition bytes:{}",
                print_id(state->query_id()), node_id(), state->task_id(),
                local_state._current_queue_partition.level,
                local_state._spill_partition_queue.size(),
                PrettyPrinter::print_bytes(local_state._current_queue_partition.total_bytes()));

        // Pre-check: if the partition is too large for available memory, repartition.
        static constexpr int64_t HASH_TABLE_OVERHEAD_FACTOR = 3;
        int64_t partition_bytes = local_state._current_queue_partition.total_bytes();
        int64_t estimated_memory = partition_bytes * HASH_TABLE_OVERHEAD_FACTOR;

        auto query_mem_tracker = state->get_query_ctx()->query_mem_tracker();
        int64_t available_memory =
                query_mem_tracker->limit() > 0
                        ? (query_mem_tracker->limit() - query_mem_tracker->consumption())
                        : std::numeric_limits<int64_t>::max();

        if (estimated_memory > available_memory && partition_bytes > 0) {
            VLOG_DEBUG << fmt::format(
                    "Query:{}, agg source:{}, task:{},"
                    " queue partition (level:{}) too large for memory"
                    " (estimated:{}, available:{}), repartitioning directly",
                    print_id(state->query_id()), node_id(), state->task_id(),
                    local_state._current_queue_partition.level,
                    PrettyPrinter::print_bytes(estimated_memory),
                    PrettyPrinter::print_bytes(available_memory));

            Status status = local_state.repartition_agg_partition(
                    state, local_state._current_queue_partition);
            RETURN_IF_ERROR(status);

            local_state._current_queue_partition = AggSpillPartitionInfo {};
            local_state._need_to_setup_queue_partition = true;
            return Status::OK();
        }

        local_state._need_to_setup_queue_partition = false;
    }

    // Merge phase: read blocks from the queue partition and merge into hash table.
    if (local_state._need_to_merge_for_queue_partition) {
        if (local_state._blocks.empty() && !local_state._current_queue_partition_eos) {
            bool has_data = false;
            Status status = local_state.recover_blocks_from_queue_partition(
                    state, local_state._current_queue_partition, has_data);
            RETURN_IF_ERROR(status);
            if (!has_data) {
                *eos = false;
                return Status::OK();
            }
            return Status::OK();
        } else if (!local_state._blocks.empty()) {
            size_t merged_rows = 0;
            while (!local_state._blocks.empty()) {
                auto block_ = std::move(local_state._blocks.front());
                merged_rows += block_.rows();
                local_state._blocks.erase(local_state._blocks.begin());
                Status status = _agg_source_operator->merge_with_serialized_key_helper(
                        local_state._runtime_state.get(), &block_);
                RETURN_IF_ERROR(status);
            }
            local_state._estimate_memory_usage +=
                    _agg_source_operator->get_estimated_memory_size_for_merging(
                            local_state._runtime_state.get(), merged_rows);

            // Mid-merge memory check in queue processing.
            if (!local_state._current_queue_partition_eos) {
                auto query_mem_tracker = state->get_query_ctx()->query_mem_tracker();
                int64_t available_memory =
                        query_mem_tracker->limit() > 0
                                ? (query_mem_tracker->limit() - query_mem_tracker->consumption())
                                : std::numeric_limits<int64_t>::max();

                static constexpr int64_t MIN_AVAILABLE_MEMORY_FOR_MERGE = 32 * 1024 * 1024; // 32MB
                if (available_memory < MIN_AVAILABLE_MEMORY_FOR_MERGE) {
                    VLOG_DEBUG << fmt::format(
                            "Query:{}, agg source:{}, task:{}, mid-merge OOM in queue partition"
                            " (level:{}, available:{}), flushing hash table and repartitioning",
                            print_id(state->query_id()), node_id(), state->task_id(),
                            local_state._current_queue_partition.level,
                            PrettyPrinter::print_bytes(available_memory));

                    int new_level = local_state._current_queue_partition.level + 1;

                    // Create output sub-streams
                    std::vector<vectorized::SpillStreamSPtr> output_streams;
                    RETURN_IF_ERROR(SpillRepartitioner::create_output_streams(
                            state, node_id(), fmt::format("agg_repart_midmerge_l{}", new_level),
                            local_state.operator_profile(), output_streams));

                    // Collect profile before flushing (flush resets the hash table).
                    {
                        auto* source_local_state =
                                local_state._runtime_state->get_local_state(
                                        _agg_source_operator->operator_id());
                        local_state.update_profile<true>(source_local_state->custom_profile());
                    }

                    // Flush hash table data to sub-streams
                    Status status =
                            local_state.flush_hash_table_to_sub_streams(state, output_streams);
                    RETURN_IF_ERROR(status);

                    // Repartition remaining unread streams from queue partition
                    auto* in_mem_state = local_state._shared_state->in_mem_shared_state;
                    size_t num_keys = in_mem_state->probe_expr_ctxs.size();
                    std::vector<size_t> key_indices(num_keys);
                    std::vector<vectorized::DataTypePtr> key_types(num_keys);
                    for (size_t i = 0; i < num_keys; ++i) {
                        key_indices[i] = i;
                        key_types[i] = in_mem_state->probe_expr_ctxs[i]->root()->data_type();
                    }
                    local_state._repartitioner.init_with_key_columns(
                            std::move(key_indices), std::move(key_types),
                            local_state.operator_profile());

                    for (auto& stream : local_state._current_queue_partition.streams) {
                        if (!stream) {
                            continue;
                        }
                        if (stream->get_written_bytes() == 0) {
                            ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(
                                    stream);
                            stream.reset();
                            continue;
                        }
                        stream->set_read_counters(local_state.operator_profile());
                        bool done = false;
                        while (!done && !state->is_cancelled()) {
                            RETURN_IF_ERROR(local_state._repartitioner.repartition(
                                    state, stream, output_streams, &done));
                        }
                        ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
                        stream.reset();
                    }
                    local_state._current_queue_partition.streams.clear();

                    RETURN_IF_ERROR(SpillRepartitioner::finalize(output_streams));

                    // Push non-empty sub-partitions into the queue
                    for (int i = 0; i < SpillRepartitioner::FANOUT; ++i) {
                        if (output_streams[i] && output_streams[i]->get_written_bytes() > 0) {
                            std::deque<vectorized::SpillStreamSPtr> sub_streams;
                            sub_streams.push_back(std::move(output_streams[i]));
                            local_state._spill_partition_queue.emplace_back(std::move(sub_streams),
                                                                            new_level);
                        } else if (output_streams[i]) {
                            ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(
                                    output_streams[i]);
                        }
                    }

                    // Reset and go back to queue setup
                    local_state._current_queue_partition = AggSpillPartitionInfo {};
                    local_state._need_to_setup_queue_partition = true;
                    *eos = false;
                    return Status::OK();
                }

                return Status::OK();
            }
        }

        // All data merged for this queue partition.
        local_state._need_to_merge_for_queue_partition = false;
    }

    // Output phase: read aggregated results from hash table.
    auto* runtime_state = local_state._runtime_state.get();
    local_state._shared_state->in_mem_shared_state->aggregate_data_container->init_once();
    bool inner_eos = false;
    Status status = _agg_source_operator->get_block(runtime_state, block, &inner_eos);
    RETURN_IF_ERROR(status);

    if (inner_eos) {
        auto* source_local_state =
                local_state._runtime_state->get_local_state(_agg_source_operator->operator_id());
        local_state.update_profile<true>(source_local_state->custom_profile());

        // Reset hash table and move to next queue entry.
        status = _agg_source_operator->reset_hash_table(runtime_state);
        RETURN_IF_ERROR(status);

        local_state._current_queue_partition = AggSpillPartitionInfo {};
        local_state._need_to_setup_queue_partition = true;

        if (local_state._spill_partition_queue.empty()) {
            *eos = true;
        }
    }

    local_state.reached_limit(block, eos);
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris::pipeline
