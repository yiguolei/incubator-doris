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

#include "partitioned_hash_join_probe_operator.h"

#include <gen_cpp/Metrics_types.h>
#include <glog/logging.h>

#include <limits>
#include <memory>
#include <utility>

#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "pipeline/pipeline_task.h"
#include "runtime/fragment_mgr.h"
#include "runtime/query_context.h"
#include "util/pretty_printer.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/spill/spill_repartitioner.h"
#include "vec/spill/spill_stream.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

PartitionedHashJoinProbeLocalState::PartitionedHashJoinProbeLocalState(RuntimeState* state,
                                                                       OperatorXBase* parent)
        : PipelineXSpillLocalState(state, parent),
          _child_block(vectorized::Block::create_unique()) {}

Status PartitionedHashJoinProbeLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(PipelineXSpillLocalState::init(state, info));
    init_spill_write_counters();

    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _internal_runtime_profile = std::make_unique<RuntimeProfile>("internal_profile");
    auto& p = _parent->cast<PartitionedHashJoinProbeOperatorX>();

    _partitioned_blocks.resize(p._partition_count);
    _probe_spilling_streams.resize(p._partition_count);
    init_counters();
    return Status::OK();
}

void PartitionedHashJoinProbeLocalState::init_counters() {
    _partition_timer = ADD_TIMER(custom_profile(), "SpillPartitionTime");
    _partition_shuffle_timer = ADD_TIMER(custom_profile(), "SpillPartitionShuffleTime");
    _spill_build_rows = ADD_COUNTER(custom_profile(), "SpillBuildRows", TUnit::UNIT);
    _spill_build_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), "SpillBuildTime", 1);
    _recovery_build_rows = ADD_COUNTER(custom_profile(), "SpillRecoveryBuildRows", TUnit::UNIT);
    _recovery_build_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), "SpillRecoveryBuildTime", 1);
    _spill_probe_rows = ADD_COUNTER(custom_profile(), "SpillProbeRows", TUnit::UNIT);
    _recovery_probe_rows = ADD_COUNTER(custom_profile(), "SpillRecoveryProbeRows", TUnit::UNIT);
    _spill_build_blocks = ADD_COUNTER(custom_profile(), "SpillBuildBlocks", TUnit::UNIT);
    _recovery_build_blocks = ADD_COUNTER(custom_profile(), "SpillRecoveryBuildBlocks", TUnit::UNIT);
    _spill_probe_blocks = ADD_COUNTER(custom_profile(), "SpillProbeBlocks", TUnit::UNIT);
    _spill_probe_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), "SpillProbeTime", 1);
    _recovery_probe_blocks = ADD_COUNTER(custom_profile(), "SpillRecoveryProbeBlocks", TUnit::UNIT);
    _recovery_probe_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), "SpillRecoveryProbeTime", 1);
    _get_child_next_timer = ADD_TIMER_WITH_LEVEL(custom_profile(), "GetChildNextTime", 1);

    _probe_blocks_bytes =
            ADD_COUNTER_WITH_LEVEL(custom_profile(), "ProbeBloksBytesInMem", TUnit::BYTES, 1);
    _memory_usage_reserved =
            ADD_COUNTER_WITH_LEVEL(custom_profile(), "MemoryUsageReserved", TUnit::BYTES, 1);
}

template <bool spilled>
void PartitionedHashJoinProbeLocalState::update_build_custom_profile(
        RuntimeProfile* child_profile) {
    update_profile_from_inner_profile<spilled>("BuildHashTableTime", custom_profile(),
                                               child_profile);
    update_profile_from_inner_profile<spilled>("MergeBuildBlockTime", custom_profile(),
                                               child_profile);
    update_profile_from_inner_profile<spilled>("BuildTableInsertTime", custom_profile(),
                                               child_profile);
    update_profile_from_inner_profile<spilled>("BuildExprCallTime", custom_profile(),
                                               child_profile);
    update_profile_from_inner_profile<spilled>("MemoryUsageBuildBlocks", custom_profile(),
                                               child_profile);
    update_profile_from_inner_profile<spilled>("MemoryUsageHashTable", custom_profile(),
                                               child_profile);
    update_profile_from_inner_profile<spilled>("MemoryUsageBuildKeyArena", custom_profile(),
                                               child_profile);
}

template <bool spilled>
void PartitionedHashJoinProbeLocalState::update_build_common_profile(
        RuntimeProfile* child_profile) {
    update_profile_from_inner_profile<spilled>("MemoryUsage", common_profile(), child_profile);
}

template <bool spilled>
void PartitionedHashJoinProbeLocalState::update_probe_custom_profile(
        RuntimeProfile* child_profile) {
    update_profile_from_inner_profile<spilled>("JoinFilterTimer", custom_profile(), child_profile);
    update_profile_from_inner_profile<spilled>("BuildOutputBlock", custom_profile(), child_profile);
    update_profile_from_inner_profile<spilled>("ProbeRows", custom_profile(), child_profile);
    update_profile_from_inner_profile<spilled>("ProbeExprCallTime", custom_profile(),
                                               child_profile);
    update_profile_from_inner_profile<spilled>("ProbeWhenSearchHashTableTime", custom_profile(),
                                               child_profile);
    update_profile_from_inner_profile<spilled>("ProbeWhenBuildSideOutputTime", custom_profile(),
                                               child_profile);
    update_profile_from_inner_profile<spilled>("ProbeWhenProbeSideOutputTime", custom_profile(),
                                               child_profile);
    update_profile_from_inner_profile<spilled>("NonEqualJoinConjunctEvaluationTime",
                                               custom_profile(), child_profile);
    update_profile_from_inner_profile<spilled>("InitProbeSideTime", custom_profile(),
                                               child_profile);
}

template <bool spilled>
void PartitionedHashJoinProbeLocalState::update_probe_common_profile(
        RuntimeProfile* child_profile) {
    update_profile_from_inner_profile<spilled>("MemoryUsage", common_profile(), child_profile);
}

void PartitionedHashJoinProbeLocalState::update_profile_from_inner() {
    auto& p = _parent->cast<PartitionedHashJoinProbeOperatorX>();
    if (_shared_state->inner_runtime_state) {
        auto* sink_local_state = _shared_state->inner_runtime_state->get_sink_local_state();
        auto* probe_local_state = _shared_state->inner_runtime_state->get_local_state(
                p._inner_probe_operator->operator_id());
        if (_shared_state->is_spilled) {
            update_build_custom_profile<true>(sink_local_state->custom_profile());
            update_probe_custom_profile<true>(probe_local_state->custom_profile());
            update_build_common_profile<true>(sink_local_state->common_profile());
            update_probe_common_profile<true>(probe_local_state->common_profile());
        } else {
            update_build_custom_profile<false>(sink_local_state->custom_profile());
            update_probe_custom_profile<false>(probe_local_state->custom_profile());
            update_build_common_profile<false>(sink_local_state->common_profile());
            update_probe_common_profile<false>(probe_local_state->common_profile());
        }
    }
}

Status PartitionedHashJoinProbeLocalState::open(RuntimeState* state) {
    RETURN_IF_ERROR(PipelineXSpillLocalState::open(state));
    auto& p = _parent->cast<PartitionedHashJoinProbeOperatorX>();
    RETURN_IF_ERROR(p._partitioner->clone(state, _partitioner));

    // Create a FANOUT-sized partitioner for repartitioning.
    // We do this once here to avoid repeated init/prepare/open during repartitioning.
    _fanout_partitioner = std::make_unique<SpillPartitionerType>(SpillRepartitioner::FANOUT);
    RETURN_IF_ERROR(_fanout_partitioner->init(p._probe_exprs));
    RETURN_IF_ERROR(_fanout_partitioner->prepare(state, p._child->row_desc()));
    RETURN_IF_ERROR(_fanout_partitioner->open(state));

    return Status::OK();
}
Status PartitionedHashJoinProbeLocalState::close(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);
    if (_closed) {
        return Status::OK();
    }
    // Clean up any remaining spill partition queue entries
    for (auto& entry : _spill_partition_queue) {
        if (entry.build_stream) {
            ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(entry.build_stream);
        }
        if (entry.probe_stream) {
            ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(entry.probe_stream);
        }
    }
    _spill_partition_queue.clear();
    if (_current_partition.build_stream) {
        ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(
                _current_partition.build_stream);
    }
    if (_current_partition.probe_stream) {
        ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(
                _current_partition.probe_stream);
    }
    _current_partition = SpillPartitionInfo {};
    _queue_probe_blocks.clear();

    RETURN_IF_ERROR(PipelineXSpillLocalState::close(state));
    return Status::OK();
}

Status PartitionedHashJoinProbeLocalState::_execute_spill_probe_blocks(RuntimeState* state,
                                                                       const UniqueId& query_id) {
    SCOPED_TIMER(_spill_probe_timer);

    size_t not_revoked_size = 0;
    auto& p = _parent->cast<PartitionedHashJoinProbeOperatorX>();
    for (uint32_t partition_index = 0; partition_index != p._partition_count; ++partition_index) {
        auto& blocks = _probe_blocks[partition_index];
        auto& partitioned_block = _partitioned_blocks[partition_index];
        if (partitioned_block) {
            const auto size = partitioned_block->allocated_bytes();
            if (size >= vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
                blocks.emplace_back(partitioned_block->to_block());
                partitioned_block.reset();
            } else {
                not_revoked_size += size;
            }
        }

        if (blocks.empty()) {
            continue;
        }

        auto& spilling_stream = _probe_spilling_streams[partition_index];
        if (!spilling_stream) {
            RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
                    state, spilling_stream, print_id(state->query_id()), "hash_probe",
                    _parent->node_id(), std::numeric_limits<int32_t>::max(),
                    std::numeric_limits<size_t>::max(), operator_profile()));
        }

        auto merged_block = vectorized::MutableBlock::create_unique(std::move(blocks.back()));
        blocks.pop_back();

        while (!blocks.empty() && !state->is_cancelled()) {
            auto block = std::move(blocks.back());
            blocks.pop_back();

            RETURN_IF_ERROR(merged_block->merge(std::move(block)));
            DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::spill_probe_blocks", {
                return Status::Error<INTERNAL_ERROR>(
                        "fault_inject partitioned_hash_join_probe "
                        "spill_probe_blocks failed");
            });
        }

        if (!merged_block->empty()) [[likely]] {
            COUNTER_UPDATE(_spill_probe_rows, merged_block->rows());
            RETURN_IF_ERROR(spilling_stream->spill_block(state, merged_block->to_block(), false));
            COUNTER_UPDATE(_spill_probe_blocks, 1);
        }
    }

    COUNTER_SET(_probe_blocks_bytes, int64_t(not_revoked_size));

    VLOG_DEBUG << fmt::format(
            "Query:{}, hash join probe:{}, task:{},"
            " spill_probe_blocks done",
            print_id(query_id), p.node_id(), state->task_id());
    return Status::OK();
}

Status PartitionedHashJoinProbeLocalState::spill_probe_blocks(RuntimeState* state) {
    auto query_id = state->query_id();

    auto exception_catch_func = [this, query_id, state]() {
        DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::spill_probe_blocks_cancel", {
            auto status = Status::InternalError(
                    "fault_inject partitioned_hash_join_probe "
                    "spill_probe_blocks canceled");
            state->get_query_ctx()->cancel(status);
            return status;
        });

        auto status = [&]() {
            RETURN_IF_CATCH_EXCEPTION({ return _execute_spill_probe_blocks(state, query_id); });
        }();
        return status;
    };

    DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::spill_probe_blocks_submit_func", {
        return Status::Error<INTERNAL_ERROR>(
                "fault_inject partitioned_hash_join_probe spill_probe_blocks "
                "submit_func failed");
    });

    SpillNonSinkRunnable spill_runnable(state, operator_profile(), exception_catch_func);
    return spill_runnable.run();
}

Status PartitionedHashJoinProbeLocalState::finish_spilling(uint32_t partition_index) {
    auto& probe_spilling_stream = _probe_spilling_streams[partition_index];

    if (probe_spilling_stream) {
        RETURN_IF_ERROR(probe_spilling_stream->spill_eof());
        probe_spilling_stream->set_read_counters(operator_profile());
    }

    return Status::OK();
}

Status PartitionedHashJoinProbeLocalState::recover_build_blocks_from_disk(
        RuntimeState* state, uint32_t partition_index, bool& recovered_data_available) {
    VLOG_DEBUG << fmt::format(
            "Query:{}, hash join probe:{}, task:{},"
            " partition:{}, recover_build_blocks_from_disk",
            print_id(state->query_id()), _parent->node_id(), state->task_id(), partition_index);
    auto& spilled_stream = _shared_state->spilled_streams[partition_index];
    // recovered_data_available indicates whether this recover call produced (or
    // attempted to produce) at least one batch of recovered data that the caller
    // should consume before continuing. This function performs synchronous
    // reads (via SpillRecoverRunnable::run) and may return after producing a
    // partial batch (to avoid long blocking); callers should yield when this
    // flag is true and resume processing when the recovered data has been
    // consumed.
    recovered_data_available = false;
    if (!spilled_stream) {
        return Status::OK();
    }
    spilled_stream->set_read_counters(operator_profile());

    auto query_id = state->query_id();

    auto read_func = [this, query_id, state, spilled_stream = spilled_stream, partition_index] {
        SCOPED_TIMER(_recovery_build_timer);

        bool eos = false;
        VLOG_DEBUG << fmt::format(
                "Query:{}, hash join probe:{}, task:{},"
                " partition:{}, recoverying build data",
                print_id(state->query_id()), _parent->node_id(), state->task_id(), partition_index);
        Status status;
        while (!eos) {
            vectorized::Block block;
            DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recover_build_blocks", {
                status = Status::Error<INTERNAL_ERROR>(
                        "fault_inject partitioned_hash_join_probe "
                        "recover_build_blocks failed");
            });
            if (status.ok()) {
                status = spilled_stream->read_next_block_sync(&block, &eos);
            }
            if (!status.ok()) {
                break;
            }
            COUNTER_UPDATE(_recovery_build_rows, block.rows());
            COUNTER_UPDATE(_recovery_build_blocks, 1);

            if (block.empty()) {
                continue;
            }

            if (UNLIKELY(state->is_cancelled())) {
                LOG(INFO) << fmt::format(
                        "Query:{}, hash join probe:{}, task:{},"
                        " partition:{}, recovery build data canceled",
                        print_id(state->query_id()), _parent->node_id(), state->task_id(),
                        partition_index);
                break;
            }

            if (!_recovered_build_block) {
                _recovered_build_block = vectorized::MutableBlock::create_unique(std::move(block));
            } else {
                DCHECK_EQ(_recovered_build_block->columns(), block.columns());
                status = _recovered_build_block->merge(std::move(block));
                if (!status.ok()) {
                    break;
                }
            }

            if (_recovered_build_block->allocated_bytes() >=
                vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM) {
                break;
            }
        }

        if (eos) {
            ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(spilled_stream);
            _shared_state->spilled_streams[partition_index].reset();
            VLOG_DEBUG << fmt::format(
                    "Query:{}, hash join probe:{}, task:{},"
                    " partition:{}, recovery build data eos",
                    print_id(state->query_id()), _parent->node_id(), state->task_id(),
                    partition_index);
        }
        return status;
    };

    auto exception_catch_func = [read_func, state, query_id]() {
        DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recover_build_blocks_cancel", {
            auto status = Status::InternalError(
                    "fault_inject partitioned_hash_join_probe "
                    "recover_build_blocks canceled");

            state->get_query_ctx()->cancel(status);
            return status;
        });

        auto status = [&]() {
            RETURN_IF_ERROR_OR_CATCH_EXCEPTION(read_func());
            return Status::OK();
        }();

        return status;
    };

    // Signal to caller that recovered data is available (one or more blocks
    // read into internal buffers). The function itself runs the read
    // synchronously but may stop early after producing a batch, so the caller
    // should return/yield and consume the data first.
    recovered_data_available = true;
    DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recovery_build_blocks_submit_func",
                    {
                        return Status::Error<INTERNAL_ERROR>(
                                "fault_inject partitioned_hash_join_probe "
                                "recovery_build_blocks submit_func failed");
                    });

    SpillRecoverRunnable spill_runnable(state, operator_profile(), exception_catch_func);
    return spill_runnable.run();
}

std::string PartitionedHashJoinProbeLocalState::debug_string(int indentation_level) const {
    auto& p = _parent->cast<PartitionedHashJoinProbeOperatorX>();
    bool need_more_input_data;
    if (_shared_state->is_spilled) {
        need_more_input_data = !_child_eos;
    } else if (_shared_state->inner_runtime_state) {
        need_more_input_data = p._inner_probe_operator->need_more_input_data(
                _shared_state->inner_runtime_state.get());
    } else {
        need_more_input_data = true;
    }
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer,
                   "{}, short_circuit_for_probe: {}, is_spilled: {}, child_eos: {}, "
                   "_shared_state->inner_runtime_state: {}, need_more_input_data: {}",
                   PipelineXSpillLocalState<PartitionedHashJoinSharedState>::debug_string(
                           indentation_level),
                   _shared_state ? std::to_string(_shared_state->short_circuit_for_probe) : "NULL",
                   _shared_state->is_spilled, _child_eos,
                   _shared_state->inner_runtime_state != nullptr, need_more_input_data);
    return fmt::to_string(debug_string_buffer);
}

Status PartitionedHashJoinProbeLocalState::recover_probe_blocks_from_disk(
        RuntimeState* state, uint32_t partition_index, bool& recovered_data_available) {
    auto& spilled_stream = _probe_spilling_streams[partition_index];
    recovered_data_available = false;
    if (!spilled_stream) {
        return Status::OK();
    }

    spilled_stream->set_read_counters(operator_profile());
    auto& blocks = _probe_blocks[partition_index];

    auto query_id = state->query_id();

    auto read_func = [this, query_id, partition_index, &spilled_stream, &blocks] {
        SCOPED_TIMER(_recovery_probe_timer);

        vectorized::Block block;
        bool eos = false;
        Status st;
        DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recover_probe_blocks", {
            st = Status::Error<INTERNAL_ERROR>(
                    "fault_inject partitioned_hash_join_probe recover_probe_blocks failed");
        });

        size_t read_size = 0;
        while (!eos && !_state->is_cancelled() && st.ok()) {
            st = spilled_stream->read_next_block_sync(&block, &eos);
            if (!st.ok()) {
                break;
            } else if (!block.empty()) {
                COUNTER_UPDATE(_recovery_probe_rows, block.rows());
                COUNTER_UPDATE(_recovery_probe_blocks, 1);
                read_size += block.allocated_bytes();
                blocks.emplace_back(std::move(block));
            }

            if (read_size >= vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM) {
                break;
            }
        }
        if (eos) {
            VLOG_DEBUG << fmt::format(
                    "Query:{}, hash join probe:{}, task:{},"
                    " partition:{}, recovery probe data done",
                    print_id(query_id), _parent->node_id(), _state->task_id(), partition_index);
            ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(spilled_stream);
            spilled_stream.reset();
        }
        return st;
    };

    auto exception_catch_func = [read_func, state, query_id]() {
        DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recover_probe_blocks_cancel", {
            auto status = Status::InternalError(
                    "fault_inject partitioned_hash_join_probe "
                    "recover_probe_blocks canceled");
            state->get_query_ctx()->cancel(status);
            return status;
        });

        auto status = [&]() {
            RETURN_IF_ERROR_OR_CATCH_EXCEPTION(read_func());
            return Status::OK();
        }();

        return status;
    };

    // See comment in recover_build_blocks_from_disk: this function performs
    // synchronous reads up to a batch threshold and sets
    // recovered_data_available=true to tell the caller to yield and consume the
    // recovered probe blocks before continuing.
    recovered_data_available = true;
    DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::recovery_probe_blocks_submit_func",
                    {
                        return Status::Error<INTERNAL_ERROR>(
                                "fault_inject partitioned_hash_join_probe "
                                "recovery_probe_blocks submit_func failed");
                    });
    return SpillRecoverRunnable(state, operator_profile(), exception_catch_func).run();
}

bool PartitionedHashJoinProbeLocalState::is_blockable() const {
    return _shared_state->is_spilled;
}

Status PartitionedHashJoinProbeLocalState::recover_build_blocks_from_partition(
        RuntimeState* state, SpillPartitionInfo& partition_info, bool& recovered_data_available) {
    // recovered_data_available signals the caller that this synchronous
    // recovery call produced (or attempted to produce) at least one batch of
    // recovered build data stored in _recovered_build_block. Callers should
    // yield and consume the recovered data before continuing further
    // processing for this partition.
    recovered_data_available = false;
    auto& build_stream = partition_info.build_stream;
    if (!build_stream || !build_stream->ready_for_reading()) {
        return Status::OK();
    }
    build_stream->set_read_counters(operator_profile());

    auto query_id = state->query_id();
    auto read_func = [this, state, &build_stream] {
        SCOPED_TIMER(_recovery_build_timer);
        bool eos = false;
        Status status;
        while (!eos) {
            vectorized::Block block;
            status = build_stream->read_next_block_sync(&block, &eos);
            if (!status.ok()) {
                break;
            }
            COUNTER_UPDATE(_recovery_build_rows, block.rows());
            COUNTER_UPDATE(_recovery_build_blocks, 1);
            if (block.empty()) {
                continue;
            }
            if (UNLIKELY(state->is_cancelled())) {
                break;
            }
            if (!_recovered_build_block) {
                _recovered_build_block = vectorized::MutableBlock::create_unique(std::move(block));
            } else {
                status = _recovered_build_block->merge(std::move(block));
                if (!status.ok()) {
                    break;
                }
            }
            if (_recovered_build_block->allocated_bytes() >=
                vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM) {
                break;
            }
        }
        if (eos) {
            ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(build_stream);
            build_stream.reset();
        }
        return status;
    };

    auto exception_catch_func = [read_func, state, query_id]() {
        auto status = [&]() {
            RETURN_IF_ERROR_OR_CATCH_EXCEPTION(read_func());
            return Status::OK();
        }();
        return status;
    };

    recovered_data_available = true;
    return SpillRecoverRunnable(state, operator_profile(), exception_catch_func).run();
}

Status PartitionedHashJoinProbeLocalState::recover_probe_blocks_from_partition(
        RuntimeState* state, SpillPartitionInfo& partition_info, bool& recovered_data_available) {
    // recovered_data_available: this call performs synchronous reads of probe
    // blocks into _queue_probe_blocks up to a batch threshold and sets this
    // flag to true when data is available. Caller should return/yield and
    // then consume the recovered probe blocks before continuing.
    recovered_data_available = false;
    auto& probe_stream = partition_info.probe_stream;
    if (!probe_stream || !probe_stream->ready_for_reading()) {
        return Status::OK();
    }
    probe_stream->set_read_counters(operator_profile());

    // For multi-level queue partitions, store recovered probe blocks in _queue_probe_blocks.
    auto& blocks = _queue_probe_blocks;
    auto query_id = state->query_id();

    auto read_func = [this, state, &probe_stream, &blocks] {
        SCOPED_TIMER(_recovery_probe_timer);
        vectorized::Block block;
        bool eos = false;
        Status st;
        size_t read_size = 0;
        while (!eos && !state->is_cancelled() && st.ok()) {
            st = probe_stream->read_next_block_sync(&block, &eos);
            if (!st.ok()) {
                break;
            }
            if (!block.empty()) {
                COUNTER_UPDATE(_recovery_probe_rows, block.rows());
                COUNTER_UPDATE(_recovery_probe_blocks, 1);
                read_size += block.allocated_bytes();
                blocks.emplace_back(std::move(block));
            }
            if (read_size >= vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM) {
                break;
            }
        }
        if (eos) {
            ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(probe_stream);
            probe_stream.reset();
        }
        return st;
    };

    auto exception_catch_func = [read_func, state, query_id]() {
        auto status = [&]() {
            RETURN_IF_ERROR_OR_CATCH_EXCEPTION(read_func());
            return Status::OK();
        }();
        return status;
    };

    recovered_data_available = true;
    return SpillRecoverRunnable(state, operator_profile(), exception_catch_func).run();
}

Status PartitionedHashJoinProbeLocalState::repartition_current_partition(
        RuntimeState* state, SpillPartitionInfo& partition) {
    auto& p = _parent->cast<PartitionedHashJoinProbeOperatorX>();
    const int new_level = partition.level + 1;

    if (new_level >= SpillRepartitioner::MAX_DEPTH) {
        return Status::InternalError(
                "Hash join spill repartition exceeded max depth {}. "
                "Likely due to extreme data skew.",
                SpillRepartitioner::MAX_DEPTH);
    }

    VLOG_DEBUG << fmt::format(
            "Query:{}, hash join probe:{}, task:{}, repartitioning partition at level {} to "
            "level {}",
            print_id(state->query_id()), p.node_id(), state->task_id(), partition.level, new_level);

    // Create a partitioner with FANOUT as partition count for repartitioning.
    // We clone from _fanout_partitioner which was pre-initialized with FANOUT=8.
    std::unique_ptr<vectorized::PartitionerBase> fanout_clone;
    RETURN_IF_ERROR(_fanout_partitioner->clone(state, fanout_clone));
    _repartitioner.init(std::move(fanout_clone), operator_profile());

    // Repartition build stream
    std::vector<vectorized::SpillStreamSPtr> build_output_streams;
    RETURN_IF_ERROR(SpillRepartitioner::create_output_streams(
            state, p.node_id(), fmt::format("hash_build_repart_l{}", new_level), operator_profile(),
            build_output_streams));

    if (partition.build_stream && partition.build_stream->ready_for_reading()) {
        partition.build_stream->set_read_counters(operator_profile());
        bool done = false;
        while (!done && !state->is_cancelled()) {
            RETURN_IF_ERROR(_repartitioner.repartition(state, partition.build_stream,
                                                       build_output_streams, &done));
        }
        // Input build stream fully consumed, clean up
        ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(partition.build_stream);
        partition.build_stream.reset();
    } else if (partition.build_stream) {
        // Stream exists but not ready for reading (empty or not finalized).
        ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(partition.build_stream);
        partition.build_stream.reset();
    }
    RETURN_IF_ERROR(SpillRepartitioner::finalize(build_output_streams));

    // Repartition probe stream
    std::vector<vectorized::SpillStreamSPtr> probe_output_streams;
    RETURN_IF_ERROR(SpillRepartitioner::create_output_streams(
            state, p.node_id(), fmt::format("hash_probe_repart_l{}", new_level), operator_profile(),
            probe_output_streams));

    if (partition.probe_stream && partition.probe_stream->ready_for_reading()) {
        partition.probe_stream->set_read_counters(operator_profile());
        // Re-init repartitioner with a fresh FANOUT partitioner clone for probe data
        std::unique_ptr<vectorized::PartitionerBase> probe_fanout_clone;
        RETURN_IF_ERROR(_fanout_partitioner->clone(state, probe_fanout_clone));
        _repartitioner.init(std::move(probe_fanout_clone), operator_profile());

        bool done = false;
        while (!done && !state->is_cancelled()) {
            RETURN_IF_ERROR(_repartitioner.repartition(state, partition.probe_stream,
                                                       probe_output_streams, &done));
        }
        // Input probe stream fully consumed, clean up
        ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(partition.probe_stream);
        partition.probe_stream.reset();
    } else if (partition.probe_stream) {
        // Stream exists but not ready for reading (empty or not finalized).
        ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(partition.probe_stream);
        partition.probe_stream.reset();
    }
    RETURN_IF_ERROR(SpillRepartitioner::finalize(probe_output_streams));

    // Push sub-partitions into work queue (only those with build data)
    for (int i = 0; i < SpillRepartitioner::FANOUT; ++i) {
        if (build_output_streams[i] && build_output_streams[i]->get_written_bytes() > 0) {
            _spill_partition_queue.emplace_back(std::move(build_output_streams[i]),
                                                std::move(probe_output_streams[i]), new_level);
        } else {
            // Clean up empty streams
            if (build_output_streams[i]) {
                ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(
                        build_output_streams[i]);
            }
            if (probe_output_streams[i]) {
                ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(
                        probe_output_streams[i]);
            }
        }
    }

    return Status::OK();
}

Status PartitionedHashJoinProbeLocalState::repartition_level0_partition(RuntimeState* state,
                                                                        uint32_t partition_index) {
    auto& p = _parent->cast<PartitionedHashJoinProbeOperatorX>();

    VLOG_DEBUG << fmt::format(
            "Query:{}, hash join probe:{}, task:{}, repartitioning level-0 partition {} "
            "(build data in memory, probe data on disk)",
            print_id(state->query_id()), p.node_id(), state->task_id(), partition_index);

    // Create FANOUT build output streams
    std::vector<vectorized::SpillStreamSPtr> build_output_streams;
    RETURN_IF_ERROR(SpillRepartitioner::create_output_streams(
            state, p.node_id(), "hash_build_repart_l1", operator_profile(), build_output_streams));

    // Repartition the in-memory build data into FANOUT streams.
    // The build data is in partitioned_build_blocks[partition_index].
    auto& build_block_ptr = _shared_state->partitioned_build_blocks[partition_index];
    if (build_block_ptr && build_block_ptr->rows() > 0) {
        // Clone a FANOUT partitioner
        std::unique_ptr<vectorized::PartitionerBase> fanout_clone;
        RETURN_IF_ERROR(_fanout_partitioner->clone(state, fanout_clone));

        auto block = build_block_ptr->to_block();
        build_block_ptr.reset();

        // Compute partitioning on the block
        RETURN_IF_ERROR(fanout_clone->do_partitioning(state, &block));
        const auto& channel_ids = fanout_clone->get_channel_ids();
        const auto rows = block.rows();

        // Build per-partition row index lists
        std::vector<std::vector<uint32_t>> partition_indexes(SpillRepartitioner::FANOUT);
        for (uint32_t i = 0; i < rows; ++i) {
            partition_indexes[channel_ids[i]].emplace_back(i);
        }

        // Write each partition's rows to the corresponding output stream
        for (int i = 0; i < SpillRepartitioner::FANOUT; ++i) {
            if (partition_indexes[i].empty()) {
                continue;
            }
            auto mutable_sub = vectorized::MutableBlock::create_unique(block.clone_empty());
            RETURN_IF_ERROR(mutable_sub->add_rows(
                    &block, partition_indexes[i].data(),
                    partition_indexes[i].data() + partition_indexes[i].size()));
            auto out_block = mutable_sub->to_block();
            RETURN_IF_ERROR(build_output_streams[i]->spill_block(state, out_block, false));
        }
    }
    RETURN_IF_ERROR(SpillRepartitioner::finalize(build_output_streams));

    // Repartition probe stream (on disk)
    std::vector<vectorized::SpillStreamSPtr> probe_output_streams;
    RETURN_IF_ERROR(SpillRepartitioner::create_output_streams(
            state, p.node_id(), "hash_probe_repart_l1", operator_profile(), probe_output_streams));

    auto& probe_stream = _probe_spilling_streams[partition_index];
    if (probe_stream && probe_stream->ready_for_reading()) {
        probe_stream->set_read_counters(operator_profile());
        std::unique_ptr<vectorized::PartitionerBase> probe_fanout_clone;
        RETURN_IF_ERROR(_fanout_partitioner->clone(state, probe_fanout_clone));
        _repartitioner.init(std::move(probe_fanout_clone), operator_profile());

        bool done = false;
        while (!done && !state->is_cancelled()) {
            RETURN_IF_ERROR(
                    _repartitioner.repartition(state, probe_stream, probe_output_streams, &done));
        }
        ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(probe_stream);
        probe_stream.reset();
    } else if (probe_stream) {
        ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(probe_stream);
        probe_stream.reset();
    }

    // Also repartition any in-memory probe blocks for this partition.
    // First, flush any unflushed _partitioned_blocks into _probe_blocks.
    auto& partitioned_block = _partitioned_blocks[partition_index];
    if (partitioned_block && !partitioned_block->empty()) {
        _probe_blocks[partition_index].emplace_back(partitioned_block->to_block());
        partitioned_block.reset();
    }
    if (!_probe_blocks[partition_index].empty()) {
        std::unique_ptr<vectorized::PartitionerBase> probe_fanout_clone2;
        RETURN_IF_ERROR(_fanout_partitioner->clone(state, probe_fanout_clone2));

        for (auto& block : _probe_blocks[partition_index]) {
            if (block.empty()) {
                continue;
            }
            RETURN_IF_ERROR(probe_fanout_clone2->do_partitioning(state, &block));
            const auto& channel_ids = probe_fanout_clone2->get_channel_ids();
            const auto rows = block.rows();

            std::vector<std::vector<uint32_t>> partition_indexes(SpillRepartitioner::FANOUT);
            for (uint32_t i = 0; i < rows; ++i) {
                partition_indexes[channel_ids[i]].emplace_back(i);
            }

            for (int i = 0; i < SpillRepartitioner::FANOUT; ++i) {
                if (partition_indexes[i].empty()) {
                    continue;
                }
                auto mutable_sub = vectorized::MutableBlock::create_unique(block.clone_empty());
                RETURN_IF_ERROR(mutable_sub->add_rows(
                        &block, partition_indexes[i].data(),
                        partition_indexes[i].data() + partition_indexes[i].size()));
                auto out_block = mutable_sub->to_block();
                RETURN_IF_ERROR(probe_output_streams[i]->spill_block(state, out_block, false));
            }
        }
        _probe_blocks[partition_index].clear();
    }
    RETURN_IF_ERROR(SpillRepartitioner::finalize(probe_output_streams));

    // Push non-empty sub-partitions into the work queue
    for (int i = 0; i < SpillRepartitioner::FANOUT; ++i) {
        if (build_output_streams[i] && build_output_streams[i]->get_written_bytes() > 0) {
            _spill_partition_queue.emplace_back(std::move(build_output_streams[i]),
                                                std::move(probe_output_streams[i]), 1);
        } else {
            if (build_output_streams[i]) {
                ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(
                        build_output_streams[i]);
            }
            if (probe_output_streams[i]) {
                ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(
                        probe_output_streams[i]);
            }
        }
    }

    return Status::OK();
}

PartitionedHashJoinProbeOperatorX::PartitionedHashJoinProbeOperatorX(ObjectPool* pool,
                                                                     const TPlanNode& tnode,
                                                                     int operator_id,
                                                                     const DescriptorTbl& descs,
                                                                     uint32_t partition_count)
        : JoinProbeOperatorX<PartitionedHashJoinProbeLocalState>(pool, tnode, operator_id, descs),
          _join_distribution(tnode.hash_join_node.__isset.dist_type ? tnode.hash_join_node.dist_type
                                                                    : TJoinDistributionType::NONE),
          _distribution_partition_exprs(tnode.__isset.distribute_expr_lists
                                                ? tnode.distribute_expr_lists[0]
                                                : std::vector<TExpr> {}),
          _tnode(tnode),
          _descriptor_tbl(descs),
          _partition_count(partition_count) {}

Status PartitionedHashJoinProbeOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(JoinProbeOperatorX::init(tnode, state));
    _op_name = "PARTITIONED_HASH_JOIN_PROBE_OPERATOR";
    auto tnode_ = _tnode;
    tnode_.runtime_filters.clear();

    for (const auto& conjunct : tnode.hash_join_node.eq_join_conjuncts) {
        _probe_exprs.emplace_back(conjunct.left);
    }
    _partitioner = std::make_unique<SpillPartitionerType>(_partition_count);
    RETURN_IF_ERROR(_partitioner->init(_probe_exprs));

    return Status::OK();
}

Status PartitionedHashJoinProbeOperatorX::prepare(RuntimeState* state) {
    // to avoid open _child twice
    auto child = std::move(_child);
    RETURN_IF_ERROR(JoinProbeOperatorX::prepare(state));
    RETURN_IF_ERROR(_inner_probe_operator->set_child(child));
    DCHECK(_build_side_child != nullptr);
    _inner_probe_operator->set_build_side_child(_build_side_child);
    RETURN_IF_ERROR(_inner_sink_operator->set_child(_build_side_child));
    RETURN_IF_ERROR(_inner_probe_operator->prepare(state));
    RETURN_IF_ERROR(_inner_sink_operator->prepare(state));
    _child = std::move(child);
    RETURN_IF_ERROR(_partitioner->prepare(state, _child->row_desc()));
    RETURN_IF_ERROR(_partitioner->open(state));
    return Status::OK();
}

Status PartitionedHashJoinProbeOperatorX::push(RuntimeState* state, vectorized::Block* input_block,
                                               bool eos) const {
    auto& local_state = get_local_state(state);
    const auto rows = input_block->rows();
    auto& partitioned_blocks = local_state._partitioned_blocks;
    if (rows == 0) {
        if (eos) {
            for (uint32_t i = 0; i != _partition_count; ++i) {
                if (partitioned_blocks[i] && !partitioned_blocks[i]->empty()) {
                    local_state._probe_blocks[i].emplace_back(partitioned_blocks[i]->to_block());
                    partitioned_blocks[i].reset();
                }
            }
        }
        return Status::OK();
    }
    {
        SCOPED_TIMER(local_state._partition_timer);
        RETURN_IF_ERROR(local_state._partitioner->do_partitioning(state, input_block));
    }

    std::vector<std::vector<uint32_t>> partition_indexes(_partition_count);
    const auto& channel_ids = local_state._partitioner->get_channel_ids();
    for (uint32_t i = 0; i != rows; ++i) {
        partition_indexes[channel_ids[i]].emplace_back(i);
    }

    SCOPED_TIMER(local_state._partition_shuffle_timer);
    int64_t bytes_of_blocks = 0;
    for (uint32_t i = 0; i != _partition_count; ++i) {
        const auto count = partition_indexes[i].size();
        if (UNLIKELY(count == 0)) {
            continue;
        }

        if (!partitioned_blocks[i]) {
            partitioned_blocks[i] =
                    vectorized::MutableBlock::create_unique(input_block->clone_empty());
        }
        RETURN_IF_ERROR(partitioned_blocks[i]->add_rows(input_block, partition_indexes[i].data(),
                                                        partition_indexes[i].data() + count));

        if (partitioned_blocks[i]->rows() > 2 * 1024 * 1024 ||
            (eos && partitioned_blocks[i]->rows() > 0)) {
            local_state._probe_blocks[i].emplace_back(partitioned_blocks[i]->to_block());
            partitioned_blocks[i].reset();
        } else {
            bytes_of_blocks += partitioned_blocks[i]->allocated_bytes();
        }

        for (auto& block : local_state._probe_blocks[i]) {
            bytes_of_blocks += block.allocated_bytes();
        }
    }

    COUNTER_SET(local_state._probe_blocks_bytes, bytes_of_blocks);

    return Status::OK();
}

Status PartitionedHashJoinProbeOperatorX::_setup_internal_operators(
        PartitionedHashJoinProbeLocalState& local_state, RuntimeState* state) const {
    local_state._shared_state->inner_runtime_state = RuntimeState::create_unique(
            state->fragment_instance_id(), state->query_id(), state->fragment_id(),
            state->query_options(), TQueryGlobals {}, state->exec_env(), state->get_query_ctx());

    local_state._shared_state->inner_runtime_state->set_task_execution_context(
            state->get_task_execution_context().lock());
    local_state._shared_state->inner_runtime_state->set_be_number(state->be_number());

    local_state._shared_state->inner_runtime_state->set_desc_tbl(&state->desc_tbl());
    local_state._shared_state->inner_runtime_state->resize_op_id_to_local_state(-1);
    local_state._shared_state->inner_runtime_state->set_runtime_filter_mgr(
            state->local_runtime_filter_mgr());

    local_state._in_mem_shared_state_sptr = _inner_sink_operator->create_shared_state();

    // set sink local state
    LocalSinkStateInfo info {.task_idx = 0,
                             .parent_profile = local_state._internal_runtime_profile.get(),
                             .sender_id = -1,
                             .shared_state = local_state._in_mem_shared_state_sptr.get(),
                             .shared_state_map = {},
                             .tsink = {}};
    RETURN_IF_ERROR(_inner_sink_operator->setup_local_state(
            local_state._shared_state->inner_runtime_state.get(), info));

    LocalStateInfo state_info {.parent_profile = local_state._internal_runtime_profile.get(),
                               .scan_ranges = {},
                               .shared_state = local_state._in_mem_shared_state_sptr.get(),
                               .shared_state_map = {},
                               .task_idx = 0};
    RETURN_IF_ERROR(_inner_probe_operator->setup_local_state(
            local_state._shared_state->inner_runtime_state.get(), state_info));

    auto* sink_local_state = local_state._shared_state->inner_runtime_state->get_sink_local_state();
    DCHECK(sink_local_state != nullptr);
    RETURN_IF_ERROR(sink_local_state->open(state));

    auto* probe_local_state = local_state._shared_state->inner_runtime_state->get_local_state(
            _inner_probe_operator->operator_id());
    DCHECK(probe_local_state != nullptr);
    RETURN_IF_ERROR(probe_local_state->open(state));

    auto& partitioned_block =
            local_state._shared_state->partitioned_build_blocks[local_state._partition_cursor];
    vectorized::Block block;
    if (partitioned_block && partitioned_block->rows() > 0) {
        block = partitioned_block->to_block();
        partitioned_block.reset();
    }
    DBUG_EXECUTE_IF("fault_inject::partitioned_hash_join_probe::sink", {
        return Status::Error<INTERNAL_ERROR>(
                "fault_inject partitioned_hash_join_probe sink failed");
    });

    RETURN_IF_ERROR(_inner_sink_operator->sink(local_state._shared_state->inner_runtime_state.get(),
                                               &block, true));
    VLOG_DEBUG << fmt::format(
            "Query:{}, hash join probe:{}, task:{},"
            " internal build operator finished, partition:{}, rows:{}, memory usage:{}",
            print_id(state->query_id()), node_id(), state->task_id(), local_state._partition_cursor,
            block.rows(),
            _inner_sink_operator->get_memory_usage(
                    local_state._shared_state->inner_runtime_state.get()));
    return Status::OK();
}

Status PartitionedHashJoinProbeOperatorX::_setup_internal_operators_from_partition(
        PartitionedHashJoinProbeLocalState& local_state, RuntimeState* state) const {
    local_state._shared_state->inner_runtime_state = RuntimeState::create_unique(
            state->fragment_instance_id(), state->query_id(), state->fragment_id(),
            state->query_options(), TQueryGlobals {}, state->exec_env(), state->get_query_ctx());

    local_state._shared_state->inner_runtime_state->set_task_execution_context(
            state->get_task_execution_context().lock());
    local_state._shared_state->inner_runtime_state->set_be_number(state->be_number());

    local_state._shared_state->inner_runtime_state->set_desc_tbl(&state->desc_tbl());
    local_state._shared_state->inner_runtime_state->resize_op_id_to_local_state(-1);
    local_state._shared_state->inner_runtime_state->set_runtime_filter_mgr(
            state->local_runtime_filter_mgr());

    local_state._in_mem_shared_state_sptr = _inner_sink_operator->create_shared_state();

    LocalSinkStateInfo info {.task_idx = 0,
                             .parent_profile = local_state._internal_runtime_profile.get(),
                             .sender_id = -1,
                             .shared_state = local_state._in_mem_shared_state_sptr.get(),
                             .shared_state_map = {},
                             .tsink = {}};
    RETURN_IF_ERROR(_inner_sink_operator->setup_local_state(
            local_state._shared_state->inner_runtime_state.get(), info));

    LocalStateInfo state_info {.parent_profile = local_state._internal_runtime_profile.get(),
                               .scan_ranges = {},
                               .shared_state = local_state._in_mem_shared_state_sptr.get(),
                               .shared_state_map = {},
                               .task_idx = 0};
    RETURN_IF_ERROR(_inner_probe_operator->setup_local_state(
            local_state._shared_state->inner_runtime_state.get(), state_info));

    auto* sink_local_state = local_state._shared_state->inner_runtime_state->get_sink_local_state();
    DCHECK(sink_local_state != nullptr);
    RETURN_IF_ERROR(sink_local_state->open(state));

    auto* probe_local_state = local_state._shared_state->inner_runtime_state->get_local_state(
            _inner_probe_operator->operator_id());
    DCHECK(probe_local_state != nullptr);
    RETURN_IF_ERROR(probe_local_state->open(state));

    // Use the recovered build block from the partition stream
    vectorized::Block block;
    if (local_state._recovered_build_block && local_state._recovered_build_block->rows() > 0) {
        block = local_state._recovered_build_block->to_block();
        local_state._recovered_build_block.reset();
    }

    RETURN_IF_ERROR(_inner_sink_operator->sink(local_state._shared_state->inner_runtime_state.get(),
                                               &block, true));
    VLOG_DEBUG << fmt::format(
            "Query:{}, hash join probe:{}, task:{},"
            " internal build from partition (level:{}) finished, rows:{}, memory usage:{}",
            print_id(state->query_id()), node_id(), state->task_id(),
            local_state._current_partition.level, block.rows(),
            _inner_sink_operator->get_memory_usage(
                    local_state._shared_state->inner_runtime_state.get()));
    return Status::OK();
}

Status PartitionedHashJoinProbeOperatorX::pull(doris::RuntimeState* state,
                                               vectorized::Block* output_block, bool* eos) const {
    auto& local_state = get_local_state(state);

    // If we've exhausted level-0 partitions and are processing the spill queue,
    // delegate to the queue processing path.
    if (local_state._processing_spill_queue) {
        return _pull_from_spill_queue(local_state, state, output_block, eos);
    }

    const auto partition_index = local_state._partition_cursor;
    auto& probe_blocks = local_state._probe_blocks[partition_index];

    if (local_state._recovered_build_block && !local_state._recovered_build_block->empty()) {
        auto& mutable_block = local_state._shared_state->partitioned_build_blocks[partition_index];
        if (!mutable_block) {
            mutable_block = std::move(local_state._recovered_build_block);
        } else {
            RETURN_IF_ERROR(mutable_block->merge(local_state._recovered_build_block->to_block()));
            local_state._recovered_build_block.reset();
        }
    }

    if (local_state._need_to_setup_internal_operators) {
        bool recovered_data_available = false;
        RETURN_IF_ERROR(local_state.recover_build_blocks_from_disk(
                state, local_state._partition_cursor, recovered_data_available));
        if (recovered_data_available) {
            // Recovered some build data — yield so the caller can consume it
            // (it has been stored in _recovered_build_block and will be merged
            // into partitioned_build_blocks on the next invocation).
            return Status::OK();
        }

        *eos = false;
        RETURN_IF_ERROR(local_state.finish_spilling(partition_index));

        // Yield-based memory check: if in low_memory_mode and we have significant build data, trigger repartition
        auto& build_block_ptr =
                local_state._shared_state->partitioned_build_blocks[partition_index];
        int64_t build_data_bytes = build_block_ptr ? build_block_ptr->allocated_bytes() : 0;
        if (local_state.low_memory_mode() &&
            build_data_bytes > vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM) {
            VLOG_DEBUG << fmt::format(
                    "Query:{}, hash join probe:{}, task:{},"
                    " level-0 partition:{} build data triggers repartition due to low memory mode "
                    "({} bytes)",
                    print_id(state->query_id()), node_id(), state->task_id(), partition_index,
                    PrettyPrinter::print_bytes(build_data_bytes));

            RETURN_IF_ERROR(local_state.repartition_level0_partition(state, partition_index));
            local_state._shared_state->partitioned_build_blocks[partition_index].reset();

            local_state._partition_cursor++;
            if (local_state._partition_cursor == _partition_count) {
                if (!local_state._spill_partition_queue.empty()) {
                    local_state._processing_spill_queue = true;
                    local_state._need_to_setup_queue_partition = true;
                } else {
                    *eos = true;
                }
            } else {
                local_state._need_to_setup_internal_operators = true;
            }
            return Status::OK();
        }

        RETURN_IF_ERROR(_setup_internal_operators(local_state, state));
        local_state._need_to_setup_internal_operators = false;
        auto& mutable_block = local_state._partitioned_blocks[partition_index];
        if (mutable_block && !mutable_block->empty()) {
            probe_blocks.emplace_back(mutable_block->to_block());
        }
    }
    bool in_mem_eos = false;
    auto* runtime_state = local_state._shared_state->inner_runtime_state.get();
    while (_inner_probe_operator->need_more_input_data(runtime_state)) {
        if (probe_blocks.empty()) {
            *eos = false;
            bool recovered_data_available = false;
            RETURN_IF_ERROR(local_state.recover_probe_blocks_from_disk(state, partition_index,
                                                                       recovered_data_available));
            if (!recovered_data_available) {
                vectorized::Block block;
                RETURN_IF_ERROR(_inner_probe_operator->push(runtime_state, &block, true));
                VLOG_DEBUG << fmt::format(
                        "Query:{}, hash join probe:{}, task:{},"
                        " partition:{}, has no data to recovery",
                        print_id(state->query_id()), node_id(), state->task_id(), partition_index);
                break;
            } else {
                return Status::OK();
            }
        }

        auto block = std::move(probe_blocks.back());
        probe_blocks.pop_back();
        if (!block.empty()) {
            RETURN_IF_ERROR(_inner_probe_operator->push(runtime_state, &block, false));
        }
    }

    RETURN_IF_ERROR(_inner_probe_operator->pull(
            local_state._shared_state->inner_runtime_state.get(), output_block, &in_mem_eos));

    *eos = false;
    if (in_mem_eos) {
        VLOG_DEBUG << fmt::format(
                "Query:{}, hash join probe:{}, task:{},"
                " partition:{}, probe done",
                print_id(state->query_id()), node_id(), state->task_id(),
                local_state._partition_cursor);
        local_state._partition_cursor++;
        local_state.update_profile_from_inner();
        if (local_state._partition_cursor == _partition_count) {
            // All level-0 partitions done. Check if we have repartitioned partitions to process.
            if (!local_state._spill_partition_queue.empty()) {
                local_state._processing_spill_queue = true;
                local_state._need_to_setup_queue_partition = true;
            } else {
                *eos = true;
            }
        } else {
            local_state._need_to_setup_internal_operators = true;
        }
    }

    return Status::OK();
}

Status PartitionedHashJoinProbeOperatorX::_pull_from_spill_queue(
        PartitionedHashJoinProbeLocalState& local_state, RuntimeState* state,
        vectorized::Block* output_block, bool* eos) const {
    *eos = false;

    if (local_state._need_to_setup_queue_partition) {
        // If the queue is empty AND we don't already have a current
        // partition or recovered build data pending, we're done. It's
        // possible the queue was popped on a previous scheduling and the
        // recovered data is waiting to be processed; in that case we must
        // not return EOS here.
        if (local_state._spill_partition_queue.empty() &&
            !local_state._current_partition.build_stream &&
            !(local_state._recovered_build_block &&
              local_state._recovered_build_block->rows() > 0)) {
            *eos = true;
            return Status::OK();
        }

        // Pop next partition to process
        // Invariant: we only pop a new queue partition when `_current_partition`
        // is empty and we have not already recovered build data for the
        // current partition. `SpillPartitionInfo::build_stream` is used as a
        // sentinel for whether we've populated `_current_partition` from the
        // queue. However, after we recover the partition's build stream into
        // `_recovered_build_block`, `build_stream` may be cleared (read to
        // eos). In that state we must NOT pop the next queued partition — we
        // still need to setup internal operators from the recovered data.
        if (!local_state._current_partition.build_stream && !local_state._recovered_build_block) {
            // Defensive checks about the expected invariants before we pop.
            DCHECK(!local_state._current_partition.build_stream);
            DCHECK(local_state._spill_partition_queue.front().build_stream)
                    << "Expected queued partition to contain build_stream";

            local_state._current_partition = std::move(local_state._spill_partition_queue.front());
            local_state._spill_partition_queue.pop_front();
            local_state._recovered_build_block.reset();
            local_state._queue_probe_blocks.clear();

            VLOG_DEBUG << fmt::format(
                    "Query:{}, hash join probe:{}, task:{},"
                    " processing queue partition at level:{}, queue remaining:{}",
                    print_id(state->query_id()), node_id(), state->task_id(),
                    local_state._current_partition.level,
                    local_state._spill_partition_queue.size());

            // NOTE: removed opportunistic low-memory-mode-triggered repartition here.
            // Repartition decisions for queue partitions should be driven by the
            // pipeline task scheduler (reserve/yield/low-memory mechanism) so
            // this operator no longer force-triggers repartition from inside the
            // pull path. Continue and let the normal recovery/repartition logic
            // (below) proceed under pipeline control.
        }

        // If we've already recovered build data into `_recovered_build_block`
        // (from a previous scheduling), we must only perform the internal
        // operator setup once we've recovered *all* build data for this
        // partition. The recovery logic may return partial batches across
        // multiple scheduling slices to avoid long blocking reads. Only when
        // the partition's `build_stream` has been exhausted (it will be
        // cleared/reset by the recovery code) may we safely build the hash
        // table from the accumulated `_recovered_build_block`.
        //
        // Note: if `_recovered_build_block` contains rows but
        // `_current_partition.build_stream` is still non-null then more build
        // data may still arrive; continue recovery instead of starting the
        // build now.
        if (local_state._recovered_build_block && local_state._recovered_build_block->rows() > 0 &&
            !local_state._current_partition.build_stream) {
            // Defensive check: ensure there is no remaining build stream to read
            // from before starting the build.
            DCHECK(!local_state._current_partition.build_stream);
            RETURN_IF_ERROR(_setup_internal_operators_from_partition(local_state, state));
            local_state._need_to_setup_queue_partition = false;
        } else {
            // Recover build data from the partition's build stream
            bool recovered_data_available = false;
            RETURN_IF_ERROR(local_state.recover_build_blocks_from_partition(
                    state, local_state._current_partition, recovered_data_available));
            if (recovered_data_available) {
                // We read some build data — yield so it can be consumed in the next
                // scheduling of this operator.
                return Status::OK();
            }

            // All build data recovered. Yield once so the pipeline scheduler can
            // re-evaluate reservations based on the recovered data before we
            // actually build the hash table. On the next scheduling of this
            // operator we will fall through and call
            // `_setup_internal_operators_from_partition`.
            return Status::OK();
        }
    }

    // Probe phase: feed probe blocks from the current partition's probe stream
    bool in_mem_eos = false;
    auto* runtime_state = local_state._shared_state->inner_runtime_state.get();
    auto& probe_blocks = local_state._queue_probe_blocks;

    while (_inner_probe_operator->need_more_input_data(runtime_state)) {
        if (probe_blocks.empty()) {
            bool recovered_data_available = false;
            RETURN_IF_ERROR(local_state.recover_probe_blocks_from_partition(
                    state, local_state._current_partition, recovered_data_available));
            if (!recovered_data_available) {
                // No more probe data — send eos to inner probe
                vectorized::Block block;
                RETURN_IF_ERROR(_inner_probe_operator->push(runtime_state, &block, true));
                VLOG_DEBUG << fmt::format(
                        "Query:{}, hash join probe:{}, task:{},"
                        " queue partition (level:{}) probe eos",
                        print_id(state->query_id()), node_id(), state->task_id(),
                        local_state._current_partition.level);
                break;
            } else {
                return Status::OK();
            }
        }

        auto block = std::move(probe_blocks.back());
        probe_blocks.pop_back();
        if (!block.empty()) {
            RETURN_IF_ERROR(_inner_probe_operator->push(runtime_state, &block, false));
        }
    }

    RETURN_IF_ERROR(_inner_probe_operator->pull(runtime_state, output_block, &in_mem_eos));

    if (in_mem_eos) {
        VLOG_DEBUG << fmt::format(
                "Query:{}, hash join probe:{}, task:{},"
                " queue partition (level:{}) probe done",
                print_id(state->query_id()), node_id(), state->task_id(),
                local_state._current_partition.level);
        local_state.update_profile_from_inner();

        // Reset for next queue entry
        local_state._current_partition = SpillPartitionInfo {};
        local_state._need_to_setup_queue_partition = true;
        local_state._queue_probe_blocks.clear();

        if (local_state._spill_partition_queue.empty()) {
            *eos = true;
        }
    }

    return Status::OK();
}

bool PartitionedHashJoinProbeOperatorX::need_more_input_data(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    if (local_state._shared_state->is_spilled) {
        return !local_state._child_eos;
    } else if (local_state._shared_state->inner_runtime_state) {
        return _inner_probe_operator->need_more_input_data(
                local_state._shared_state->inner_runtime_state.get());
    } else {
        return true;
    }
}

size_t PartitionedHashJoinProbeOperatorX::revocable_mem_size(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    // After _child_eos, we may still have recovered build data that is revocable (can be repartitioned)
    size_t revocable_size = 0;
    if (!local_state._child_eos) {
        revocable_size = _revocable_mem_size(state, true);
        if (_child) {
            revocable_size += _child->revocable_mem_size(state);
        }
    } else {
        // After child_eos, only consider build-side memory
        revocable_size = _revocable_mem_size(state, true);
    }
    return revocable_size;
}

size_t PartitionedHashJoinProbeOperatorX::_revocable_mem_size(RuntimeState* state,
                                                              bool force) const {
    const auto spill_size_threshold = force ? vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM
                                            : vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM;
    auto& local_state = get_local_state(state);
    size_t mem_size = 0;
    auto& probe_blocks = local_state._probe_blocks;
    for (uint32_t i = 0; i < _partition_count; ++i) {
        for (auto& block : probe_blocks[i]) {
            mem_size += block.allocated_bytes();
        }

        auto& partitioned_block = local_state._partitioned_blocks[i];
        if (partitioned_block) {
            auto block_bytes = partitioned_block->allocated_bytes();
            if (block_bytes >= spill_size_threshold) {
                mem_size += block_bytes;
            }
        }
    }

    // Include build-side memory that has been recovered but not yet consumed by the hash table.
    // This data is revocable because we can repartition instead of building the hash table.
    if (local_state._recovered_build_block) {
        mem_size += local_state._recovered_build_block->allocated_bytes();
    }
    // Also include build data in partitioned_build_blocks for the current partition being processed.
    if (local_state._partition_cursor < _partition_count) {
        auto& build_block =
                local_state._shared_state->partitioned_build_blocks[local_state._partition_cursor];
        if (build_block) {
            mem_size += build_block->allocated_bytes();
        }
    }

    return mem_size;
}

size_t PartitionedHashJoinProbeOperatorX::get_reserve_mem_size(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    const auto is_spilled = local_state._shared_state->is_spilled;

    // Non-spill path: delegate to the inner probe operator / base class.
    if (!is_spilled) {
        return Base::get_reserve_mem_size(state);
    }

    // Spill path, probe data still flowing in (child not yet EOS):
    // We only need room for incoming probe blocks being partitioned. Reserve
    // one batch worth of spill-write memory; no hash table will be built yet.
    if (!local_state._child_eos) {
        return vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM;
    }

    // Spill path, child EOS — we are in the recovery / build / probe phase.
    // Baseline reservation is one block of spill I/O.
    size_t size_to_reserve = vectorized::SpillStream::MAX_SPILL_WRITE_BATCH_MEM;

    // Determine whether we are *about to build* a hash table for the current
    // partition (level-0 path) or a queue partition (multi-level path).
    //
    // Level-0: _need_to_setup_internal_operators is true when the next pull()
    //          call will build the hash table from partitioned_build_blocks.
    //
    // Queue path: _need_to_setup_queue_partition is true AND recovered build
    //             data is fully available (build_stream exhausted), meaning the
    //             next pull() will call _setup_internal_operators_from_partition.
    const bool about_to_build_level0 = local_state._need_to_setup_internal_operators;
    const bool about_to_build_queue =
            local_state._need_to_setup_queue_partition && local_state._recovered_build_block &&
            local_state._recovered_build_block->rows() > 0 &&
            !local_state._current_partition.build_stream; // stream exhausted → ready to build

    if (about_to_build_level0 || about_to_build_queue) {
        // Estimate rows that will land in the hash table so we can reserve
        // enough for JoinHashTable::first[] + JoinHashTable::next[].
        size_t rows = state->batch_size(); // minimum headroom
        if (about_to_build_level0) {
            const auto& build_block =
                    local_state._shared_state
                            ->partitioned_build_blocks[local_state._partition_cursor];
            if (build_block) {
                rows = std::max(rows, static_cast<size_t>(build_block->rows()));
            }
            if (local_state._recovered_build_block) {
                rows = std::max(rows, rows + local_state._recovered_build_block->rows());
            }
        } else {
            // Queue path: rows come from _recovered_build_block
            rows = std::max(rows, static_cast<size_t>(local_state._recovered_build_block->rows()));
        }

        const size_t bucket_size = hash_join_table_calc_bucket_size(rows);
        size_to_reserve += bucket_size * sizeof(uint32_t); // JoinHashTable::first
        size_to_reserve += rows * sizeof(uint32_t);        // JoinHashTable::next

        if (_join_op == TJoinOp::FULL_OUTER_JOIN || _join_op == TJoinOp::RIGHT_OUTER_JOIN ||
            _join_op == TJoinOp::RIGHT_ANTI_JOIN || _join_op == TJoinOp::RIGHT_SEMI_JOIN) {
            size_to_reserve += rows * sizeof(uint8_t); // JoinHashTable::visited
        }
    }
    // Otherwise (not about to build): we only need the spill I/O baseline
    // already included above — no hash table allocation is imminent.

    COUNTER_SET(local_state._memory_usage_reserved, int64_t(size_to_reserve));
    return size_to_reserve;
}

// Public revoke_memory: called by the pipeline task scheduler when memory
// pressure requires probe-side blocks to be spilled. The scheduler only calls
// _sink->revoke_memory, so the probe operator overrides this interface so the
// scheduler can reach probe-side memory as well (see pipeline_task.cpp).
Status PartitionedHashJoinProbeOperatorX::revoke_memory(
        RuntimeState* state, const std::shared_ptr<SpillContext>& /*spill_context*/) {
    auto& local_state = get_local_state(state);
    VLOG_DEBUG << fmt::format("Query:{}, hash join probe:{}, task:{}, revoke_memory",
                              print_id(state->query_id()), node_id(), state->task_id());
    return local_state.spill_probe_blocks(state);
}

Status PartitionedHashJoinProbeOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                                    bool* eos) {
    *eos = false;
    auto& local_state = get_local_state(state);
    local_state.copy_shared_spill_profile();
    const auto is_spilled = local_state._shared_state->is_spilled;
#ifndef NDEBUG
    Defer eos_check_defer([&] {
        if (*eos) {
            LOG(INFO) << fmt::format(
                    "Query:{}, hash join probe:{}, task:{}, child eos:{}, need spill:{}",
                    print_id(state->query_id()), node_id(), state->task_id(),
                    local_state._child_eos, is_spilled);
        }
    });
#endif

    Defer defer([&]() {
        COUNTER_SET(local_state._memory_usage_reserved,
                    int64_t(local_state.estimate_memory_usage()));
    });

    if (need_more_input_data(state)) {
        {
            SCOPED_TIMER(local_state._get_child_next_timer);
            RETURN_IF_ERROR(_child->get_block_after_projects(state, local_state._child_block.get(),
                                                             &local_state._child_eos));
        }

        SCOPED_TIMER(local_state.exec_time_counter());
        if (local_state._child_block->rows() == 0 && !local_state._child_eos) {
            return Status::OK();
        }

        Defer clear_defer([&] { local_state._child_block->clear_column_data(); });
        if (is_spilled) {
            RETURN_IF_ERROR(push(state, local_state._child_block.get(), local_state._child_eos));
            // Memory revocation for probe blocks is now driven by the pipeline
            // task scheduler via revoke_memory() (see above). No inline check
            // needed here.
        } else {
            DCHECK(local_state._shared_state->inner_runtime_state);
            RETURN_IF_ERROR(_inner_probe_operator->push(
                    local_state._shared_state->inner_runtime_state.get(),
                    local_state._child_block.get(), local_state._child_eos));
        }
    }

    if (!need_more_input_data(state)) {
        SCOPED_TIMER(local_state.exec_time_counter());
        if (is_spilled) {
            RETURN_IF_ERROR(pull(state, block, eos));
        } else {
            RETURN_IF_ERROR(_inner_probe_operator->pull(
                    local_state._shared_state->inner_runtime_state.get(), block, eos));
            local_state.update_profile_from_inner();
        }

        local_state.add_num_rows_returned(block->rows());
        COUNTER_UPDATE(local_state._blocks_returned_counter, 1);
    }
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris::pipeline
