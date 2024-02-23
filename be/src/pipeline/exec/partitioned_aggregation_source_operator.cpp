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

#include <string>

#include "aggregation_source_operator.h"
#include "common/exception.h"
#include "pipeline/exec/operator.h"
#include "vec//utils/util.hpp"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {

PartitionedAggLocalState::PartitionedAggLocalState(RuntimeState* state, OperatorXBase* parent)
        : Base(state, parent) {}

Status PartitionedAggLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    return Status::OK();
}
Status PartitionedAggLocalState::close(RuntimeState* state) {
    return Base::close(state);
}
PartitionedAggSourceOperatorX ::PartitionedAggSourceOperatorX(ObjectPool* pool,
                                                              const TPlanNode& tnode,
                                                              int operator_id,
                                                              const DescriptorTbl& descs,
                                                              bool is_streaming)
        : Base(pool, tnode, operator_id, descs) {
    _agg_source_operator =
            std::make_unique<AggSourceOperatorX>(pool, tnode, operator_id, descs, is_streaming);
}

Status PartitionedAggSourceOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(OperatorXBase::init(tnode, state));
    return _agg_source_operator->init(tnode, state);
}

Status PartitionedAggSourceOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorXBase::prepare(state));
    return _agg_source_operator->prepare(state);
}

Status PartitionedAggSourceOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorXBase::open(state));
    return _agg_source_operator->open(state);
}

Status PartitionedAggSourceOperatorX::close(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorXBase::close(state));
    return _agg_source_operator->close(state);
}

Status PartitionedAggSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                                SourceState& source_state) {
    auto& local_state = get_local_state(state);
    RETURN_IF_ERROR(local_state._status);

    if (local_state._need_setup_in_memory_agg_op) {
        local_state._need_setup_in_memory_agg_op = false;
        RETURN_IF_ERROR(local_state.setup_in_memory_agg_op(state));
    }
    RETURN_IF_ERROR(local_state.initiate_merge_spill_partition_agg_data(state));
    auto* runtime_state = local_state._runtime_state.get();
    RETURN_IF_ERROR(_agg_source_operator->get_block(runtime_state, block, source_state));
    if (SourceState::FINISHED == source_state) {
        auto& local_state = get_local_state(state);
        if (!local_state._shared_state->spill_partitions_.empty()) {
            source_state = SourceState::DEPEND_ON_SOURCE;
        }
    }
    return Status::OK();
}

Status PartitionedAggLocalState::setup_in_memory_agg_op(RuntimeState* state) {
    _runtime_state = RuntimeState::create_unique(
            nullptr, state->fragment_instance_id(), state->query_id(), state->fragment_id(),
            state->query_options(), TQueryGlobals {}, state->exec_env(), state->get_query_ctx());
    _runtime_state->set_query_mem_tracker(state->query_mem_tracker());
    _runtime_state->set_task_execution_context(state->get_task_execution_context().lock());
    _runtime_state->set_be_number(state->be_number());

    _runtime_state->set_desc_tbl(&state->desc_tbl());
    _runtime_state->resize_op_id_to_local_state(state->max_operator_id());
    _runtime_state->set_pipeline_x_runtime_filter_mgr(state->runtime_filter_mgr());

    auto& parent = Base::_parent->template cast<Parent>();

    auto dep = parent._agg_source_operator->get_dependency(
            const_cast<QueryContext*>(_dependency->_query_ctx), _shared_state->_shared_states);
    LocalStateInfo state_info {
            _runtime_profile.get(),
            {},
            _shared_state->_upstream_deps,
            _shared_state->_shared_states[parent._agg_source_operator->operator_id()].get(),
            {},
            0,
            dep};
    LOG(INFO) << "agg node source, id: " << Base::_parent->id()
              << ", operator id: " << parent._agg_source_operator->operator_id()
              << ", setup in mem agg op";
    auto* agg_shared_state =
            _shared_state->_shared_states[parent._agg_source_operator->operator_id()].get();
    DCHECK(agg_shared_state);

    RETURN_IF_ERROR(
            parent._agg_source_operator->setup_local_state(_runtime_state.get(), state_info));

    _in_memory_agg_op_shared_state =
            parent._agg_source_operator->get_shared_state(_runtime_state.get());

    auto* source_local_state =
            _runtime_state->get_local_state(parent._agg_source_operator->operator_id());
    DCHECK(source_local_state != nullptr);
    return source_local_state->open(state);
}

Status PartitionedAggLocalState::initiate_merge_spill_partition_agg_data(RuntimeState* state) {
    {
        std::unique_lock<std::mutex> lk(_merge_spill_lock);
        DCHECK(!_is_merging);
        _is_merging = true;
    }
    _in_memory_agg_op_shared_state->aggregate_data_container->init_once();
    if (_in_memory_agg_op_shared_state->aggregate_data_container->iterator !=
                _in_memory_agg_op_shared_state->aggregate_data_container->end() ||
        _shared_state->spill_partitions_.empty()) {
        {
            std::unique_lock<std::mutex> lk(_merge_spill_lock);
            _is_merging = false;
            _merge_spill_cv.notify_all();
        }
        return Status::OK();
    }

    RETURN_IF_ERROR(_in_memory_agg_op_shared_state->reset_hash_table());
    _dependency->Dependency::block();

    RETURN_IF_ERROR(
            ExecEnv::GetInstance()->spill_stream_mgr()->get_async_task_thread_pool()->submit_func(
                    [this] {
                        Defer defer {[&]() {
                            if (!_status.ok() || _shared_state->spill_partitions_.empty()) {
                                LOG(WARNING) << "agg node id: " << _parent->node_id()
                                             << ", operator id: " << _parent->operator_id()
                                             << " merge spilled agg data finished: " << _status;
                            }
                            _in_memory_agg_op_shared_state->aggregate_data_container->init_once();
                            {
                                std::unique_lock<std::mutex> lk(_merge_spill_lock);
                                _is_merging = false;
                                _dependency->Dependency::set_ready();
                                _merge_spill_cv.notify_all();
                            }
                        }};
                        bool has_agg_data = false;
                        while (!_closed && !has_agg_data &&
                               !_shared_state->spill_partitions_.empty()) {
                            for (auto& stream :
                                 _shared_state->spill_partitions_[0]->spill_streams_) {
                                RETURN_IF_ERROR(_status);
                                vectorized::Block block;
                                bool eos = false;
                                while (!eos) {
                                    _status = stream->read_next_block_sync(&block, &eos);
                                    RETURN_IF_ERROR(_status);

                                    if (!block.empty()) {
                                        has_agg_data = true;
                                        _status = _in_memory_agg_op_shared_state
                                                          ->merge_with_serialized_key_helper<false,
                                                                                             true>(
                                                                  &block);
                                        RETURN_IF_ERROR(_status);
                                    }
                                }
                                (void)ExecEnv::GetInstance()
                                        ->spill_stream_mgr()
                                        ->delete_spill_stream(stream);
                            }
                            _shared_state->spill_partitions_.pop_front();
                        }
                        if (_shared_state->spill_partitions_.empty()) {
                            _shared_state->clear();
                        }
                        return _status;
                    }));
    return Status::WaitForIO("Merging spilt agg data");
}
} // namespace doris::pipeline
