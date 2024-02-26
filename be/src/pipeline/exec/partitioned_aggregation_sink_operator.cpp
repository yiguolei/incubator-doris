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

#include "partitioned_aggregation_sink_operator.h"

#include <cstdint>

#include "aggregation_sink_operator.h"
#include "common/status.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {
PartitionedAggSinkLocalState::PartitionedAggSinkLocalState(DataSinkOperatorXBase* parent,
                                                           RuntimeState* state)
        : PipelineXSinkLocalState<PartitionedAggSinkDependency>(parent, state) {
    _finish_dependency = std::make_shared<FinishDependency>(
            parent->operator_id(), parent->node_id(), parent->get_name() + "_FINISH_DEPENDENCY",
            state->get_query_ctx());
}
Status PartitionedAggSinkLocalState::init(doris::RuntimeState* state,
                                          doris::pipeline::LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(PipelineXSinkLocalState::init(state, info));
    _finish_dependency->block();
    return Status::OK();
}

Status PartitionedAggSinkLocalState::open(RuntimeState* state) {
    RETURN_IF_ERROR(PipelineXSinkLocalState::open(state));
    return setup_in_memory_agg_op(state);
}
Status PartitionedAggSinkLocalState::close(RuntimeState* state, Status exec_status) {
    {
        std::unique_lock<std::mutex> lk(_spill_lock);
        if (_is_spilling) {
            _spill_cv.wait(lk);
        }
    }
    return Status::OK();
}

template <typename LocalStateType>
PartitionedAggSinkOperatorX<LocalStateType>::PartitionedAggSinkOperatorX(ObjectPool* pool,
                                                                         int operator_id,
                                                                         const TPlanNode& tnode,
                                                                         const DescriptorTbl& descs,
                                                                         bool is_streaming)
        : DataSinkOperatorX<LocalStateType>(operator_id, tnode.node_id),
          _partition_exprs(tnode.__isset.distribute_expr_lists ? tnode.distribute_expr_lists[0]
                                                               : std::vector<TExpr> {}),
          _has_probe_expr(!tnode.agg_node.grouping_exprs.empty()),
          _needs_finalize(tnode.agg_node.need_finalize),
          _is_colocate(tnode.agg_node.__isset.is_colocate && tnode.agg_node.is_colocate) {
    _agg_sink_operator =
            std::make_unique<AggSinkOperatorX<>>(pool, operator_id, tnode, descs, is_streaming);
}

template <typename LocalStateType>
Status PartitionedAggSinkOperatorX<LocalStateType>::init(const TPlanNode& tnode,
                                                         RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<LocalStateType>::init(tnode, state));
    if (state->query_options().__isset.external_agg_partition_bits) {
        _spill_partition_count_bits = state->query_options().external_agg_partition_bits;
    }

    _agg_sink_operator->set_dests_id(DataSinkOperatorX<LocalStateType>::dests_id());
    RETURN_IF_ERROR(_agg_sink_operator->set_child(DataSinkOperatorX<LocalStateType>::_child_x));
    return _agg_sink_operator->init(tnode, state);
}

template <typename LocalStateType>
Status PartitionedAggSinkOperatorX<LocalStateType>::prepare(RuntimeState* state) {
    return _agg_sink_operator->prepare(state);
}

template <typename LocalStateType>
Status PartitionedAggSinkOperatorX<LocalStateType>::open(RuntimeState* state) {
    return _agg_sink_operator->open(state);
}

template <typename LocalStateType>
Status PartitionedAggSinkOperatorX<LocalStateType>::close(RuntimeState* state) {
    return _agg_sink_operator->close(state);
}
template <typename LocalStateType>
Status PartitionedAggSinkOperatorX<LocalStateType>::sink(doris::RuntimeState* state,
                                                         vectorized::Block* in_block,
                                                         SourceState source_state) {
    auto& local_state = get_local_state(state);
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
    RETURN_IF_ERROR(local_state.Base::_shared_state->_sink_status);
    local_state._source_state = source_state;
    auto* runtime_state = local_state._runtime_state.get();
    RETURN_IF_ERROR(
            _agg_sink_operator->sink(runtime_state, in_block, SourceState::DEPEND_ON_SOURCE));
    if (source_state == SourceState::FINISHED) {
        if (revocable_mem_size(state) > 0) {
            RETURN_IF_ERROR(revoke_memory(state));
        } else {
            local_state._dependency->set_ready_to_read();
        }
    }
    return Status::OK();
}
template <typename LocalStateType>
Status PartitionedAggSinkOperatorX<LocalStateType>::revoke_memory(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    RETURN_IF_ERROR(local_state.revoke_memory(state));
    return Status::WaitForIO("Spilling");
}
template <typename LocalStateType>
size_t PartitionedAggSinkOperatorX<LocalStateType>::revocable_mem_size(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    if (!local_state.Base::_shared_state->_sink_status.ok()) {
        return UINT64_MAX;
    }
    auto* runtime_state = local_state._runtime_state.get();
    auto size = _agg_sink_operator->get_revocable_mem_size(runtime_state);
    // LOG(INFO) << "agg node id: " << id() << ", operator id: " << operator_id()
    //           << ", revocable mem size: " << size;
    return size;
}

Status PartitionedAggSinkLocalState::setup_in_memory_agg_op(RuntimeState* state) {
    Base::_shared_state->_shared_states.clear();
    Base::_shared_state->_upstream_deps.clear();
    Base::_shared_state->_downstream_deps.clear();
    _runtime_state = RuntimeState::create_unique(
            nullptr, state->fragment_instance_id(), state->query_id(), state->fragment_id(),
            state->query_options(), TQueryGlobals {}, state->exec_env(), state->get_query_ctx());
    _runtime_state->set_query_mem_tracker(state->query_mem_tracker());
    _runtime_state->set_task_execution_context(state->get_task_execution_context().lock());
    _runtime_state->set_be_number(state->be_number());

    _runtime_state->set_desc_tbl(&state->desc_tbl());
    _runtime_state->set_pipeline_x_runtime_filter_mgr(state->runtime_filter_mgr());

    auto& parent = Base::_parent->template cast<Parent>();
    parent._agg_sink_operator->get_dependency(Base::_shared_state->_downstream_deps,
                                              Base::_shared_state->_shared_states,
                                              const_cast<QueryContext*>(_dependency->_query_ctx));

    LocalSinkStateInfo info {0, Base::profile(), -1, Base::_shared_state->_downstream_deps, {}, {}};
    RETURN_IF_ERROR(parent._agg_sink_operator->setup_local_state(_runtime_state.get(), info));
    _in_memory_agg_op_shared_state =
            parent._agg_sink_operator->get_shared_state(_runtime_state.get());
    Base::_shared_state->init_spill_params(parent._spill_partition_count_bits,
                                           _in_memory_agg_op_shared_state);
    auto* sink_local_state =
            _runtime_state->get_sink_local_state(parent._agg_sink_operator->operator_id());
    DCHECK(sink_local_state != nullptr);
    return sink_local_state->open(state);
}

Status PartitionedAggSinkLocalState::revoke_memory(RuntimeState* state) {
    LOG(INFO) << "agg node " << Base::_parent->id() << " operator " << Base::_parent->operator_id()
              << " revoke_memory"
              << ", source status: " << (int)_source_state;
    RETURN_IF_ERROR(Base::_shared_state->_sink_status);
    DCHECK(!_is_spilling);
    _is_spilling = true;

    // TODO: spill thread may set_ready before the task::execute thread put the task to blocked state
    if (_source_state != SourceState::FINISHED) {
        Base::_dependency->Dependency::block();
    }
    auto& parent = Base::_parent->template cast<Parent>();
    Status status;
    Defer defer {[&]() {
        if (!status.ok()) {
            _is_spilling = false;
            if (_source_state != SourceState::FINISHED) {
                Base::_dependency->Dependency::set_ready();
            }
        }
    }};
    status = ExecEnv::GetInstance()->spill_stream_mgr()->get_async_task_thread_pool()->submit_func(
            [this, &parent, state] {
                Defer defer {[&]() {
                    if (!Base::_shared_state->_sink_status.ok()) {
                        LOG(WARNING)
                                << "agg node " << Base::_parent->id() << " operator "
                                << Base::_parent->operator_id()
                                << " revoke_memory error: " << Base::_shared_state->_sink_status;
                    } else {
                        LOG(INFO) << "agg node " << Base::_parent->id() << " operator "
                                  << Base::_parent->operator_id() << " revoke_memory finish";
                    }
                    {
                        std::unique_lock<std::mutex> lk(_spill_lock);
                        if (_source_state == SourceState::FINISHED) {
                            Base::_dependency->set_ready_to_read();
                            _finish_dependency->set_ready();
                        } else {
                            Base::_dependency->Dependency::set_ready();
                        }
                        _is_spilling = false;
                        _spill_cv.notify_one();
                    }
                }};
                auto* runtime_state = _runtime_state.get();
                auto* agg_data = parent._agg_sink_operator->get_agg_data(runtime_state);
                Base::_shared_state->_sink_status = std::visit(
                        [&](auto&& agg_method) -> Status {
                            auto& hash_table = *agg_method.hash_table;
                            return spill_hash_table2(state, agg_method, hash_table);
                        },
                        agg_data->method_variant);
                RETURN_IF_ERROR(Base::_shared_state->_sink_status);
                Base::_shared_state->_sink_status =
                        parent._agg_sink_operator->reset_hash_table(runtime_state);
                return Base::_shared_state->_sink_status;
            });
    return status;
}

template class PartitionedAggSinkOperatorX<PartitionedAggSinkLocalState>;
} // namespace doris::pipeline