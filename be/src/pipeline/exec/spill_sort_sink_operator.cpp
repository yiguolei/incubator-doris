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

#include "spill_sort_sink_operator.h"

#include "common/status.h"
#include "pipeline/exec/sort_sink_operator.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {
SpillSortSinkLocalState::SpillSortSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
        : PipelineXSinkLocalState<SpillSortSinkDependency>(parent, state) {
    _finish_dependency = std::make_shared<FinishDependency>(
            parent->operator_id(), parent->node_id(), parent->get_name() + "_FINISH_DEPENDENCY",
            state->get_query_ctx());
}
Status SpillSortSinkLocalState::init(doris::RuntimeState* state,
                                     doris::pipeline::LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(PipelineXSinkLocalState::init(state, info));
    _finish_dependency->block();
    return Status::OK();
}
Status SpillSortSinkLocalState::open(RuntimeState* state) {
    RETURN_IF_ERROR(PipelineXSinkLocalState::open(state));
    auto& parent = Base::_parent->template cast<Parent>();
    Base::_shared_state->_enable_spill = parent._enable_spill;
    return setup_in_memory_sort_op(state);
}
Status SpillSortSinkLocalState::close(RuntimeState* state, Status exec_sink_status) {
    {
        std::unique_lock<std::mutex> lk(_spill_lock);
        if (_is_spilling) {
            _spill_cv.wait(lk);
        }
    }
    return Status::OK();
}
Status SpillSortSinkLocalState::setup_in_memory_sort_op(RuntimeState* state) {
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
    parent._sort_sink_operator->get_dependency(Base::_shared_state->_downstream_deps,
                                               Base::_shared_state->_shared_states,
                                               const_cast<QueryContext*>(_dependency->_query_ctx));

    LocalSinkStateInfo info {0, Base::profile(), -1, Base::_shared_state->_downstream_deps, {}, {}};
    RETURN_IF_ERROR(parent._sort_sink_operator->setup_local_state(_runtime_state.get(), info));
    _in_memory_sort_shared_state =
            parent._sort_sink_operator->get_shared_state(_runtime_state.get());
    auto* sink_local_state =
            _runtime_state->get_sink_local_state(parent._sort_sink_operator->operator_id());
    DCHECK(sink_local_state != nullptr);
    return sink_local_state->open(state);
}

SpillSortSinkOperatorX::SpillSortSinkOperatorX(ObjectPool* pool, int operator_id,
                                               const TPlanNode& tnode, const DescriptorTbl& descs)
        : DataSinkOperatorX(operator_id, tnode.node_id) {
    _sort_sink_operator = std::make_unique<SortSinkOperatorX>(pool, operator_id, tnode, descs);
}

Status SpillSortSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX::init(tnode, state));

    _sort_sink_operator->set_dests_id(DataSinkOperatorX<LocalStateType>::dests_id());
    RETURN_IF_ERROR(_sort_sink_operator->set_child(DataSinkOperatorX<LocalStateType>::_child_x));
    return _sort_sink_operator->init(tnode, state);
}

Status SpillSortSinkOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<LocalStateType>::prepare(state));
    RETURN_IF_ERROR(_sort_sink_operator->prepare(state));
    _enable_spill = _sort_sink_operator->is_full_sort();
    return Status::OK();
}
Status SpillSortSinkOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<LocalStateType>::open(state));
    return _sort_sink_operator->open(state);
}
Status SpillSortSinkOperatorX::close(RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<LocalStateType>::close(state));
    return _sort_sink_operator->close(state);
}
Status SpillSortSinkOperatorX::revoke_memory(RuntimeState* state) {
    if (!_enable_spill) {
        return Status::OK();
    }
    auto& local_state = get_local_state(state);
    RETURN_IF_ERROR(local_state.revoke_memory(state));
    return Status::WaitForIO("Spilling");
}
size_t SpillSortSinkOperatorX::revocable_mem_size(RuntimeState* state) const {
    if (!_enable_spill) {
        return 0;
    }
    auto& local_state = get_local_state(state);
    if (!local_state.Base::_shared_state->_sink_status.ok()) {
        return UINT64_MAX;
    }
    auto size = _sort_sink_operator->get_revocable_mem_size(local_state._runtime_state.get());
    // LOG(INFO) << "sort node id: " << id() << ", operator id: " << operator_id()
    //           << ", revocable mem size: " << size;
    return size;
}
Status SpillSortSinkOperatorX::sink(doris::RuntimeState* state, vectorized::Block* in_block,
                                    SourceState source_state) {
    auto& local_state = get_local_state(state);
    RETURN_IF_ERROR(local_state.Base::_shared_state->_sink_status);
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
    if (in_block->rows() > 0) {
        local_state._shared_state->update_spill_block_batch_row_count(in_block);
    }
    local_state._source_state = source_state;
    RETURN_IF_ERROR(_sort_sink_operator->sink(local_state._runtime_state.get(), in_block,
                                              SourceState::DEPEND_ON_SOURCE));
    if (source_state == SourceState::FINISHED) {
        if (_enable_spill) {
            if (revocable_mem_size(state) > 0) {
                RETURN_IF_ERROR(revoke_memory(state));
            } else {
                local_state._dependency->set_ready_to_read();
            }
        } else {
            local_state._dependency->set_ready_to_read();
        }
    }
    return Status::OK();
}
Status SpillSortSinkLocalState::revoke_memory(RuntimeState* state) {
    LOG(INFO) << "sort node " << Base::_parent->id() << " operator " << Base::_parent->operator_id()
              << " revoke_memory"
              << ", source status: " << (int)_source_state;
    RETURN_IF_ERROR(Base::_shared_state->_sink_status);
    DCHECK(!_is_spilling);
    _is_spilling = true;

    // TODO: spill thread may set_ready before the task::execute thread put the task to blocked state
    if (_source_state != SourceState::FINISHED) {
        Base::_dependency->Dependency::block();
    }

    Status status;
    Defer defer {[&]() {
        if (!status.ok()) {
            _is_spilling = false;

            if (_source_state != SourceState::FINISHED) {
                Base::_dependency->Dependency::set_ready();
            }
        }
    }};
    status = ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
            _spilling_stream, print_id(state->query_id()), "sort", _parent->id(),
            _shared_state->_spill_block_batch_row_count,
            SpillSortSharedState::SORT_BLOCK_SPILL_BATCH_BYTES, profile());
    RETURN_IF_ERROR(status);

    status = _spilling_stream->prepare_spill();
    RETURN_IF_ERROR(status);
    _shared_state->_sorted_streams.emplace_back(_spilling_stream);

    auto& parent = Base::_parent->template cast<Parent>();
    status =
            ExecEnv::GetInstance()
                    ->spill_stream_mgr()
                    ->get_spill_io_thread_pool(_spilling_stream->get_spill_root_dir())
                    ->submit_func([this, state, &parent] {
                        Defer defer {[&]() {
                            if (!_shared_state->_sink_status.ok()) {
                                LOG(WARNING)
                                        << "sort node " << _parent->id() << " operator "
                                        << Base::_parent->operator_id()
                                        << " revoke memory error: " << _shared_state->_sink_status;
                            } else {
                                LOG(INFO)
                                        << "sort node " << _parent->id() << " operator "
                                        << Base::_parent->operator_id() << " revoke memory finish";
                            }

                            _spilling_stream->end_spill(_shared_state->_sink_status);
                            if (!_shared_state->_sink_status.ok()) {
                                _shared_state->clear();
                            }

                            if (_source_state == SourceState::FINISHED) {
                                _dependency->set_ready_to_read();
                                _finish_dependency->set_ready();
                            } else {
                                _dependency->Dependency::set_ready();
                            }
                            {
                                std::unique_lock<std::mutex> lk(_spill_lock);
                                _spilling_stream.reset();
                                _is_spilling = false;
                                _spill_cv.notify_one();
                            }
                        }};

                        _shared_state->_sink_status =
                                parent._sort_sink_operator->prepare_for_spill(_runtime_state.get());
                        RETURN_IF_ERROR(_shared_state->_sink_status);
                        bool eos = false;
                        vectorized::Block block;
                        while (!eos && !state->is_cancelled()) {
                            _shared_state->_sink_status =
                                    parent._sort_sink_operator->merge_sort_read_for_spill(
                                            _runtime_state.get(), &block,
                                            _shared_state->_spill_block_batch_row_count, &eos);
                            RETURN_IF_ERROR(_shared_state->_sink_status);
                            _shared_state->_sink_status = _spilling_stream->spill_block(block, eos);
                            RETURN_IF_ERROR(_shared_state->_sink_status);
                            block.clear_column_data();
                        }
                        parent._sort_sink_operator->reset(_runtime_state.get());

                        return Status::OK();
                    });
    return status;
}
} // namespace doris::pipeline