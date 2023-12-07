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

#include "sort_sink_operator.h"

#include <string>

#include "pipeline/exec/operator.h"
#include "runtime/query_context.h"
#include "vec/common/sort/heap_sorter.h"
#include "vec/common/sort/topn_sorter.h"
#include "vec/spill/spill_stream.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {

OPERATOR_CODE_GENERATOR(SortSinkOperator, StreamingOperator)

Status SortSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(PipelineXSinkLocalState<SortSinkDependency>::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    auto& p = _parent->cast<SortSinkOperatorX>();

    _shared_state->enable_spill_ =
            p._enable_spill && (p._algorithm == SortAlgorithm::FULL_SORT) && p._limit <= 0;

    RETURN_IF_ERROR(p._vsort_exec_exprs.clone(state, _vsort_exec_exprs));
    switch (p._algorithm) {
    case SortAlgorithm::HEAP_SORT: {
        _shared_state->sorter = vectorized::HeapSorter::create_unique(
                _vsort_exec_exprs, p._limit, p._offset, p._pool, p._is_asc_order, p._nulls_first,
                p._child_x->row_desc());
        break;
    }
    case SortAlgorithm::TOPN_SORT: {
        _shared_state->sorter = vectorized::TopNSorter::create_unique(
                _vsort_exec_exprs, p._limit, p._offset, p._pool, p._is_asc_order, p._nulls_first,
                p._child_x->row_desc(), state, _profile);
        break;
    }
    case SortAlgorithm::FULL_SORT: {
        _shared_state->sorter = vectorized::FullSorter::create_unique(
                _vsort_exec_exprs, p._limit, p._offset, p._pool, p._is_asc_order, p._nulls_first,
                p._child_x->row_desc(), state, _profile);
        break;
    }
    default: {
        return Status::InvalidArgument("Invalid sort algorithm!");
    }
    }

    _shared_state->sorter->init_profile(_profile);

    _profile->add_info_string("TOP-N", p._limit == -1 ? "false" : "true");

    _sort_blocks_memory_usage =
            ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "SortBlocks", TUnit::BYTES, "MemoryUsage", 1);

    if (_shared_state->enable_spill_) {
        finish_dependency_->block();
    }
    return Status::OK();
}

Status SortSinkLocalState::close(RuntimeState* state, Status exec_status) {
    SCOPED_TIMER(Base::exec_time_counter());
    SCOPED_TIMER(Base::_close_timer);
    if (Base::_closed) {
        return Status::OK();
    }
    {
        std::unique_lock<std::mutex> lk(spill_lock_);
        if (spilling_stream_) {
            spill_cv_.wait(lk);
        }
    }
    return Base::close(state, exec_status);
}

Status SortSinkLocalState::revoke_memory(RuntimeState* state) {
    LOG(INFO) << _shared_state << " sort node id: " << _parent->id()
              << ", operator id: " << _parent->operator_id() << " revoke memory";
    DCHECK(!spilling_stream_);

    RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
            spilling_stream_, print_id(state->query_id()), "sort", _parent->id(),
            _shared_state->spill_block_batch_size_, SortSharedState::SORT_BLOCK_SPILL_BATCH_BYTES,
            profile()));

    RETURN_IF_ERROR(spilling_stream_->prepare_spill());
    _shared_state->sorted_streams_.emplace_back(spilling_stream_);

    if (_source_state != SourceState::FINISHED) {
        _dependency->Dependency::block();
    }

    return ExecEnv::GetInstance()
            ->spill_stream_mgr()
            ->get_spill_io_thread_pool(spilling_stream_->get_spill_root_dir())
            ->submit_func([this, state] {
                Defer defer {[&]() {
                    LOG(WARNING) << _shared_state << " sort node id: " << _parent->id()
                                 << ", operator id: " << Base::_parent->operator_id()
                                 << " revoke memory finish: " << _shared_state->sink_status_
                                 << " source state: " << (int)_source_state;

                    spilling_stream_->end_spill(_shared_state->sink_status_);
                    if (!_shared_state->sink_status_.ok()) {
                        _shared_state->clear();
                    }

                    if (_source_state == SourceState::FINISHED) {
                        _dependency->set_ready_to_read();
                        finish_dependency_->set_ready();
                    } else {
                        _dependency->Dependency::set_ready();
                    }
                    {
                        std::unique_lock<std::mutex> lk(spill_lock_);
                        spilling_stream_.reset();
                        spill_cv_.notify_one();
                    }
                }};

                _shared_state->sink_status_ = _shared_state->sorter->prepare_for_read();
                RETURN_IF_ERROR(_shared_state->sink_status_);
                bool eos = false;
                vectorized::Block block;
                while (!eos && !state->is_cancelled()) {
                    _shared_state->sink_status_ = _shared_state->sorter->merge_sort_read_for_spill(
                            state, &block, _shared_state->spill_block_batch_size_, &eos);
                    RETURN_IF_ERROR(_shared_state->sink_status_);
                    _shared_state->sink_status_ = spilling_stream_->spill_block(block, eos);
                    RETURN_IF_ERROR(_shared_state->sink_status_);
                    block.clear_column_data();
                }
                _shared_state->sorter->reset();

                return Status::OK();
            });
}
SortSinkOperatorX::SortSinkOperatorX(ObjectPool* pool, int operator_id, const TPlanNode& tnode,
                                     const DescriptorTbl& descs)
        : DataSinkOperatorX(operator_id, tnode.node_id),
          _offset(tnode.sort_node.__isset.offset ? tnode.sort_node.offset : 0),
          _pool(pool),
          _reuse_mem(true),
          _limit(tnode.limit),
          _use_topn_opt(tnode.sort_node.use_topn_opt),
          _row_descriptor(descs, tnode.row_tuples, tnode.nullable_tuples),
          _use_two_phase_read(tnode.sort_node.sort_info.use_two_phase_read),
          _merge_by_exchange(tnode.sort_node.merge_by_exchange),
          _is_colocate(tnode.sort_node.__isset.is_colocate ? tnode.sort_node.is_colocate : false),
          _is_analytic_sort(tnode.sort_node.__isset.is_analytic_sort
                                    ? tnode.sort_node.is_analytic_sort
                                    : false),
          _partition_exprs(tnode.__isset.distribute_expr_lists ? tnode.distribute_expr_lists[0]
                                                               : std::vector<TExpr> {}) {}

Status SortSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX::init(tnode, state));
    RETURN_IF_ERROR(_vsort_exec_exprs.init(tnode.sort_node.sort_info, _pool));
    _is_asc_order = tnode.sort_node.sort_info.is_asc_order;
    _nulls_first = tnode.sort_node.sort_info.nulls_first;
    _enable_spill = state->enable_sort_spill();

    // init runtime predicate
    if (_use_topn_opt) {
        auto query_ctx = state->get_query_ctx();
        auto first_sort_expr_node = tnode.sort_node.sort_info.ordering_exprs[0].nodes[0];
        if (first_sort_expr_node.node_type == TExprNodeType::SLOT_REF) {
            auto first_sort_slot = first_sort_expr_node.slot_ref;
            for (auto tuple_desc : _row_descriptor.tuple_descriptors()) {
                if (tuple_desc->id() != first_sort_slot.tuple_id) {
                    continue;
                }
                for (auto slot : tuple_desc->slots()) {
                    if (slot->id() == first_sort_slot.slot_id) {
                        RETURN_IF_ERROR(query_ctx->get_runtime_predicate().init(slot->type().type,
                                                                                _nulls_first[0]));
                        break;
                    }
                }
            }
        }
        if (!query_ctx->get_runtime_predicate().inited()) {
            return Status::InternalError("runtime predicate is not properly initialized");
        }
    }
    return Status::OK();
}

Status SortSinkOperatorX::prepare(RuntimeState* state) {
    const auto& row_desc = _child_x->row_desc();

    // If `limit` is smaller than HEAP_SORT_THRESHOLD, we consider using heap sort in priority.
    // To do heap sorting, each income block will be filtered by heap-top row. There will be some
    // `memcpy` operations. To ensure heap sort will not incur performance fallback, we should
    // exclude cases which incoming blocks has string column which is sensitive to operations like
    // `filter` and `memcpy`
    if (_limit > 0 && _limit + _offset < vectorized::HeapSorter::HEAP_SORT_THRESHOLD &&
        (_use_two_phase_read || _use_topn_opt || !row_desc.has_varlen_slots())) {
        _algorithm = SortAlgorithm::HEAP_SORT;
        _reuse_mem = false;
    } else if (_limit > 0 && row_desc.has_varlen_slots() &&
               _limit + _offset < vectorized::TopNSorter::TOPN_SORT_THRESHOLD) {
        _algorithm = SortAlgorithm::TOPN_SORT;
    } else {
        _algorithm = SortAlgorithm::FULL_SORT;
    }
    _enable_spill = _enable_spill && (_algorithm == SortAlgorithm::FULL_SORT) && _limit <= 0;
    return _vsort_exec_exprs.prepare(state, _child_x->row_desc(), _row_descriptor);
}

Status SortSinkOperatorX::open(RuntimeState* state) {
    return _vsort_exec_exprs.open(state);
}

Status SortSinkOperatorX::sink(doris::RuntimeState* state, vectorized::Block* in_block,
                               SourceState source_state) {
    auto& local_state = get_local_state(state);
    RETURN_IF_ERROR(local_state._shared_state->sink_status_);
    local_state._source_state = source_state;
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());
    if (in_block->rows() > 0) {
        local_state._shared_state->update_spill_block_batch_size(in_block);
        RETURN_IF_ERROR(local_state._shared_state->sorter->append_block(in_block));
        local_state._mem_tracker->set_consumption(local_state._shared_state->sorter->data_size());
        COUNTER_SET(local_state._sort_blocks_memory_usage,
                    (int64_t)local_state._shared_state->sorter->data_size());
        RETURN_IF_CANCELLED(state);

        // update runtime predicate
        if (_use_topn_opt) {
            vectorized::Field new_top = local_state._shared_state->sorter->get_top_value();
            if (!new_top.is_null() && new_top != local_state.old_top) {
                const auto& sort_description =
                        local_state._shared_state->sorter->get_sort_description();
                auto col = in_block->get_by_position(sort_description[0].column_number);
                bool is_reverse = sort_description[0].direction < 0;
                auto* query_ctx = state->get_query_ctx();
                RETURN_IF_ERROR(
                        query_ctx->get_runtime_predicate().update(new_top, col.name, is_reverse));
                local_state.old_top = std::move(new_top);
            }
        }
        if (!_reuse_mem) {
            in_block->clear();
        }
    }

    if (source_state == SourceState::FINISHED) {
        if (revocable_mem_size(state) > 0) {
            RETURN_IF_ERROR(revoke_memory(state));
        } else {
            if (!_enable_spill) {
                RETURN_IF_ERROR(local_state._shared_state->sorter->prepare_for_read());
            }
            local_state._dependency->set_ready_to_read();
        }
    }
    return Status::OK();
}

size_t SortSinkOperatorX::revocable_mem_size(RuntimeState* state) const {
    if (_enable_spill) {
        auto& local_state = get_local_state(state);
        return local_state._shared_state->sorter->data_size();
    }
    return 0;
}

Status SortSinkOperatorX::revoke_memory(RuntimeState* state) {
    DCHECK(_enable_spill);
    auto& local_state = get_local_state(state);
    RETURN_IF_ERROR(local_state._shared_state->sink_status_);
    RETURN_IF_ERROR(local_state.revoke_memory(state));
    return Status::WaitForIO("Spilling");
}
} // namespace doris::pipeline
