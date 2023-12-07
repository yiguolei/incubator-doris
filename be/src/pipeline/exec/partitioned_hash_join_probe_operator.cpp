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

namespace doris::pipeline {

PartitionedHashJoinProbeLocalState::PartitionedHashJoinProbeLocalState(RuntimeState* state,
                                                                       OperatorXBase* parent)
        : JoinProbeLocalState(state, parent) {}

Status PartitionedHashJoinProbeLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(JoinProbeLocalState::init(state, info));

    auto& p = _parent->cast<PartitionedHashJoinProbeOperatorX>();

    _partitioned_blocks.resize(p._partition_count);
    _partitioner = std::make_unique<PartitionerType>(p._partition_count);
    RETURN_IF_ERROR(_partitioner->init(p._probe_exprs));
    return _partitioner->prepare(state, p._child_x->row_desc());
}

Status PartitionedHashJoinProbeLocalState::open(RuntimeState* state) {
    RETURN_IF_ERROR(PipelineXLocalStateBase::open(state));
    return _partitioner->open(state);
}
Status PartitionedHashJoinProbeLocalState::close(RuntimeState* state) {
    RETURN_IF_ERROR(JoinProbeLocalState::close(state));
    return Status::OK();
}

void PartitionedHashJoinProbeLocalState::prepare_for_next() {}

void PartitionedHashJoinProbeLocalState::add_tuple_is_null_column(vectorized::Block* block) {}

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
    const std::vector<TEqJoinCondition>& eq_join_conjuncts = tnode.hash_join_node.eq_join_conjuncts;

    for (const auto& eq_join_conjunct : eq_join_conjuncts) {
        vectorized::VExprContextSPtr ctx;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(eq_join_conjunct.left, ctx));
        _probe_expr_ctxs.push_back(ctx);
        _probe_exprs.emplace_back(eq_join_conjunct.left);
    }

    if (tnode.hash_join_node.__isset.other_join_conjuncts &&
        !tnode.hash_join_node.other_join_conjuncts.empty()) {
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(
                tnode.hash_join_node.other_join_conjuncts, _other_join_conjuncts));

        DCHECK(!_build_unique);
        DCHECK(_have_other_join_conjunct);
    } else if (tnode.hash_join_node.__isset.vother_join_conjunct) {
        _other_join_conjuncts.resize(1);
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(
                tnode.hash_join_node.vother_join_conjunct, _other_join_conjuncts[0]));

        // If LEFT SEMI JOIN/LEFT ANTI JOIN with not equal predicate,
        // build table should not be deduplicated.
        DCHECK(!_build_unique);
        DCHECK(_have_other_join_conjunct);
    }

    if (tnode.hash_join_node.__isset.mark_join_conjuncts &&
        !tnode.hash_join_node.mark_join_conjuncts.empty()) {
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(
                tnode.hash_join_node.mark_join_conjuncts, _mark_join_conjuncts));
        DCHECK(_is_mark_join);

        /// We make mark join conjuncts as equal conjuncts for null aware join,
        /// so `_mark_join_conjuncts` should be empty if this is null aware join.
        DCHECK_EQ(_mark_join_conjuncts.empty(),
                  _join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
                          _join_op == TJoinOp::NULL_AWARE_LEFT_SEMI_JOIN);
    }

    auto tnode_ = _tnode;
    tnode_.runtime_filters.clear();

    _sink_operator = std::make_unique<HashJoinBuildSinkOperatorX>(_pool, _operator_id, tnode_,
                                                                  _descriptor_tbl, false);
    _probe_operator =
            std::make_unique<HashJoinProbeOperatorX>(_pool, tnode_, _operator_id, _descriptor_tbl);
    RETURN_IF_ERROR(_sink_operator->init(tnode_, state));
    return _probe_operator->init(tnode_, state);
}
Status PartitionedHashJoinProbeOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorXBase::prepare(state));
    RETURN_IF_ERROR(_probe_operator->set_child(_child_x));
    RETURN_IF_ERROR(_probe_operator->set_child(_build_side_child));
    RETURN_IF_ERROR(_sink_operator->set_child(_build_side_child));
    RETURN_IF_ERROR(_probe_operator->prepare(state));
    RETURN_IF_ERROR(_sink_operator->prepare(state));
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_output_expr_ctxs, state, *_intermediate_row_desc));
    return vectorized::VExpr::prepare(_probe_expr_ctxs, state, _child_x->row_desc());
}

Status PartitionedHashJoinProbeOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(JoinProbeOperatorX::open(state));
    RETURN_IF_ERROR(_probe_operator->open(state));
    RETURN_IF_ERROR(_sink_operator->open(state));
    return vectorized::VExpr::open(_probe_expr_ctxs, state);
}

Status PartitionedHashJoinProbeOperatorX::push(RuntimeState* state, vectorized::Block* input_block,
                                               SourceState source_state) const {
    auto& local_state = get_local_state(state);
    local_state._source_state = source_state;

    const auto rows = input_block->rows();
    auto& partitioned_blocks = local_state._partitioned_blocks;
    if (rows == 0) {
        if (source_state == SourceState::FINISHED) {
            for (uint32_t i = 0; i != _partition_count; ++i) {
                if (partitioned_blocks[i] && !partitioned_blocks[i]->empty()) {
                    local_state._probe_blocks[i].emplace_back(partitioned_blocks[i]->to_block());
                }
            }
            RETURN_IF_ERROR(_setup_internal_operators(local_state, state));
        }
        return Status::OK();
    }

    RETURN_IF_ERROR(local_state._partitioner->do_partitioning(state, input_block,
                                                              local_state._mem_tracker.get()));

    std::vector<uint32_t> partition_indexes[_partition_count];
    auto* channel_ids = reinterpret_cast<uint64_t*>(local_state._partitioner->get_channel_ids());
    for (uint32_t i = 0; i != rows; ++i) {
        partition_indexes[channel_ids[i]].emplace_back(i);
    }

    for (uint32_t i = 0; i != _partition_count; ++i) {
        const auto count = partition_indexes[i].size();
        if (UNLIKELY(count == 0)) {
            continue;
        }

        if (!partitioned_blocks[i]) {
            partitioned_blocks[i] =
                    vectorized::MutableBlock::create_unique(input_block->clone_empty());
        }
        partitioned_blocks[i]->add_rows(input_block, &(partition_indexes[i][0]),
                                        &(partition_indexes[i][count]));

        if (partitioned_blocks[i]->rows() > 2 * 1024 * 1024 ||
            (source_state == SourceState::FINISHED && partitioned_blocks[i]->rows() > 0)) {
            local_state._probe_blocks[i].emplace_back(partitioned_blocks[i]->to_block());
        }
    }

    if (source_state == SourceState::FINISHED) {
        RETURN_IF_ERROR(_setup_internal_operators(local_state, state));
    }

    return Status::OK();
}

Status PartitionedHashJoinProbeOperatorX::_setup_internal_operators(
        PartitionedHashJoinProbeLocalState& local_state, RuntimeState* state) const {
    local_state._shared_states.clear();
    local_state._upstream_deps.clear();
    local_state._downstream_deps.clear();

    local_state._runtime_state = RuntimeState::create_unique(
            nullptr, state->fragment_instance_id(), state->query_id(), state->fragment_id(),
            state->query_options(), TQueryGlobals {}, state->exec_env(), state->get_query_ctx());

    local_state._runtime_state->set_query_mem_tracker(state->query_mem_tracker());

    local_state._runtime_state->set_task_execution_context(
            state->get_task_execution_context().lock());
    local_state._runtime_state->set_be_number(state->be_number());

    local_state._runtime_state->set_desc_tbl(&state->desc_tbl());
    local_state._runtime_state->resize_op_id_to_local_state(-1);
    local_state._runtime_state->set_pipeline_x_runtime_filter_mgr(state->runtime_filter_mgr());

    _sink_operator->get_dependency(local_state._downstream_deps, local_state._shared_states,
                                   const_cast<QueryContext*>(local_state._dependency->_query_ctx));

    // set sink local state
    LocalSinkStateInfo info {
            0, local_state._runtime_profile.get(), -1, local_state._downstream_deps, {}, {}};
    RETURN_IF_ERROR(_sink_operator->setup_local_state(local_state._runtime_state.get(), info));

    auto dep = _probe_operator->get_dependency(
            const_cast<QueryContext*>(local_state._dependency->_query_ctx),
            local_state._shared_states);
    LocalStateInfo state_info {local_state._runtime_profile.get(),
                               {},
                               local_state._upstream_deps,
                               local_state._shared_states[1].get(),
                               {},
                               0,
                               dep};
    RETURN_IF_ERROR(
            _probe_operator->setup_local_state(local_state._runtime_state.get(), state_info));

    auto& partitioned_block =
            local_state._shared_state->partitioned_build_blocks[local_state._partition_cursor];
    vectorized::Block block;
    if (partitioned_block && partitioned_block->rows() > 0) {
        block = partitioned_block->to_block();
    }
    RETURN_IF_ERROR(
            _sink_operator->sink(local_state._runtime_state.get(), &block, SourceState::FINISHED));

    auto* sink_local_state =
            local_state._runtime_state->get_sink_local_state(_sink_operator->operator_id());
    DCHECK(sink_local_state != nullptr);
    RETURN_IF_ERROR(sink_local_state->open(state));

    auto* probe_local_state =
            local_state._runtime_state->get_local_state(_probe_operator->operator_id());
    DCHECK(probe_local_state != nullptr);
    RETURN_IF_ERROR(probe_local_state->open(state));

    return Status::OK();
}

Status PartitionedHashJoinProbeOperatorX::pull(doris::RuntimeState* state,
                                               vectorized::Block* output_block,
                                               SourceState& source_state) const {
    auto& local_state = get_local_state(state);
    auto partition_index = local_state._partition_cursor;
    SourceState source_state_;
    auto* runtime_state = local_state._runtime_state.get();
    auto& probe_blocks = local_state._probe_blocks[partition_index];
    while (_probe_operator->need_more_input_data(runtime_state)) {
        if (probe_blocks.empty()) {
            vectorized::Block block;
            RETURN_IF_ERROR(_probe_operator->push(runtime_state, &block, SourceState::FINISHED));
            break;
        }
        auto block = std::move(probe_blocks.back());
        probe_blocks.pop_back();
        RETURN_IF_ERROR(_probe_operator->push(
                runtime_state, &block,
                probe_blocks.empty() ? SourceState::FINISHED : SourceState::MORE_DATA));
    }

    RETURN_IF_ERROR(
            _probe_operator->pull(local_state._runtime_state.get(), output_block, source_state_));

    source_state = SourceState::MORE_DATA;
    if (source_state_ == SourceState::FINISHED) {
        local_state._partition_cursor++;
        if (local_state._partition_cursor == _partition_count) {
            source_state = SourceState::FINISHED;
        } else {
            RETURN_IF_ERROR(_setup_internal_operators(local_state, state));
        }
    }

    return Status::OK();
}

bool PartitionedHashJoinProbeOperatorX::need_more_input_data(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    return local_state._source_state != SourceState::FINISHED;
}

} // namespace doris::pipeline