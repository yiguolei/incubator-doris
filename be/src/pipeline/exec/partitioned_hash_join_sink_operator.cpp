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

#include "partitioned_hash_join_sink_operator.h"

#include "pipeline/exec/operator.h"

namespace doris::pipeline {

Status PartitionedHashJoinSinkLocalState::init(doris::RuntimeState* state,
                                               doris::pipeline::LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(PipelineXSinkLocalState::init(state, info));
    auto& p = _parent->cast<PartitionedHashJoinSinkOperatorX>();
    _shared_state->partitioned_build_blocks.resize(p._partition_count);

    _partitioner = std::make_unique<PartitionerType>(p._partition_count);
    RETURN_IF_ERROR(_partitioner->init(p._build_exprs));
    return _partitioner->prepare(state, p._child_x->row_desc());
}

Status PartitionedHashJoinSinkLocalState::open(RuntimeState* state) {
    RETURN_IF_ERROR(PipelineXSinkLocalState::open(state));
    return _partitioner->open(state);
}

PartitionedHashJoinSinkOperatorX::PartitionedHashJoinSinkOperatorX(
        ObjectPool* pool, int operator_id, const TPlanNode& tnode, const DescriptorTbl& descs,
        bool use_global_rf, uint32_t partition_count)
        : JoinBuildSinkOperatorX<PartitionedHashJoinSinkLocalState>(pool, operator_id, tnode,
                                                                    descs),
          _join_distribution(tnode.hash_join_node.__isset.dist_type ? tnode.hash_join_node.dist_type
                                                                    : TJoinDistributionType::NONE),
          _distribution_partition_exprs(tnode.__isset.distribute_expr_lists && !_is_broadcast_join
                                                ? tnode.distribute_expr_lists[1]
                                                : std::vector<TExpr> {}),
          _tnode(tnode),
          _descriptor_tbl(descs),
          _partition_count(partition_count) {}

Status PartitionedHashJoinSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    const std::vector<TEqJoinCondition>& eq_join_conjuncts = tnode.hash_join_node.eq_join_conjuncts;
    std::vector<TExpr> partition_exprs;
    for (const auto& eq_join_conjunct : eq_join_conjuncts) {
        vectorized::VExprContextSPtr ctx;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(eq_join_conjunct.right, ctx));
        _build_exprs.emplace_back(eq_join_conjunct.right);
        _build_expr_ctxs.emplace_back(ctx);
        partition_exprs.emplace_back(eq_join_conjunct.right);
    }

    return Status::OK();
}

Status PartitionedHashJoinSinkOperatorX::prepare(RuntimeState* state) {
    return vectorized::VExpr::prepare(_build_expr_ctxs, state, _child_x->row_desc());
}

Status PartitionedHashJoinSinkOperatorX::open(RuntimeState* state) {
    return vectorized::VExpr::open(_build_expr_ctxs, state);
}

Status PartitionedHashJoinSinkOperatorX::_do_evaluate(vectorized::Block& block,
                                                      vectorized::VExprContextSPtrs& exprs,
                                                      RuntimeProfile::Counter& expr_call_timer,
                                                      std::vector<int>& res_col_ids) {
    for (size_t i = 0; i < exprs.size(); ++i) {
        int result_col_id = -1;
        // execute build column
        {
            SCOPED_TIMER(&expr_call_timer);
            RETURN_IF_ERROR(exprs[i]->execute(&block, &result_col_id));
        }

        // TODO: opt the column is const
        block.get_by_position(result_col_id).column =
                block.get_by_position(result_col_id).column->convert_to_full_column_if_const();
        res_col_ids[i] = result_col_id;
    }
    return Status::OK();
}

Status PartitionedHashJoinSinkOperatorX::sink(RuntimeState* state, vectorized::Block* in_block,
                                              SourceState source_state) {
    const auto rows = in_block->rows();
    auto& local_state = get_local_state(state);
    if (rows > 0) {
        /// TODO: DO NOT execute build exprs twice(when partition and building hash table)
        RETURN_IF_ERROR(local_state._partitioner->do_partitioning(state, in_block,
                                                                  local_state._mem_tracker.get()));

        auto* channel_ids =
                reinterpret_cast<uint64_t*>(local_state._partitioner->get_channel_ids());
        std::vector<uint32_t> partition_indexes[_partition_count];
        for (uint32_t i = 0; i != rows; ++i) {
            partition_indexes[channel_ids[i]].emplace_back(i);
        }

        auto& partitioned_blocks = local_state._shared_state->partitioned_build_blocks;
        for (uint32_t i = 0; i != _partition_count; ++i) {
            const auto count = partition_indexes[i].size();
            if (UNLIKELY(count == 0)) {
                continue;
            }

            if (UNLIKELY(!partitioned_blocks[i])) {
                partitioned_blocks[i] =
                        vectorized::MutableBlock::create_unique(in_block->clone_empty());
            }
            partitioned_blocks[i]->add_rows(in_block, &(partition_indexes[i][0]),
                                            &(partition_indexes[i][count]));
        }
    }

    if (source_state == SourceState::FINISHED) {
        local_state._dependency->set_ready_to_read();
    }
    return Status::OK();
}

size_t PartitionedHashJoinSinkOperatorX::revocable_mem_size(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    auto& partitioned_blocks = local_state._shared_state->partitioned_build_blocks;

    size_t mem_size = 0;
    for (uint32_t i = 0; i != _partition_count; ++i) {
        auto& block = partitioned_blocks[i];
        if (block) {
            mem_size += block->allocated_bytes();
        }
    }
    return mem_size;
}
} // namespace doris::pipeline