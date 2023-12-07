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

#include <stdint.h>

#include "common/status.h"
#include "operator.h"
#include "pipeline/exec/hashjoin_build_sink.h"
#include "pipeline/exec/hashjoin_probe_operator.h"
#include "pipeline/exec/join_build_sink_operator.h"
#include "pipeline/pipeline_x/local_exchange/local_exchange_sink_operator.h" // LocalExchangeChannelIds
#include "pipeline/pipeline_x/operator.h"
#include "vec/runtime/partitioner.h"

namespace doris {
class ExecNode;
class RuntimeState;

namespace pipeline {

using PartitionerType = vectorized::XXHashPartitioner<LocalExchangeChannelIds>;

class PartitionedHashJoinSinkOperatorX;

class SharedPartitionedHashJoinDependency final : public Dependency {
public:
    using SharedState = PartitionedHashJoinSharedState;
    ENABLE_FACTORY_CREATOR(SharedPartitionedHashJoinDependency);
    SharedPartitionedHashJoinDependency(int id, int node_id, QueryContext* query_ctx)
            : Dependency(id, node_id, "SharedPartitionedHashJoinDependency", true, query_ctx) {}
    ~SharedPartitionedHashJoinDependency() override = default;
};

class PartitionedHashJoinSinkLocalState
        : public PipelineXSinkLocalState<SharedPartitionedHashJoinDependency> {
public:
    using Parent = PartitionedHashJoinSinkOperatorX;
    ENABLE_FACTORY_CREATOR(PartitionedHashJoinSinkLocalState);
    ~PartitionedHashJoinSinkLocalState() override = default;
    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;

protected:
    PartitionedHashJoinSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : PipelineXSinkLocalState<DependencyType>(parent, state) {}

    friend class PartitionedHashJoinSinkOperatorX;

    std::vector<std::unique_ptr<vectorized::MutableBlock>> _partitioned_blocks;

    std::unique_ptr<HashJoinBuildSinkOperatorX> _sink_operator;

    ObjectPool* _obj_pool;

    uint32_t _partition_cursor {0};
    std::unique_ptr<PartitionerType> _partitioner;

    RuntimeProfile::Counter* _build_rows_counter = nullptr;
    RuntimeProfile::Counter* _publish_runtime_filter_timer = nullptr;
    RuntimeProfile::Counter* _runtime_filter_compute_timer = nullptr;
    RuntimeProfile::Counter* _build_table_timer = nullptr;
    RuntimeProfile::Counter* _build_expr_call_timer = nullptr;
    RuntimeProfile::Counter* _build_table_insert_timer = nullptr;
    RuntimeProfile::Counter* _build_side_compute_hash_timer = nullptr;
    RuntimeProfile::Counter* _build_side_merge_block_timer = nullptr;

    RuntimeProfile::Counter* _allocate_resource_timer = nullptr;

    RuntimeProfile::Counter* _build_blocks_memory_usage = nullptr;
    RuntimeProfile::Counter* _hash_table_memory_usage = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _build_arena_memory_usage = nullptr;
};

class PartitionedHashJoinSinkOperatorX
        : public JoinBuildSinkOperatorX<PartitionedHashJoinSinkLocalState> {
public:
    PartitionedHashJoinSinkOperatorX(ObjectPool* pool, int operator_id, const TPlanNode& tnode,
                                     const DescriptorTbl& descs, bool use_global_rf,
                                     uint32_t partition_count);

    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TDataSink",
                                     PartitionedHashJoinSinkOperatorX::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override;

    bool should_dry_run(RuntimeState* state) override { return false; }

    size_t revocable_mem_size(RuntimeState* state) const override;

    DataDistribution required_data_distribution() const override {
        if (_join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
            return {ExchangeType::NOOP};
        } else if (_is_broadcast_join) {
            return _child_x->ignore_data_distribution()
                           ? DataDistribution(ExchangeType::PASS_TO_ONE)
                           : DataDistribution(ExchangeType::NOOP);
        }
        return _join_distribution == TJoinDistributionType::BUCKET_SHUFFLE ||
                               _join_distribution == TJoinDistributionType::COLOCATE
                       ? DataDistribution(ExchangeType::BUCKET_HASH_SHUFFLE,
                                          _distribution_partition_exprs)
                       : DataDistribution(ExchangeType::HASH_SHUFFLE,
                                          _distribution_partition_exprs);
    }

    bool is_shuffled_hash_join() const override {
        return _join_distribution == TJoinDistributionType::PARTITIONED;
    }

private:
    friend class PartitionedHashJoinSinkLocalState;

    static Status _do_evaluate(vectorized::Block& block, vectorized::VExprContextSPtrs& exprs,
                               RuntimeProfile::Counter& expr_call_timer,
                               std::vector<int>& res_col_ids);

    const TJoinDistributionType::type _join_distribution;

    std::vector<TExpr> _build_exprs;
    // build expr
    vectorized::VExprContextSPtrs _build_expr_ctxs;
    // mark the build hash table whether it needs to store null value
    std::vector<bool> _store_null_in_hash_table;

    // mark the join column whether support null eq
    std::vector<bool> _is_null_safe_eq_join;

    bool _is_broadcast_join = false;

    vectorized::SharedHashTableContextPtr _shared_hash_table_context = nullptr;
    const std::vector<TExpr> _distribution_partition_exprs;
    const TPlanNode _tnode;
    const DescriptorTbl _descriptor_tbl;
    const uint32_t _partition_count;
};

} // namespace pipeline
} // namespace doris