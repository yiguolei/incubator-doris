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

#include <memory>

#include "common/status.h"
#include "operator.h"
#include "pipeline/pipeline_x/dependency.h"
#include "pipeline/pipeline_x/operator.h"

namespace doris {
class ExecNode;
class RuntimeState;

namespace pipeline {

class PartitionedAggSourceOperatorX;
class PartitionedAggLocalState;
class PartitionedAggSourceDependency final : public Dependency {
public:
    using SharedState = PartitionedAggSharedState;
    PartitionedAggSourceDependency(int id, int node_id, QueryContext* query_ctx)
            : Dependency(id, node_id, "PartitionedAggSourceDependency", query_ctx) {}
    ~PartitionedAggSourceDependency() override = default;

    friend class PartitionedAggSourceOperatorX;
    friend class PartitionedAggLocalState;

    void block() override {
        // if (_is_streaming_agg_state()) {
        Dependency::block();
        // }
    }
};

class PartitionedAggSourceOperatorX;
class PartitionedAggLocalState final : public PipelineXLocalState<PartitionedAggSourceDependency> {
public:
    ENABLE_FACTORY_CREATOR(PartitionedAggLocalState);
    using Base = PipelineXLocalState<PartitionedAggSourceDependency>;
    using Parent = PartitionedAggSourceOperatorX;
    PartitionedAggLocalState(RuntimeState* state, OperatorXBase* parent);
    ~PartitionedAggLocalState() override = default;

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status close(RuntimeState* state) override;

    Status initiate_merge_spill_partition_agg_data(RuntimeState* state);
    Status setup_in_memory_agg_op(RuntimeState* state);

protected:
    friend class PartitionedAggSourceOperatorX;
    std::unique_ptr<RuntimeState> _runtime_state;
    volatile bool _need_setup_in_memory_agg_op = true;
    AggSharedState* _in_memory_agg_op_shared_state = nullptr;

    Status _status;
    std::unique_ptr<std::promise<Status>> _spill_merge_promise;
    std::future<Status> _spill_merge_future;
    bool _current_partition_eos = true;
    bool _is_merging = false;
    std::mutex _merge_spill_lock;
    std::condition_variable _merge_spill_cv;
};
class AggSourceOperatorX;
class PartitionedAggSourceOperatorX : public OperatorX<PartitionedAggLocalState> {
public:
    using Base = OperatorX<PartitionedAggLocalState>;
    PartitionedAggSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                                  const DescriptorTbl& descs, bool is_streaming = false);
    ~PartitionedAggSourceOperatorX() override = default;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override;

    bool is_source() const override { return true; }

private:
    friend class PartitionedAggLocalState;
    Status _initiate_merge_spill_partition_agg_data(RuntimeState* state);

    std::unique_ptr<AggSourceOperatorX> _agg_source_operator;
};
} // namespace pipeline
} // namespace doris
