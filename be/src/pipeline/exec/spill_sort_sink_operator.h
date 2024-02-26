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

#include "operator.h"
#include "pipeline/pipeline_x/operator.h"
#include "sort_sink_operator.h"

namespace doris::pipeline {
class SpillSortSinkLocalState;
class SpillSortSinkOperatorX;
class SpillSortSinkDependency final : public Dependency {
public:
    using SharedState = SpillSortSharedState;
    SpillSortSinkDependency(int id, int node_id, QueryContext* query_ctx)
            : Dependency(id, node_id, "SpillSortSinkDependency", true, query_ctx) {}
    ~SpillSortSinkDependency() override = default;

    friend class SpillSortSinkLocalState;
    friend class SpillSortSinkOperatorX;
};

class SpillSortSinkLocalState : public PipelineXSinkLocalState<SpillSortSinkDependency> {
    ENABLE_FACTORY_CREATOR(SpillSortSinkLocalState);

public:
    using Base = PipelineXSinkLocalState<SpillSortSinkDependency>;
    using Parent = SpillSortSinkOperatorX;
    SpillSortSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state);
    ~SpillSortSinkLocalState() override = default;

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;
    Dependency* finishdependency() override { return _finish_dependency.get(); }

    Status setup_in_memory_sort_op(RuntimeState* state);
    Status revoke_memory(RuntimeState* state);

private:
    friend class SpillSortSinkOperatorX;

    SortSharedState* _in_memory_sort_shared_state = nullptr;
    std::unique_ptr<RuntimeState> _runtime_state;

    SourceState _source_state = SourceState::DEPEND_ON_SOURCE;
    bool _is_spilling = false;
    vectorized::SpillStreamSPtr _spilling_stream;
    std::shared_ptr<Dependency> _finish_dependency;
    std::mutex _spill_lock;
    std::condition_variable _spill_cv;
};

class SpillSortSinkOperatorX final : public DataSinkOperatorX<SpillSortSinkLocalState> {
public:
    using LocalStateType = SpillSortSinkLocalState;
    SpillSortSinkOperatorX(ObjectPool* pool, int operator_id, const TPlanNode& tnode,
                           const DescriptorTbl& descs);
    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TPlanNode",
                                     DataSinkOperatorX<SpillSortSinkLocalState>::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;
    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override;
    /*
    DataDistribution required_data_distribution() const override {
        if (_is_analytic_sort) {
            return _is_colocate
                           ? DataDistribution(ExchangeType::BUCKET_HASH_SHUFFLE, _partition_exprs)
                           : DataDistribution(ExchangeType::HASH_SHUFFLE, _partition_exprs);
        } else if (_merge_by_exchange) {
            // The current sort node is used for the ORDER BY
            return {ExchangeType::PASSTHROUGH};
        }
        return DataSinkOperatorX<SpillSortSinkLocalState>::required_data_distribution();
    }
    */
    Status set_child(OperatorXPtr child) override {
        RETURN_IF_ERROR(DataSinkOperatorX<SpillSortSinkLocalState>::set_child(child));
        return _sort_sink_operator->set_child(child);
    }

    size_t revocable_mem_size(RuntimeState* state) const override;

    Status revoke_memory(RuntimeState* state) override;

    using DataSinkOperatorX<LocalStateType>::id;
    using DataSinkOperatorX<LocalStateType>::operator_id;
    using DataSinkOperatorX<LocalStateType>::get_local_state;

private:
    friend class SpillSortSinkLocalState;
    std::unique_ptr<SortSinkOperatorX> _sort_sink_operator;
    bool _enable_spill = false;
};
} // namespace doris::pipeline