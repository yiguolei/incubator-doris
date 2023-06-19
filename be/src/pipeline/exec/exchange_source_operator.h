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

#include "operator.h"
#include "vec/exec/vexchange_node.h"

namespace doris {
class ExecNode;
} // namespace doris

namespace doris::pipeline {

class ExchangeSourceOperatorBuilder final : public OperatorBuilder<vectorized::VExchangeNode> {
public:
    ExchangeSourceOperatorBuilder(int32_t id, ExecNode* exec_node);

    bool is_source() const override { return true; }

    OperatorPtr build_operator() override;
};

class ExchangeSourceOperator final : public SourceOperator<ExchangeSourceOperatorBuilder> {
public:
    ExchangeSourceOperator(OperatorBuilderBase*, ExecNode*);
    bool can_read() override;
    bool is_pending_finish() const override;

    // Should add wait source time to exchange node
    void update_profile(PipelineTaskTimer& pipeline_task_timer) override {
        StreamingOperator<ExchangeSourceOperatorBuilder>::_node->runtime_profile()
                ->total_time_counter()
                ->update(pipeline_task_timer.wait_source_time +
                         pipeline_task_timer.wait_dependency_time);

        StreamingOperator<ExchangeSourceOperatorBuilder>::_node->update_wait_source_time(
                pipeline_task_timer.wait_source_time);
    }
};

} // namespace doris::pipeline