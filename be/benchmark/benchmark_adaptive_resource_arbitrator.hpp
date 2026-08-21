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

#include <benchmark/benchmark.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "common/status.h"
#include "runtime/adaptive_resource_arbitrator.h"

namespace doris {
namespace {

// 用固定的 128 个直接 child 模拟一个 root 下有 128 个 scanner 的场景。
//
// 每次迭代轮转选择一个 child。
// 因此不会只测到某个固定 child 的缓存状态。
constexpr size_t kAdaptiveResourceArbitratorChildNum = 128;

struct AdaptiveResourceArbitratorBenchmarkTree {
    std::shared_ptr<AdaptiveResourceArbitrator> root;
    std::array<std::shared_ptr<AdaptiveResourceArbitrator>, kAdaptiveResourceArbitratorChildNum>
            children;
};

AdaptiveResourceArbitratorBenchmarkTree create_adaptive_resource_arbitrator_benchmark_tree() {
    AdaptiveResourceArbitratorBenchmarkTree tree;

    // root 的 limit 设为 128。
    // 计时区间内始终只有当前 token 未释放，因此普通申请一定可以成功。
    tree.root = std::make_shared<AdaptiveResourceArbitrator>(
            static_cast<int64_t>(kAdaptiveResourceArbitratorChildNum));
    for (auto& child : tree.children) {
        child = std::make_shared<AdaptiveResourceArbitrator>(int32_t {1});
        tree.root->add_child(child);
    }
    return tree;
}

void BM_AdaptiveResourceArbitratorTryAcquireAndRelease128Children(benchmark::State& state) {
    // 建树会触发多次重平衡。
    // 建树发生在 benchmark 循环外，不会计入每次迭代的耗时。
    auto tree = create_adaptive_resource_arbitrator_benchmark_tree();

    size_t child_index = 0;
    for (auto _ : state) {
        auto token = tree.children[child_index]->try_acquire(1);
        benchmark::DoNotOptimize(token);
        child_index = (child_index + 1) % tree.children.size();

        // Google Benchmark 的计时器在整个循环内保持运行。
        // 本用例统计 try_acquire、token 创建、release 和 release 后重平衡的总成本。
        // token 释放后，下一次迭代仍从无在用资源的稳定状态开始。
        DORIS_CHECK(token != nullptr);
        token.reset();
    }
}

void BM_AdaptiveResourceArbitratorAcquireAndRelease128Children(benchmark::State& state) {
    // 与 try_acquire 基准使用完全相同的树。
    // 两者的差值主要反映申请阶段 limit、target 和 available quota 检查的成本。
    auto tree = create_adaptive_resource_arbitrator_benchmark_tree();

    size_t child_index = 0;
    for (auto _ : state) {
        auto token = tree.children[child_index]->acquire(1);
        benchmark::DoNotOptimize(token);
        child_index = (child_index + 1) % tree.children.size();

        // 强制申请和 token release 都处于计时区间。
        // 这样不需要在循环内反复暂停计时器。
        DORIS_CHECK(token != nullptr);
        token.reset();
    }
}

BENCHMARK(BM_AdaptiveResourceArbitratorTryAcquireAndRelease128Children)
        ->Unit(benchmark::kNanosecond)
        ->Threads(1)
        ->Repetitions(5)
        ->DisplayAggregatesOnly();

BENCHMARK(BM_AdaptiveResourceArbitratorAcquireAndRelease128Children)
        ->Unit(benchmark::kNanosecond)
        ->Threads(1)
        ->Repetitions(5)
        ->DisplayAggregatesOnly();

} // namespace
} // namespace doris
