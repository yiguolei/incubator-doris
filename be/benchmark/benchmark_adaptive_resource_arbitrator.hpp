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

void BM_AdaptiveResourceArbitratorTryAcquire128Children(benchmark::State& state) {
    // 建树会触发多次重平衡。
    // 这里暂停计时，只测稳定运行时一次 try_acquire(1) 的成本。
    state.PauseTiming();
    auto tree = create_adaptive_resource_arbitrator_benchmark_tree();
    state.ResumeTiming();

    size_t child_index = 0;
    for (auto _ : state) {
        auto token = tree.children[child_index]->try_acquire(1);
        benchmark::DoNotOptimize(token);
        child_index = (child_index + 1) % tree.children.size();

        // 本 benchmark 只统计申请。
        // token 析构会触发 release 和下一轮重平衡，因此在暂停计时后释放。
        // 这样下一次迭代仍从无在用资源的稳定状态开始。
        state.PauseTiming();
        DORIS_CHECK(token != nullptr);
        token.reset();
        state.ResumeTiming();
    }
    state.PauseTiming();
}

void BM_AdaptiveResourceArbitratorAcquire128Children(benchmark::State& state) {
    // 与 try_acquire 基准使用完全相同的树。
    // 两者的差值主要反映 limit、target 和 available quota 检查的成本。
    state.PauseTiming();
    auto tree = create_adaptive_resource_arbitrator_benchmark_tree();
    state.ResumeTiming();

    size_t child_index = 0;
    for (auto _ : state) {
        auto token = tree.children[child_index]->acquire(1);
        benchmark::DoNotOptimize(token);
        child_index = (child_index + 1) % tree.children.size();

        // 强制申请同样需要释放 token 以恢复下一次迭代的初始状态。
        // release 不属于本用例要统计的 acquire 成本。
        state.PauseTiming();
        DORIS_CHECK(token != nullptr);
        token.reset();
        state.ResumeTiming();
    }
    state.PauseTiming();
}

BENCHMARK(BM_AdaptiveResourceArbitratorTryAcquire128Children)
        ->Unit(benchmark::kNanosecond)
        ->Threads(1)
        ->Repetitions(5)
        ->DisplayAggregatesOnly();

BENCHMARK(BM_AdaptiveResourceArbitratorAcquire128Children)
        ->Unit(benchmark::kNanosecond)
        ->Threads(1)
        ->Repetitions(5)
        ->DisplayAggregatesOnly();

} // namespace
} // namespace doris
