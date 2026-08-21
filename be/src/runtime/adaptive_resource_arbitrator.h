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

#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

namespace doris {

class AdaptiveResourceArbitrator;

// 一个 token 表示叶子节点已经拿到并正在使用的一段资源。
//
// token 不支持拆分、转移或抢占。
//
// 使用方结束后销毁 token。
// 析构函数会沿仍存活的父链扣减 usage。
//
// 因此仲裁器只能控制后续申请。
// 已发放的 token 不能被仲裁器收回。
class AdaptiveResourceToken {
public:
    AdaptiveResourceToken(std::shared_ptr<AdaptiveResourceArbitrator> arbitrator, int64_t quota_num)
            : _arbitrator(std::move(arbitrator)), _quota_num(quota_num) {}

    AdaptiveResourceToken(const AdaptiveResourceToken&) = delete;
    AdaptiveResourceToken& operator=(const AdaptiveResourceToken&) = delete;
    AdaptiveResourceToken(AdaptiveResourceToken&&) = delete;
    AdaptiveResourceToken& operator=(AdaptiveResourceToken&&) = delete;

    ~AdaptiveResourceToken();

    int64_t quota_num() const { return _quota_num; }

private:
    std::shared_ptr<AdaptiveResourceArbitrator> _arbitrator;
    int64_t _quota_num = 0;
};

// 一个非抢占式的层级资源仲裁器。
//
// 资源不紧张时：
//   所有 child 按需使用资源。
//   此时不按 weight 切分固定配额。
//   空闲资源可以被任意 child 借用。
//
// 资源紧张时：
//   父节点对直接 child 运行加权 Water-Filling。
//
//   target[i] = min(demand[i], lambda * weight[i])
//
//   低需求 child 只拿到自己的 demand。
//   它未使用的份额会继续分给仍有需求的 child。
//   整数除法产生的余数会被丢弃，以保持实现简单。
//
// target 是本轮计算后允许继续申请到的目标 usage。
// target 不是可回收的硬上限。
//
// 某 child 可以暂时 used > target。
// 原因是它已经借用到的 token 无法被收回。
//
// 此时只拒绝该 child 的后续申请。
// 下一次 try_acquire 或 release 会用当前 usage 重新计算 target。
// 不保存跨轮次的 debt。
//
// 只有叶子节点能申请 token。
// 内部节点只聚合子树 usage/demand，并向下分配 target。
//
// 应在开始并发申请前构建完整树形关系。
// 树接入后，同一棵树共享一把锁。
// usage 更新和重平衡均在该锁内完成。
class AdaptiveResourceArbitrator : public std::enable_shared_from_this<AdaptiveResourceArbitrator> {
public:
    // 创建非根节点。
    //
    // 父节点资源不足时，weight 决定该节点与兄弟节点的相对份额。
    explicit AdaptiveResourceArbitrator(int32_t weight);

    // 创建根节点。
    //
    // total_quota_num 是整棵树 try_acquire 必须遵守的硬上限。
    explicit AdaptiveResourceArbitrator(int64_t total_quota_num);

    // 将 child 接到当前节点。
    //
    // parent 会用 shared_ptr 强持有 child。
    // 这样仲裁扫描可直接遍历 children，避免 weak_ptr::lock 和临时 child 列表。
    //
    // child 及其后代会改用当前树的共享锁。
    // 随后从根节点重算整棵树的 target。
    void add_child(const std::shared_ptr<AdaptiveResourceArbitrator>& child);

    // 显式移除没有在用资源的 child。
    //
    // parent 强持有 child，因此 child 结束时必须调用本方法解除父节点所有权。
    // 带有 usage 的子树不能移除。
    // 否则 token 释放时无法正确回溯到原来的父链。
    void remove_child(const std::shared_ptr<AdaptiveResourceArbitrator>& child);

    // 仅根节点可修改硬上限。
    //
    // 降低上限不会回收既有 token。
    // 新上限只影响后续申请。
    void update_limit(int64_t total_quota_num);

    // 原子地申请完整 quota_num。
    //
    // 调用时把本次请求临时计入 demand。
    // 随后重算每一层的 target。
    //
    // 请求路径上每一层和根节点都有完整配额时，才返回 token。
    // 否则返回 nullptr。
    //
    // 失败请求不会保留到下一次调用。
    std::shared_ptr<AdaptiveResourceToken> try_acquire(int64_t quota_num);

    // 强制申请完整 quota_num。
    //
    // 不检查 target 和任意层的 limit。
    // 此接口可能使 root.used 超过 root.limit。
    //
    // 仍会更新从叶子到根的 usage。
    // token 析构时会对称释放。
    // 后续 try_acquire 能看到这部分 usage。
    std::shared_ptr<AdaptiveResourceToken> acquire(int64_t quota_num);

    // 返回当前子树的实际 usage。
    // 返回最近一次重平衡得到的 target。
    // 这两个接口主要用于观测和测试。
    int64_t used_quota_num() const;
    int64_t target_quota_num() const;

private:
    friend class AdaptiveResourceToken;

    struct SharedState {
        std::mutex lock;
    };

    // try_acquire 的带锁实现。
    std::shared_ptr<AdaptiveResourceToken> try_acquire_impl(int64_t quota_num);
    // 由 token 析构调用，将 usage 从叶子节点一直扣减到根节点。
    void release(int64_t quota_num);

    // child 加入/移除树时递归切换共享锁。
    // 调用方必须持有旧树的共享锁。
    void rebind_shared_state(const std::shared_ptr<SharedState>& state);
    // 返回 parent 强持有的直接 child。
    //
    // 调用者必须持有 SharedState::lock。
    // 返回引用避免每次重平衡都复制 shared_ptr 或分配临时 vector。
    const std::vector<std::shared_ptr<AdaptiveResourceArbitrator>>& child_arbitrators() const;
    // 顺着 weak_ptr 父链找到根节点。
    std::shared_ptr<AdaptiveResourceArbitrator> root();
    // 只有没有直接 child 的节点才能实际持有 token。
    bool is_leaf();

    // 一次完整重平衡分两步。
    //
    // 第一步：自底向上刷新 demand。
    // 第二步：自顶向下分配 target。
    //
    // requester/requested_quota 只在 try_acquire 评估期间临时加入。
    // 不保存 debt 或持久等待请求。
    //
    // 调用者必须持有当前树的 SharedState::lock，并显式传入该锁。
    void rebalance(const AdaptiveResourceArbitrator* requester, int64_t requested_quota,
                   std::unique_lock<std::mutex>& lock);
    // 自底向上计算 demand。
    //
    // 叶子 demand = used + 本次临时请求。
    // 内部节点 demand = 直接 child demand 之和。
    //
    // 递归时原样传递当前树的 SharedState::lock。
    int64_t refresh_demand(const AdaptiveResourceArbitrator* requester, int64_t requested_quota,
                           std::unique_lock<std::mutex>& lock);
    // 使用父节点给定的 capacity 作为预算。
    //
    // 对直接 child 执行加权 Water-Filling。
    // 再将每个 child 的 target 递归传递到下一层。
    //
    // 递归时原样传递当前树的 SharedState::lock。
    void assign_targets(int64_t capacity, std::unique_lock<std::mutex>& lock);
    // 在刚完成重平衡后，计算从当前叶子到根路径上的最小可用资源数。
    int64_t available_quota_num();
    // 实际发放/释放 token 时沿父链增减 usage。
    //
    // 这样内部节点 usage 始终等于子树实际 usage 之和。
    void add_used_quota_num(int64_t quota_num);
    void sub_used_quota_num(int64_t quota_num);

private:
    // 节点相对父节点的权重；根节点不参与父级分配。
    int32_t _weight = 1;
    // 用于区分根节点与普通节点，只有根节点拥有可修改的硬上限。
    bool _has_explicit_total_quota_num = false;
    // 根节点硬上限；普通节点的实际预算来自每轮计算出的 _target_quota_num。
    int64_t _total_quota_num = 0;

    // 以下字段由 SharedState::lock 保护。
    // used 是已发 token 的实际使用量。
    // demand 是最近一次重平衡的中间状态。
    // target 是最近一次重平衡的结果。
    //
    // demand 不包含跨调用保存的等待量。
    int64_t _used_quota_num = 0;
    int64_t _demand_quota_num = 0;
    int64_t _target_quota_num = 0;
    // 同一棵树共享一把锁。
    //
    // parent 为弱引用，避免 child -> parent -> child 的所有权环。
    // children 为强引用，保证 child 在 parent 显式 remove_child 前持续存活。
    // 这也避免仲裁扫描中的 weak_ptr::lock、shared_ptr 临时复制和 vector 分配。
    // 树生命周期由调用方管理。
    //
    // token 只保持叶子节点存活。
    // 根节点销毁后，后续 token 释放不再维护树级 usage。
    std::shared_ptr<SharedState> _state = std::make_shared<SharedState>();
    std::weak_ptr<AdaptiveResourceArbitrator> _parent;
    std::vector<std::shared_ptr<AdaptiveResourceArbitrator>> _children;
};

} // namespace doris
