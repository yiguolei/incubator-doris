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

#include "runtime/adaptive_resource_arbitrator.h"

#include <algorithm>
#include <limits>
#include <utility>

#include "common/status.h"

namespace doris {
namespace {

// 配额和权重均为非负整数。
//
// C++ 的整数除法在这里就是向下取整。
// 不使用浮点 std::floor，避免大配额转换为 double 后丢失精度。
int64_t floor_divide(__int128 numerator, int64_t denominator) {
    DCHECK(numerator >= 0);
    DCHECK(denominator > 0);
    return static_cast<int64_t>(numerator / denominator);
}

} // namespace

AdaptiveResourceToken::~AdaptiveResourceToken() {
    // 资源的唯一归还时机：使用方持有的 token 生命周期结束。
    if (_quota_num > 0) {
        _arbitrator->release(_quota_num);
    }
}

AdaptiveResourceArbitrator::AdaptiveResourceArbitrator(int32_t weight) : _weight(weight) {
    DCHECK(weight > 0);
}

AdaptiveResourceArbitrator::AdaptiveResourceArbitrator(int64_t total_quota_num)
        : _has_explicit_total_quota_num(true), _total_quota_num(total_quota_num) {
    DCHECK(total_quota_num >= 0);
}

void AdaptiveResourceArbitrator::add_child(
        const std::shared_ptr<AdaptiveResourceArbitrator>& child) {
    DCHECK(child != nullptr);
    DCHECK(child.get() != this);
    // child 的预算必须完全由父节点本轮 target 决定。
    // 带显式总限额的节点只能作为独立 root，不能接入另一棵树。
    DCHECK(!child->_has_explicit_total_quota_num);

    std::unique_lock<std::mutex> lock(_state->lock);
    DCHECK(child->_parent.expired());

    // 一个 child 只能挂载一次。
    // 重复挂载会破坏 usage 向父链聚合的语义。
    const auto& children = child_arbitrators();
    for (const auto& existing_child : children) {
        DCHECK(existing_child.get() != child.get());
    }

    // 接入后的所有节点必须共享同一把锁。
    // 这样整棵树重平衡时才能看到一致状态。
    child->rebind_shared_state(_state);
    child->_parent = shared_from_this();
    _children.emplace_back(child);
    // 当前节点可能是中间节点。
    //
    // 因此必须从根开始重平衡。
    // 普通节点没有独立硬上限，不能使用其默认的 _total_quota_num。
    root()->rebalance(nullptr, 0, lock);
}

void AdaptiveResourceArbitrator::remove_child(
        const std::shared_ptr<AdaptiveResourceArbitrator>& child) {
    DCHECK(child != nullptr);

    std::unique_lock<std::mutex> lock(_state->lock);
    DCHECK(child->_parent.lock().get() == this);
    // 仍有 token 的子树不能脱离父树。
    // 否则 token 析构时无法保持父级 usage 正确。
    DCHECK(child->_used_quota_num == 0);

    _children.erase(
            std::remove_if(_children.begin(), _children.end(),
                           [&child](const std::shared_ptr<AdaptiveResourceArbitrator>& item) {
                               return item.get() == child.get();
                           }),
            _children.end());
    child->_parent.reset();
    child->rebind_shared_state(std::make_shared<SharedState>());
    root()->rebalance(nullptr, 0, lock);
}

void AdaptiveResourceArbitrator::update_limit(int64_t total_quota_num) {
    DCHECK(total_quota_num >= 0);
    DCHECK(_has_explicit_total_quota_num);

    std::unique_lock<std::mutex> lock(_state->lock);
    _total_quota_num = total_quota_num;
    // 新 limit 可能小于既有 used。
    //
    // 此时只更新后续准入目标。
    // 已发 token 不会被回收。
    rebalance(nullptr, 0, lock);
}

std::shared_ptr<AdaptiveResourceToken> AdaptiveResourceArbitrator::try_acquire(int64_t quota_num) {
    return try_acquire_impl(quota_num);
}

std::shared_ptr<AdaptiveResourceToken> AdaptiveResourceArbitrator::acquire(int64_t quota_num) {
    DCHECK(quota_num > 0);

    std::unique_lock<std::mutex> lock(_state->lock);
    DCHECK(is_leaf());

    // 强制路径故意跳过 available_quota_num()。
    //
    // 但仍需更新 usage 和 target。
    // 后续普通 try_acquire 必须感知已经发生的超限占用。
    auto root_arbitrator = root();
    add_used_quota_num(quota_num);
    root_arbitrator->rebalance(nullptr, 0, lock);
    return std::make_shared<AdaptiveResourceToken>(shared_from_this(), quota_num);
}

int64_t AdaptiveResourceArbitrator::used_quota_num() const {
    std::lock_guard<std::mutex> lock(_state->lock);
    return _used_quota_num;
}

int64_t AdaptiveResourceArbitrator::target_quota_num() const {
    std::lock_guard<std::mutex> lock(_state->lock);
    return _target_quota_num;
}

std::shared_ptr<AdaptiveResourceToken> AdaptiveResourceArbitrator::try_acquire_impl(
        int64_t quota_num) {
    DCHECK(quota_num > 0);

    std::unique_lock<std::mutex> lock(_state->lock);
    DCHECK(is_leaf());

    auto root_arbitrator = root();
    // 先把本次完整请求临时加入 demand。
    // 再进行 Water-Filling。
    //
    // 示例：
    //   A.used = 8
    //   B 本次申请 3
    //
    // 本轮按需求 [8, 3] 计算。
    // 不能只按旧 usage [8, 0] 计算。
    root_arbitrator->rebalance(this, quota_num, lock);
    if (available_quota_num() < quota_num) {
        // all-or-nothing：资源不够时不发部分 token。
        //
        // 重新计算一次，去掉本次临时需求。
        // 失败请求不会保留到下一次调用。
        root_arbitrator->rebalance(nullptr, 0, lock);
        return nullptr;
    }

    // 发放后 request 已经变成真实 usage。
    // 重新计算一次 target，以去掉临时请求。
    add_used_quota_num(quota_num);
    root_arbitrator->rebalance(nullptr, 0, lock);
    return std::make_shared<AdaptiveResourceToken>(shared_from_this(), quota_num);
}

void AdaptiveResourceArbitrator::release(int64_t quota_num) {
    DCHECK(quota_num >= 0);

    std::unique_lock<std::mutex> lock(_state->lock);
    // token 析构后 usage 减少。
    //
    // 根节点仍存活时，后续申请会基于新 usage 再次仲裁。
    // 根节点已经析构时，父链在此处结束。
    // 此时只清理仍存活节点的本地 usage。
    sub_used_quota_num(quota_num);
    auto root_arbitrator = root();
    // parent 是 weak_ptr。原始 root 被析构后，root() 只能返回当前仍存活父链的
    // 最高节点，通常就是持有 token 的叶子节点。
    //
    // 示例：
    //   显式 root -> child(weight) -> leaf(token)
    //
    // 显式 root 析构后，leaf.root() 返回 leaf 自己。
    // leaf 按 weight 构造，_has_explicit_total_quota_num 为 false。
    // 此时没有仍存活的硬上限节点，因此跳过重平衡。
    if (root_arbitrator->_has_explicit_total_quota_num) {
        root_arbitrator->rebalance(nullptr, 0, lock);
    }
}

void AdaptiveResourceArbitrator::rebind_shared_state(const std::shared_ptr<SharedState>& state) {
    // 子树整体迁移到父树的锁域。
    // 该操作只允许在建树或拆树阶段进行。
    _state = state;
    for (const auto& child : child_arbitrators()) {
        child->rebind_shared_state(state);
    }
}

const std::vector<std::shared_ptr<AdaptiveResourceArbitrator>>&
AdaptiveResourceArbitrator::child_arbitrators() const {
    return _children;
}

std::shared_ptr<AdaptiveResourceArbitrator> AdaptiveResourceArbitrator::root() {
    auto current = shared_from_this();
    while (auto parent = current->_parent.lock()) {
        current = std::move(parent);
    }
    return current;
}

bool AdaptiveResourceArbitrator::is_leaf() {
    return child_arbitrators().empty();
}

void AdaptiveResourceArbitrator::rebalance(const AdaptiveResourceArbitrator* requester,
                                           int64_t requested_quota,
                                           std::unique_lock<std::mutex>& lock) {
    DCHECK(lock.owns_lock());
    DCHECK(lock.mutex() == &_state->lock);

    // 先聚合真实 usage 和本次临时请求。
    // 再把根限额逐层分发为动态 target。
    refresh_demand(requester, requested_quota, lock);
    assign_targets(_total_quota_num, lock);
}

int64_t AdaptiveResourceArbitrator::refresh_demand(const AdaptiveResourceArbitrator* requester,
                                                   int64_t requested_quota,
                                                   std::unique_lock<std::mutex>& lock) {
    DCHECK(lock.owns_lock());
    DCHECK(lock.mutex() == &_state->lock);

    const auto& children = child_arbitrators();
    if (children.empty()) {
        // 叶子节点只持有自身 token。
        // pending 不持久保存。
        // 本次请求由 requester 临时注入。
        _demand_quota_num = _used_quota_num;
    } else {
        _demand_quota_num = 0;
        for (const auto& child : children) {
            _demand_quota_num += child->refresh_demand(requester, requested_quota, lock);
        }
        DCHECK(_used_quota_num <= _demand_quota_num);
    }
    if (this == requester) {
        // requester 必定是叶子。
        // 该值只影响当前 try_acquire。
        // 调用结束后会消失。
        _demand_quota_num += requested_quota;
    }
    return _demand_quota_num;
}

void AdaptiveResourceArbitrator::assign_targets(int64_t capacity,
                                                std::unique_lock<std::mutex>& lock) {
    DCHECK(lock.owns_lock());
    DCHECK(lock.mutex() == &_state->lock);

    // capacity 由父节点在本轮下发。
    //
    // 资源充足时 target = demand。
    // 资源紧张时 target 由下面的 Water-Filling 计算得出。
    _target_quota_num = std::min(_demand_quota_num, capacity);
    const auto& children = child_arbitrators();
    if (children.empty()) {
        return;
    }

    int64_t total_demand = 0;
    // 汇总当前层所有直接 child 的 demand。
    // 用于判断父节点下发的 capacity 是否足够。
    //
    // 内部节点 demand 应等于直接 child demand 之和。
    // 随后的 DCHECK 验证该不变量。
    for (const auto& child : children) {
        total_demand += child->_demand_quota_num;
    }
    DCHECK(total_demand == _demand_quota_num);

    if (total_demand <= _target_quota_num) {
        // 没有竞争。
        // child 可以按需使用，weight 不参与限制。
        //
        // 将完整 demand 原样作为 child 的 capacity。
        // child 再将该 capacity 继续传给自己的后代。
        for (const auto& child : children) {
            child->assign_targets(child->_demand_quota_num, lock);
        }
        return;
    }

    // 有竞争时，active 保存尚未满足 demand 的 child。
    //
    // 每轮先找出需求不超过其比例份额的 child。
    // 它们会被完全满足。
    // 它们未使用的份额留给下一轮。
    std::vector<bool> active(children.size(), false);
    size_t active_num = 0;
    int64_t remaining_capacity = _target_quota_num;
    // 初始化本轮参与竞争的 child。
    //
    // demand 为 0 的 child 不需要分配。
    // 它们不参与 total_weight 计算。
    // 它们的 child_capacities 保持为 0。
    for (size_t index = 0; index < children.size(); ++index) {
        active[index] = children[index]->_demand_quota_num > 0;
        if (active[index]) {
            ++active_num;
        }
    }

    std::vector<int64_t> child_capacities(children.size(), 0);
    while (active_num > 0) {
        int64_t total_weight = 0;
        // 只累计 active child 的权重。
        //
        // 已完全满足的 child 已经固定 target。
        // 它们不能再参与下一轮比例分配。
        for (size_t index = 0; index < children.size(); ++index) {
            if (active[index]) {
                total_weight += children[index]->_weight;
            }
        }
        DCHECK(total_weight > 0);

        bool has_saturated_child = false;
        // 根据剩余资源和剩余权重计算理论份额。
        //
        // 找出 demand 不超过理论份额的 child。
        // 这些 child 可以一次性满足。
        //
        // 未用完的比例份额会归还到 remaining_capacity。
        // 下一轮由高需求 child 继续竞争。
        for (size_t index = 0; index < children.size(); ++index) {
            if (!active[index]) {
                continue;
            }
            const int64_t proportional_share = floor_divide(
                    static_cast<__int128>(remaining_capacity) * children[index]->_weight,
                    total_weight);
            if (children[index]->_demand_quota_num <= proportional_share) {
                // 该 child 吃不完按 weight 分到的份额。
                //
                // 先固定为其真实 demand。
                // 剩余资源在下一轮按 active child 的 weight 再分配。
                child_capacities[index] = children[index]->_demand_quota_num;
                remaining_capacity -= child_capacities[index];
                active[index] = false;
                --active_num;
                has_saturated_child = true;
            }
        }
        if (has_saturated_child) {
            continue;
        }

        // 所有剩余 child 都有足够需求。
        // 现在按 weight 做最后一次比例切分。
        //
        // 使用 128 位中间值避免 capacity * weight 溢出。
        // floor_divide 明确表示整数配额向下取整。
        // 取整后的余数不再补偿，可能留下少量未分配的父节点资源。
        // 上一轮筛选后，所有 active child 的 demand 都大于理论份额。
        // 因此本轮可以直接切分全部 remaining_capacity。
        //
        // 先把整数商写入 child_capacities。
        // 余数直接丢弃。
        for (size_t index = 0; index < children.size(); ++index) {
            if (!active[index]) {
                continue;
            }
            const __int128 weighted_capacity =
                    static_cast<__int128>(remaining_capacity) * children[index]->_weight;
            child_capacities[index] = floor_divide(weighted_capacity, total_weight);
        }
        break;
    }

    // 当前层每个 child 的动态 capacity 已经确定。
    //
    // 递归进入下一层。
    // child 在自己的 capacity 内继续做同样的计算。
    for (size_t index = 0; index < children.size(); ++index) {
        children[index]->assign_targets(child_capacities[index], lock);
    }
}

int64_t AdaptiveResourceArbitrator::available_quota_num() {
    // 请求必须同时满足叶子自身 target、每一级祖先 target 和根硬上限。
    // 因此取路径上所有剩余量的最小值。
    //
    // used > target 时，剩余量为负。
    // 返回前会截断为 0。
    // 不能只检查叶子，因为非抢占式借用允许兄弟节点暂时 used > target。
    //
    // 示例：
    //   parent.target = 10
    //   A.target = 7, A.used = 8
    //   B.target = 3, B.used = 0
    //
    // B 自己还可使用 3，但 parent 只剩 10 - (8 + 0) = 2。
    // 若不检查 parent，给 B 3 个 token 会使 parent 的实际 usage 达到 11。
    // 多层树中同样需要检查每一级父节点的剩余量。
    int64_t available_quota_num = std::numeric_limits<int64_t>::max();
    auto current = shared_from_this();
    while (true) {
        available_quota_num = std::min(available_quota_num,
                                       current->_target_quota_num - current->_used_quota_num);
        auto parent = current->_parent.lock();
        if (parent == nullptr) {
            available_quota_num = std::min(available_quota_num,
                                           current->_total_quota_num - current->_used_quota_num);
            return std::max<int64_t>(available_quota_num, 0);
        }
        current = std::move(parent);
    }
}

void AdaptiveResourceArbitrator::add_used_quota_num(int64_t quota_num) {
    // 叶子发一个 token。
    // 所有祖先子树的真实 usage 都必须同步增加。
    auto current = shared_from_this();
    while (current != nullptr) {
        current->_used_quota_num += quota_num;
        current = current->_parent.lock();
    }
}

void AdaptiveResourceArbitrator::sub_used_quota_num(int64_t quota_num) {
    // 与 add_used_quota_num 对称。
    // DCHECK 保证 token 不会重复释放或跨树释放。
    auto current = shared_from_this();
    while (current != nullptr) {
        DCHECK(current->_used_quota_num >= quota_num);
        current->_used_quota_num -= quota_num;
        current = current->_parent.lock();
    }
}

} // namespace doris
