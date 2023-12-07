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

#include "dependency.h"

#include <memory>
#include <mutex>

#include "common/logging.h"
#include "pipeline/pipeline_fragment_context.h"
#include "pipeline/pipeline_x/local_exchange/local_exchanger.h"
#include "pipeline/pipeline_x/pipeline_x_task.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {

void Dependency::_add_block_task(PipelineXTask* task) {
    DCHECK(_blocked_task.empty() || _blocked_task[_blocked_task.size() - 1] != task)
            << "Duplicate task: " << task->debug_string();
    _blocked_task.push_back(task);
}

void Dependency::set_ready() {
    if (_ready) {
        return;
    }
    _watcher.stop();
    std::vector<PipelineXTask*> local_block_task {};
    {
        std::unique_lock<std::mutex> lc(_task_lock);
        if (_ready) {
            return;
        }
        _ready = true;
        local_block_task.swap(_blocked_task);
    }
    for (auto* task : local_block_task) {
        task->wake_up();
    }
}

Dependency* Dependency::is_blocked_by(PipelineXTask* task) {
    std::unique_lock<std::mutex> lc(_task_lock);
    auto ready = _ready.load() || _is_cancelled();
    if (!ready && task) {
        _add_block_task(task);
    }
    return ready ? nullptr : this;
}

Dependency* FinishDependency::is_blocked_by(PipelineXTask* task) {
    std::unique_lock<std::mutex> lc(_task_lock);
    auto ready = _ready.load();
    if (!ready && task) {
        _add_block_task(task);
    }
    return ready ? nullptr : this;
}

Dependency* RuntimeFilterDependency::is_blocked_by(PipelineXTask* task) {
    if (!_blocked_by_rf) {
        return nullptr;
    }
    std::unique_lock<std::mutex> lc(_task_lock);
    if (*_blocked_by_rf && !_is_cancelled()) {
        if (LIKELY(task)) {
            _add_block_task(task);
        }
        return this;
    }
    return nullptr;
}

std::string Dependency::debug_string(int indentation_level) {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}{}: id={}, block task = {}, ready={}",
                   std::string(indentation_level * 2, ' '), _name, _node_id, _blocked_task.size(),
                   _ready);
    return fmt::to_string(debug_string_buffer);
}

std::string RuntimeFilterDependency::debug_string(int indentation_level) {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer,
                   "{}{}: id={}, block task = {}, ready={}, _filters = {}, _blocked_by_rf = {}",
                   std::string(indentation_level * 2, ' '), _name, _node_id, _blocked_task.size(),
                   _ready, _filters.load(), _blocked_by_rf ? _blocked_by_rf->load() : false);
    return fmt::to_string(debug_string_buffer);
}

std::string AndDependency::debug_string(int indentation_level) {
    fmt::memory_buffer debug_string_buffer;
    fmt::format_to(debug_string_buffer, "{}{}: id={}, children=[",
                   std::string(indentation_level * 2, ' '), _name, _node_id);
    for (auto& child : _children) {
        fmt::format_to(debug_string_buffer, "{}, \n", child->debug_string(indentation_level = 1));
    }
    fmt::format_to(debug_string_buffer, "{}]", std::string(indentation_level * 2, ' '));
    return fmt::to_string(debug_string_buffer);
}

bool RuntimeFilterTimer::has_ready() {
    std::unique_lock<std::mutex> lc(_lock);
    return _is_ready;
}

void RuntimeFilterTimer::call_timeout() {
    std::unique_lock<std::mutex> lc(_lock);
    if (_call_ready) {
        return;
    }
    _call_timeout = true;
    if (_parent) {
        _parent->sub_filters();
    }
}

void RuntimeFilterTimer::call_ready() {
    std::unique_lock<std::mutex> lc(_lock);
    if (_call_timeout) {
        return;
    }
    _call_ready = true;
    if (_parent) {
        _parent->sub_filters();
    }
    _is_ready = true;
}

void RuntimeFilterTimer::call_has_ready() {
    std::unique_lock<std::mutex> lc(_lock);
    DCHECK(!_call_timeout);
    if (!_call_ready) {
        _parent->sub_filters();
    }
}

void RuntimeFilterTimer::call_has_release() {
    // When the use count is equal to 1, only the timer queue still holds ownership,
    // so there is no need to take any action.
}

void RuntimeFilterDependency::add_filters(IRuntimeFilter* runtime_filter) {
    _filters++;
    int64_t registration_time = runtime_filter->registration_time();
    int32 wait_time_ms = runtime_filter->wait_time_ms();
    auto filter_timer = std::make_shared<RuntimeFilterTimer>(
            registration_time, wait_time_ms,
            std::dynamic_pointer_cast<RuntimeFilterDependency>(shared_from_this()));
    runtime_filter->set_filter_timer(filter_timer);
    ExecEnv::GetInstance()->runtime_filter_timer_queue()->push_filter_timer(filter_timer);
}

void RuntimeFilterDependency::sub_filters() {
    auto value = _filters.fetch_sub(1);
    if (value == 1) {
        _watcher.stop();
        std::vector<PipelineXTask*> local_block_task {};
        {
            std::unique_lock<std::mutex> lc(_task_lock);
            *_blocked_by_rf = false;
            local_block_task.swap(_blocked_task);
        }
        for (auto* task : local_block_task) {
            task->wake_up();
        }
    }
}

void LocalExchangeSharedState::sub_running_sink_operators() {
    std::unique_lock<std::mutex> lc(le_lock);
    if (exchanger->_running_sink_operators.fetch_sub(1) == 1) {
        _set_ready_for_read();
    }
}

LocalExchangeSharedState::LocalExchangeSharedState(int num_instances) {
    source_dependencies.resize(num_instances, nullptr);
    mem_trackers.resize(num_instances, nullptr);
}

void AggSharedState::clear() {
    for (auto& stream : spilled_streams_) {
        (void)ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
    }
    spilled_streams_.clear();
}
void AggSharedState::_find_in_hash_table(vectorized::AggregateDataPtr* places,
                                         vectorized::ColumnRawPtrs& key_columns, size_t num_rows) {
    std::visit(
            [&](auto&& agg_method) -> void {
                using HashMethodType = std::decay_t<decltype(agg_method)>;
                using AggState = typename HashMethodType::State;
                AggState state(key_columns);
                agg_method.init_serialized_keys(key_columns, num_rows);

                /// For all rows.
                for (size_t i = 0; i < num_rows; ++i) {
                    auto find_result = agg_method.find(state, i);

                    if (find_result.is_found()) {
                        places[i] = find_result.get_mapped();
                    } else {
                        places[i] = nullptr;
                    }
                }
            },
            agg_data->method_variant);
}

Status AggSharedState::_create_agg_status(vectorized::AggregateDataPtr data) {
    for (int i = 0; i < aggregate_evaluators.size(); ++i) {
        try {
            aggregate_evaluators[i]->create(data + offsets_of_aggregate_states[i]);
        } catch (...) {
            for (int j = 0; j < i; ++j) {
                aggregate_evaluators[j]->destroy(data + offsets_of_aggregate_states[j]);
            }
            throw;
        }
    }
    return Status::OK();
}
void AggSharedState::_emplace_into_hash_table(vectorized::AggregateDataPtr* places,
                                              vectorized::ColumnRawPtrs& key_columns,
                                              const size_t num_rows) {
    std::visit(
            [&](auto&& agg_method) -> void {
                // SCOPED_TIMER(_hash_table_compute_timer);
                using HashMethodType = std::decay_t<decltype(agg_method)>;
                using AggState = typename HashMethodType::State;
                AggState state(key_columns);
                agg_method.init_serialized_keys(key_columns, num_rows);

                auto creator = [this](const auto& ctor, auto& key, auto& origin) {
                    HashMethodType::try_presis_key_and_origin(key, origin, *agg_arena_pool);
                    auto mapped = aggregate_data_container->append_data(origin);
                    auto st = _create_agg_status(mapped);
                    if (!st) {
                        throw Exception(st.code(), st.to_string());
                    }
                    ctor(key, mapped);
                };

                auto creator_for_null_key = [&](auto& mapped) {
                    mapped = agg_arena_pool->aligned_alloc(total_size_of_aggregate_states,
                                                           align_aggregate_states);
                    auto st = _create_agg_status(mapped);
                    if (!st) {
                        throw Exception(st.code(), st.to_string());
                    }
                };

                // SCOPED_TIMER(_hash_table_emplace_timer);
                for (size_t i = 0; i < num_rows; ++i) {
                    places[i] = agg_method.lazy_emplace(state, i, creator, creator_for_null_key);
                }

                // COUNTER_UPDATE(_hash_table_input_counter, num_rows);
            },
            agg_data->method_variant);
}

size_t AggSharedState::_get_hash_table_size() const {
    return std::visit([&](auto&& agg_method) { return agg_method.hash_table->size(); },
                      agg_data->method_variant);
}

void SortSharedState::clear() {
    for (auto& stream : sorted_streams_) {
        (void)ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
    }
    sorted_streams_.clear();
}
} // namespace doris::pipeline
