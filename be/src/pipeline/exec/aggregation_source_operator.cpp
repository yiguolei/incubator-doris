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

#include "aggregation_source_operator.h"

#include <memory>
#include <string>

#include "common/exception.h"
#include "common/status.h"
#include "pipeline/exec/operator.h"
#include "pipeline/exec/streaming_aggregation_source_operator.h"
#include "vec//utils/util.hpp"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {

OPERATOR_CODE_GENERATOR(AggSourceOperator, SourceOperator)

AggLocalState::AggLocalState(RuntimeState* state, OperatorXBase* parent)
        : Base(state, parent),
          _get_results_timer(nullptr),
          _serialize_result_timer(nullptr),
          _hash_table_iterate_timer(nullptr),
          _insert_keys_to_column_timer(nullptr),
          _serialize_data_timer(nullptr),
          _hash_table_size_counter(nullptr) {}

Status AggLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    _agg_data = _shared_state->agg_data.get();
    _get_results_timer = ADD_TIMER(profile(), "GetResultsTime");
    _serialize_result_timer = ADD_TIMER(profile(), "SerializeResultTime");
    _hash_table_iterate_timer = ADD_TIMER(profile(), "HashTableIterateTime");
    _insert_keys_to_column_timer = ADD_TIMER(profile(), "InsertKeysToColumnTime");
    _serialize_data_timer = ADD_TIMER(profile(), "SerializeDataTime");
    _hash_table_size_counter = ADD_COUNTER(profile(), "HashTableSize", TUnit::UNIT);
    auto& p = _parent->template cast<AggSourceOperatorX>();
    if (p._is_streaming) {
        _shared_state->data_queue->set_source_dependency(info.dependency);
    }
    if (p._without_key) {
        if (p._needs_finalize) {
            _executor.get_result = std::bind<Status>(&AggLocalState::_get_without_key_result, this,
                                                     std::placeholders::_1, std::placeholders::_2,
                                                     std::placeholders::_3);
        } else {
            _executor.get_result = std::bind<Status>(&AggLocalState::_serialize_without_key, this,
                                                     std::placeholders::_1, std::placeholders::_2,
                                                     std::placeholders::_3);
        }
    } else {
        if (p._needs_finalize) {
            _executor.get_result = std::bind<Status>(
                    &AggLocalState::_get_with_serialized_key_result, this, std::placeholders::_1,
                    std::placeholders::_2, std::placeholders::_3);
        } else {
            _executor.get_result = std::bind<Status>(
                    &AggLocalState::_serialize_with_serialized_key_result, this,
                    std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
        }
    }

    _shared_state->agg_data_created_without_key = p._without_key;
    return Status::OK();
}

Status AggLocalState::_destroy_agg_status(vectorized::AggregateDataPtr data) {
    auto& shared_state = *Base::_shared_state;
    for (int i = 0; i < shared_state.aggregate_evaluators.size(); ++i) {
        shared_state.aggregate_evaluators[i]->function()->destroy(
                data + shared_state.offsets_of_aggregate_states[i]);
    }
    return Status::OK();
}

Status AggLocalState::_serialize_with_serialized_key_result(RuntimeState* state,
                                                            vectorized::Block* block,
                                                            SourceState& source_state) {
    RETURN_IF_ERROR(_shared_state->sink_status_);
    RETURN_IF_ERROR(status_);
    if (_shared_state->_enable_spill) {
        return _serialize_with_serialized_key_result_with_spilt_data(state, block, source_state);
    } else {
        return _serialize_with_serialized_key_result_non_spill(state, block, source_state);
    }
}

Status AggLocalState::_serialize_with_serialized_key_result_with_spilt_data(
        RuntimeState* state, vectorized::Block* block, SourceState& source_state) {
    RETURN_IF_ERROR(_initiate_merge_spill_partition_agg_data(state));

    RETURN_IF_ERROR(_serialize_with_serialized_key_result_non_spill(state, block, source_state));
    if (source_state == SourceState::FINISHED) {
        source_state = _shared_state->read_cursor_ == _shared_state->partition_count_
                               ? SourceState::FINISHED
                               : SourceState::DEPEND_ON_SOURCE;
    }
    CHECK(!block->empty() || source_state == SourceState::FINISHED);
    return Status::OK();
}

Status AggLocalState::_serialize_with_serialized_key_result_non_spill(RuntimeState* state,
                                                                      vectorized::Block* block,
                                                                      SourceState& source_state) {
    SCOPED_TIMER(_serialize_result_timer);
    auto& shared_state = *_shared_state;
    int key_size = _shared_state->probe_expr_ctxs.size();
    int agg_size = _shared_state->aggregate_evaluators.size();
    vectorized::MutableColumns value_columns(agg_size);
    vectorized::DataTypes value_data_types(agg_size);

    // non-nullable column(id in `_make_nullable_keys`) will be converted to nullable.
    bool mem_reuse = shared_state.make_nullable_keys.empty() && block->mem_reuse();

    vectorized::MutableColumns key_columns;
    for (int i = 0; i < key_size; ++i) {
        if (mem_reuse) {
            key_columns.emplace_back(std::move(*block->get_by_position(i).column).mutate());
        } else {
            key_columns.emplace_back(
                    shared_state.probe_expr_ctxs[i]->root()->data_type()->create_column());
        }
    }

    SCOPED_TIMER(_get_results_timer);
    std::visit(
            [&](auto&& agg_method) -> void {
                agg_method.init_iterator();
                auto& data = *agg_method.hash_table;
                const auto size = std::min(data.size(), size_t(state->batch_size()));
                using KeyType = std::decay_t<decltype(agg_method.iterator->get_first())>;
                std::vector<KeyType> keys(size);
                if (shared_state.values.size() < size + 1) {
                    shared_state.values.resize(size + 1);
                }

                size_t num_rows = 0;
                shared_state.aggregate_data_container->init_once();
                auto& iter = shared_state.aggregate_data_container->iterator;

                {
                    SCOPED_TIMER(_hash_table_iterate_timer);
                    while (iter != shared_state.aggregate_data_container->end() &&
                           num_rows < state->batch_size()) {
                        keys[num_rows] = iter.template get_key<KeyType>();
                        shared_state.values[num_rows] = iter.get_aggregate_data();
                        ++iter;
                        ++num_rows;
                    }
                }

                {
                    SCOPED_TIMER(_insert_keys_to_column_timer);
                    agg_method.insert_keys_into_columns(keys, key_columns, num_rows);
                }

                if (iter == shared_state.aggregate_data_container->end()) {
                    if (agg_method.hash_table->has_null_key_data()) {
                        // only one key of group by support wrap null key
                        // here need additional processing logic on the null key / value
                        DCHECK(key_columns.size() == 1);
                        DCHECK(key_columns[0]->is_nullable());
                        if (agg_method.hash_table->has_null_key_data()) {
                            key_columns[0]->insert_data(nullptr, 0);
                            shared_state.values[num_rows] =
                                    agg_method.hash_table->template get_null_key_data<
                                            vectorized::AggregateDataPtr>();
                            ++num_rows;
                            source_state = SourceState::FINISHED;
                        }
                    } else {
                        source_state = SourceState::FINISHED;
                    }
                }

                {
                    SCOPED_TIMER(_serialize_data_timer);
                    for (size_t i = 0; i < shared_state.aggregate_evaluators.size(); ++i) {
                        value_data_types[i] = shared_state.aggregate_evaluators[i]
                                                      ->function()
                                                      ->get_serialized_type();
                        if (mem_reuse) {
                            value_columns[i] =
                                    std::move(*block->get_by_position(i + key_size).column)
                                            .mutate();
                        } else {
                            value_columns[i] = shared_state.aggregate_evaluators[i]
                                                       ->function()
                                                       ->create_serialize_column();
                        }
                        shared_state.aggregate_evaluators[i]->function()->serialize_to_column(
                                shared_state.values, shared_state.offsets_of_aggregate_states[i],
                                value_columns[i], num_rows);
                    }
                }
            },
            _agg_data->method_variant);

    if (!mem_reuse) {
        vectorized::ColumnsWithTypeAndName columns_with_schema;
        for (int i = 0; i < key_size; ++i) {
            columns_with_schema.emplace_back(std::move(key_columns[i]),
                                             shared_state.probe_expr_ctxs[i]->root()->data_type(),
                                             shared_state.probe_expr_ctxs[i]->root()->expr_name());
        }
        for (int i = 0; i < agg_size; ++i) {
            columns_with_schema.emplace_back(std::move(value_columns[i]), value_data_types[i], "");
        }
        *block = vectorized::Block(columns_with_schema);
    }

    return Status::OK();
}

Status AggLocalState::_get_with_serialized_key_result(RuntimeState* state, vectorized::Block* block,
                                                      SourceState& source_state) {
    RETURN_IF_ERROR(_shared_state->sink_status_);
    RETURN_IF_ERROR(status_);
    if (_shared_state->_enable_spill) {
        return _get_result_with_spilt_data(state, block, source_state);
    } else {
        return _get_result_with_serialized_key_non_spill(state, block, source_state);
    }
}

Status AggLocalState::_get_result_with_spilt_data(RuntimeState* state, vectorized::Block* block,
                                                  SourceState& source_state) {
    RETURN_IF_ERROR(_initiate_merge_spill_partition_agg_data(state));

    RETURN_IF_ERROR(_get_result_with_serialized_key_non_spill(state, block, source_state));
    if (source_state == SourceState::FINISHED) {
        source_state = _shared_state->read_cursor_ == _shared_state->partition_count_
                               ? SourceState::FINISHED
                               : SourceState::DEPEND_ON_SOURCE;
    }
    CHECK(!block->empty() || source_state == SourceState::FINISHED);
    return Status::OK();
}

Status AggLocalState::_initiate_merge_spill_partition_agg_data(RuntimeState* state) {
    DCHECK(!is_merging_);
    _shared_state->aggregate_data_container->init_once();
    if (_shared_state->aggregate_data_container->iterator !=
                _shared_state->aggregate_data_container->end() ||
        _shared_state->read_cursor_ == _shared_state->partition_count_) {
        return Status::OK();
    }
    is_merging_ = true;

    RETURN_IF_ERROR(_shared_state->reset_hash_table());
    RETURN_IF_ERROR(_shared_state->reset_agg_data());
    _dependency->Dependency::block();

    RETURN_IF_ERROR(
            ExecEnv::GetInstance()->spill_stream_mgr()->get_async_task_thread_pool()->submit_func(
                    [this] {
                        Defer defer {[&]() {
                            if (!status_.ok() ||
                                _shared_state->read_cursor_ == _shared_state->partition_count_) {
                                LOG(WARNING) << "agg node id: " << _parent->node_id()
                                             << ", operator id: " << _parent->operator_id()
                                             << " merge spilled agg data finished: " << status_;
                            }
                            _shared_state->aggregate_data_container->init_once();
                            {
                                std::unique_lock<std::mutex> lk(merge_spill_lock_);
                                is_merging_ = false;
                                _dependency->Dependency::set_ready();
                                merge_spill_cv_.notify_all();
                            }
                        }};
                        bool has_agg_data = false;
                        while (!_closed && !has_agg_data &&
                               _shared_state->read_cursor_ != _shared_state->partition_count_) {
                            // merge aggregation data of a spilled partition
                            VLOG_ROW
                                    << "agg node id: " << _parent->node_id()
                                    << ", operator id: " << _parent->operator_id()
                                    << " read spilled partition " << _shared_state->read_cursor_
                                    << ", stream count: " << _shared_state->spilled_streams_.size();
                            // optmize: initiate read for many streams and wait for read finish
                            // need to consider memory pressure
                            for (auto& stream : _shared_state->spilled_streams_) {
                                status_ = stream->seek_for_read(_shared_state->read_cursor_);
                                RETURN_IF_ERROR(status_);
                                vectorized::Block block;
                                bool eos;
                                status_ = stream->read_current_block_sync(&block, &eos);
                                RETURN_IF_ERROR(status_);

                                if (!block.empty()) {
                                    has_agg_data = true;
                                    status_ =
                                            _shared_state
                                                    ->merge_with_serialized_key_helper<false, true>(
                                                            &block);
                                    RETURN_IF_ERROR(status_);
                                }
                            }
                            _shared_state->read_cursor_++;
                        }
                        if (_shared_state->read_cursor_ == _shared_state->partition_count_) {
                            _shared_state->clear();
                        }
                        return status_;
                    }));
    return Status::WaitForIO("Merging spilt agg data");
}

Status AggLocalState::_get_result_with_serialized_key_non_spill(RuntimeState* state,
                                                                vectorized::Block* block,
                                                                SourceState& source_state) {
    auto& shared_state = *_shared_state;
    // non-nullable column(id in `_make_nullable_keys`) will be converted to nullable.
    bool mem_reuse = shared_state.make_nullable_keys.empty() && block->mem_reuse();

    auto columns_with_schema = vectorized::VectorizedUtils::create_columns_with_type_and_name(
            _parent->cast<AggSourceOperatorX>()._row_descriptor);
    int key_size = shared_state.probe_expr_ctxs.size();

    vectorized::MutableColumns key_columns;
    for (int i = 0; i < key_size; ++i) {
        if (!mem_reuse) {
            key_columns.emplace_back(columns_with_schema[i].type->create_column());
        } else {
            key_columns.emplace_back(std::move(*block->get_by_position(i).column).mutate());
        }
    }
    vectorized::MutableColumns value_columns;
    for (int i = key_size; i < columns_with_schema.size(); ++i) {
        if (!mem_reuse) {
            value_columns.emplace_back(columns_with_schema[i].type->create_column());
        } else {
            value_columns.emplace_back(std::move(*block->get_by_position(i).column).mutate());
        }
    }

    SCOPED_TIMER(_get_results_timer);
    std::visit(
            [&](auto&& agg_method) -> void {
                auto& data = *agg_method.hash_table;
                agg_method.init_iterator();
                const auto size = std::min(data.size(), size_t(state->batch_size()));
                using KeyType = std::decay_t<decltype(agg_method.iterator->get_first())>;
                std::vector<KeyType> keys(size);
                if (shared_state.values.size() < size) {
                    shared_state.values.resize(size);
                }

                size_t num_rows = 0;
                shared_state.aggregate_data_container->init_once();
                auto& iter = shared_state.aggregate_data_container->iterator;

                {
                    SCOPED_TIMER(_hash_table_iterate_timer);
                    while (iter != shared_state.aggregate_data_container->end() &&
                           num_rows < state->batch_size()) {
                        keys[num_rows] = iter.template get_key<KeyType>();
                        shared_state.values[num_rows] = iter.get_aggregate_data();
                        ++iter;
                        ++num_rows;
                    }
                }

                {
                    SCOPED_TIMER(_insert_keys_to_column_timer);
                    agg_method.insert_keys_into_columns(keys, key_columns, num_rows);
                }

                for (size_t i = 0; i < shared_state.aggregate_evaluators.size(); ++i) {
                    shared_state.aggregate_evaluators[i]->insert_result_info_vec(
                            shared_state.values, shared_state.offsets_of_aggregate_states[i],
                            value_columns[i].get(), num_rows);
                }

                if (iter == shared_state.aggregate_data_container->end()) {
                    if (agg_method.hash_table->has_null_key_data()) {
                        // only one key of group by support wrap null key
                        // here need additional processing logic on the null key / value
                        DCHECK(key_columns.size() == 1);
                        DCHECK(key_columns[0]->is_nullable());
                        if (key_columns[0]->size() < state->batch_size()) {
                            key_columns[0]->insert_data(nullptr, 0);
                            auto mapped = agg_method.hash_table->template get_null_key_data<
                                    vectorized::AggregateDataPtr>();
                            for (size_t i = 0; i < shared_state.aggregate_evaluators.size(); ++i)
                                shared_state.aggregate_evaluators[i]->insert_result_info(
                                        mapped + shared_state.offsets_of_aggregate_states[i],
                                        value_columns[i].get());
                            source_state = SourceState::FINISHED;
                        }
                    } else {
                        source_state = SourceState::FINISHED;
                    }
                }
            },
            _agg_data->method_variant);

    if (!mem_reuse) {
        *block = columns_with_schema;
        vectorized::MutableColumns columns(block->columns());
        for (int i = 0; i < block->columns(); ++i) {
            if (i < key_size) {
                columns[i] = std::move(key_columns[i]);
            } else {
                columns[i] = std::move(value_columns[i - key_size]);
            }
        }
        block->set_columns(std::move(columns));
    }

    return Status::OK();
}

Status AggLocalState::_serialize_without_key(RuntimeState* state, vectorized::Block* block,
                                             SourceState& source_state) {
    auto& shared_state = *_shared_state;
    // 1. `child(0)->rows_returned() == 0` mean not data from child
    // in level two aggregation node should return NULL result
    //    level one aggregation node set `eos = true` return directly
    SCOPED_TIMER(_serialize_result_timer);
    if (UNLIKELY(_shared_state->input_num_rows == 0)) {
        source_state = SourceState::FINISHED;
        return Status::OK();
    }
    block->clear();

    DCHECK(_agg_data->without_key != nullptr);
    int agg_size = shared_state.aggregate_evaluators.size();

    vectorized::MutableColumns value_columns(agg_size);
    std::vector<vectorized::DataTypePtr> data_types(agg_size);
    // will serialize data to string column
    for (int i = 0; i < shared_state.aggregate_evaluators.size(); ++i) {
        data_types[i] = shared_state.aggregate_evaluators[i]->function()->get_serialized_type();
        value_columns[i] =
                shared_state.aggregate_evaluators[i]->function()->create_serialize_column();
    }

    for (int i = 0; i < shared_state.aggregate_evaluators.size(); ++i) {
        shared_state.aggregate_evaluators[i]->function()->serialize_without_key_to_column(
                _agg_data->without_key + shared_state.offsets_of_aggregate_states[i],
                *value_columns[i]);
    }

    {
        vectorized::ColumnsWithTypeAndName data_with_schema;
        for (int i = 0; i < shared_state.aggregate_evaluators.size(); ++i) {
            vectorized::ColumnWithTypeAndName column_with_schema = {nullptr, data_types[i], ""};
            data_with_schema.push_back(std::move(column_with_schema));
        }
        *block = vectorized::Block(data_with_schema);
    }

    block->set_columns(std::move(value_columns));
    source_state = SourceState::FINISHED;
    return Status::OK();
}

Status AggLocalState::_get_without_key_result(RuntimeState* state, vectorized::Block* block,
                                              SourceState& source_state) {
    auto& shared_state = *_shared_state;
    DCHECK(_agg_data->without_key != nullptr);
    block->clear();

    auto& p = _parent->cast<AggSourceOperatorX>();
    *block = vectorized::VectorizedUtils::create_empty_columnswithtypename(p._row_descriptor);
    int agg_size = shared_state.aggregate_evaluators.size();

    vectorized::MutableColumns columns(agg_size);
    std::vector<vectorized::DataTypePtr> data_types(agg_size);
    for (int i = 0; i < shared_state.aggregate_evaluators.size(); ++i) {
        data_types[i] = shared_state.aggregate_evaluators[i]->function()->get_return_type();
        columns[i] = data_types[i]->create_column();
    }

    for (int i = 0; i < shared_state.aggregate_evaluators.size(); ++i) {
        auto column = columns[i].get();
        shared_state.aggregate_evaluators[i]->insert_result_info(
                _agg_data->without_key + shared_state.offsets_of_aggregate_states[i], column);
    }

    const auto& block_schema = block->get_columns_with_type_and_name();
    DCHECK_EQ(block_schema.size(), columns.size());
    for (int i = 0; i < block_schema.size(); ++i) {
        const auto column_type = block_schema[i].type;
        if (!column_type->equals(*data_types[i])) {
            if (!vectorized::is_array(remove_nullable(column_type))) {
                if (!column_type->is_nullable() || data_types[i]->is_nullable() ||
                    !remove_nullable(column_type)->equals(*data_types[i])) {
                    return Status::InternalError(
                            "node id = {}, column_type not match data_types, column_type={}, "
                            "data_types={}",
                            _parent->node_id(), column_type->get_name(), data_types[i]->get_name());
                }
            }

            if (column_type->is_nullable() && !data_types[i]->is_nullable()) {
                vectorized::ColumnPtr ptr = std::move(columns[i]);
                // unless `count`, other aggregate function dispose empty set should be null
                // so here check the children row return
                ptr = make_nullable(ptr, shared_state.input_num_rows == 0);
                columns[i] = ptr->assume_mutable();
            }
        }
    }

    block->set_columns(std::move(columns));
    source_state = SourceState::FINISHED;
    return Status::OK();
}

AggSourceOperatorX::AggSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                                       const DescriptorTbl& descs, bool is_streaming)
        : Base(pool, tnode, operator_id, descs),
          _is_streaming(is_streaming),
          _needs_finalize(tnode.agg_node.need_finalize),
          _without_key(tnode.agg_node.grouping_exprs.empty()) {}

Status AggSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                     SourceState& source_state) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    RETURN_IF_ERROR(local_state._executor.get_result(state, block, source_state));
    local_state.make_nullable_output_key(block);
    // dispose the having clause, should not be execute in prestreaming agg
    RETURN_IF_ERROR(vectorized::VExprContext::filter_block(_conjuncts, block, block->columns()));
    local_state.reached_limit(block, source_state);
    return Status::OK();
}

void AggLocalState::make_nullable_output_key(vectorized::Block* block) {
    if (block->rows() != 0) {
        for (auto cid : _shared_state->make_nullable_keys) {
            block->get_by_position(cid).column = make_nullable(block->get_by_position(cid).column);
            block->get_by_position(cid).type = make_nullable(block->get_by_position(cid).type);
        }
    }
}

Status AggLocalState::close(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_close_timer);
    if (_closed) {
        return Status::OK();
    }
    {
        std::unique_lock<std::mutex> lk(merge_spill_lock_);
        if (is_merging_) {
            merge_spill_cv_.wait(lk);
        }
    }

    /// _hash_table_size_counter may be null if prepare failed.
    if (_hash_table_size_counter) {
        std::visit(
                [&](auto&& agg_method) {
                    COUNTER_SET(_hash_table_size_counter, int64_t(agg_method.hash_table->size()));
                },
                _agg_data->method_variant);
    }
    RETURN_IF_ERROR(Base::close(state));
    _shared_state->clear();
    return Status::OK();
}

} // namespace doris::pipeline
