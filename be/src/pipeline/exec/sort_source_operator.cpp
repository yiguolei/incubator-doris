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

#include "sort_source_operator.h"

#include <string>

#include "pipeline/exec/operator.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {

OPERATOR_CODE_GENERATOR(SortSourceOperator, SourceOperator)

SortLocalState::SortLocalState(RuntimeState* state, OperatorXBase* parent)
        : PipelineXLocalState<SortSourceDependency>(state, parent) {
    if (state->external_sort_bytes_threshold() > 0) {
        external_sort_bytes_threshold_ = state->external_sort_bytes_threshold();
    }
}

Status SortLocalState::close(RuntimeState* state) {
    if (_closed) {
        return Status::OK();
    }
    {
        std::unique_lock<std::mutex> lk(merge_spill_lock_);
        if (is_merging_) {
            merge_spill_cv_.wait(lk);
        }
    }
    RETURN_IF_ERROR(Base::close(state));
    for (auto& stream : current_merging_streams_) {
        (void)ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
    }
    current_merging_streams_.clear();
    _shared_state->clear();
    return Status::OK();
}

int SortLocalState::_calc_spill_blocks_to_merge() const {
    int count = external_sort_bytes_threshold_ / SortSharedState::SORT_BLOCK_SPILL_BATCH_BYTES;
    return std::max(2, count);
}

Status SortLocalState::initiate_merge_sort_spill_streams(RuntimeState* state) {
    LOG(INFO) << _shared_state << " sort node id: " << _parent->node_id()
              << ", operator id: " << _parent->operator_id() << " merge spill data";
    DCHECK(!is_merging_);
    is_merging_ = true;
    _dependency->Dependency::block();

    RETURN_IF_ERROR(
            ExecEnv::GetInstance()->spill_stream_mgr()->get_async_task_thread_pool()->submit_func(
                    [this, state] {
                        Defer defer {[&]() {
                            LOG(WARNING) << _shared_state << " sort node id: " << _parent->node_id()
                                         << ", operator id: " << _parent->operator_id()
                                         << " merge spill data end: " << status_;
                            _dependency->Dependency::set_ready();
                            {
                                std::unique_lock<std::mutex> lk(merge_spill_lock_);
                                is_merging_ = false;
                                merge_spill_cv_.notify_one();
                            }
                        }};
                        vectorized::Block merge_sorted_block;
                        vectorized::SpillStreamSPtr tmp_stream;
                        while (true) {
                            int max_stream_count = _calc_spill_blocks_to_merge();
                            LOG(WARNING) << _shared_state << "sort node id: " << _parent->id()
                                         << ", operator id: " << _parent->operator_id()
                                         << " spill sort merge intermediate streams, stream count: "
                                         << _shared_state->sorted_streams_.size()
                                         << ", create merger stream count: " << max_stream_count;
                            status_ = _create_intermediate_merger(
                                    max_stream_count,
                                    _shared_state->sorter->get_sort_description());
                            RETURN_IF_ERROR(status_);

                            // all the remaining streams can be merged in a run
                            if (_shared_state->sorted_streams_.empty()) {
                                return Status::OK();
                            }

                            {
                                status_ = ExecEnv::GetInstance()
                                                  ->spill_stream_mgr()
                                                  ->register_spill_stream(
                                                          tmp_stream, print_id(state->query_id()),
                                                          "sort", _parent->id(),
                                                          _shared_state->spill_block_batch_size_,
                                                          SortSharedState::
                                                                  SORT_BLOCK_SPILL_BATCH_BYTES,
                                                          profile());
                                RETURN_IF_ERROR(status_);
                                status_ = tmp_stream->prepare_spill();
                                RETURN_IF_ERROR(status_);
                                Defer defer {[&]() { tmp_stream->end_spill(status_); }};
                                _shared_state->sorted_streams_.emplace_back(tmp_stream);

                                bool eos = false;
                                while (!eos && !state->is_cancelled()) {
                                    merge_sorted_block.clear_column_data();
                                    status_ = merger_->get_next(&merge_sorted_block, &eos);
                                    RETURN_IF_ERROR(status_);
                                    status_ = tmp_stream->spill_block(merge_sorted_block, eos);
                                    RETURN_IF_ERROR(status_);
                                }
                            }
                            for (auto& stream : current_merging_streams_) {
                                (void)ExecEnv::GetInstance()
                                        ->spill_stream_mgr()
                                        ->delete_spill_stream(stream);
                            }
                            current_merging_streams_.clear();
                        }
                        DCHECK(false);
                        return Status::OK();
                    }));
    return Status::WaitForIO("Merging spilt agg data");
}

Status SortLocalState::_create_intermediate_merger(
        int num_blocks, const vectorized::SortDescription& sort_description) {
    std::vector<vectorized::BlockSupplier> child_block_suppliers;
    merger_ = std::make_unique<vectorized::VSortedRunMerger>(
            sort_description, _shared_state->spill_block_batch_size_,
            _shared_state->sorter->limit(), _shared_state->sorter->offset(), profile());

    current_merging_streams_.clear();
    for (int i = 0; i < num_blocks && !_shared_state->sorted_streams_.empty(); ++i) {
        auto stream = _shared_state->sorted_streams_.front();
        current_merging_streams_.emplace_back(stream);
        child_block_suppliers.emplace_back(
                std::bind(std::mem_fn(&vectorized::SpillStream::read_current_block_sync),
                          stream.get(), std::placeholders::_1, std::placeholders::_2));

        _shared_state->sorted_streams_.pop_front();
    }
    RETURN_IF_ERROR(merger_->prepare(child_block_suppliers));
    return Status::OK();
}
SortSourceOperatorX::SortSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                                         const DescriptorTbl& descs)
        : OperatorX<SortLocalState>(pool, tnode, operator_id, descs) {}

Status SortSourceOperatorX::get_block(RuntimeState* state, vectorized::Block* block,
                                      SourceState& source_state) {
    auto& local_state = get_local_state(state);
    RETURN_IF_ERROR(local_state._shared_state->sink_status_);
    RETURN_IF_ERROR(local_state.status_);
    SCOPED_TIMER(local_state.exec_time_counter());
    bool eos = false;
    if (local_state._shared_state->enable_spill_) {
        if (!local_state.merger_) {
            return local_state.initiate_merge_sort_spill_streams(state);
        } else {
            RETURN_IF_ERROR(local_state.merger_->get_next(block, &eos));
        }
    } else {
        RETURN_IF_ERROR_OR_CATCH_EXCEPTION(
                local_state._shared_state->sorter->get_next(state, block, &eos));
    }
    if (eos) {
        source_state = SourceState::FINISHED;
    }
    local_state.reached_limit(block, source_state);
    return Status::OK();
}

} // namespace doris::pipeline
