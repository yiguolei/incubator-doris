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

#include <deque>
#include <memory>
#include <vector>

#include "common/status.h"
#include "operator.h"
#include "vec/spill/spill_repartitioner.h"
#include "vec/spill/spill_stream.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;

namespace pipeline {

class PartitionedAggSourceOperatorX;
class PartitionedAggLocalState;

/// Represents one partition in the multi-level spill queue for aggregation.
/// Unlike Join (which has build + probe), Agg only has a single data flow:
/// spilled aggregation intermediate results stored in one or more SpillStreams.
struct AggSpillPartitionInfo {
    // All spill streams for this partition (may come from multiple spill rounds).
    std::deque<vectorized::SpillStreamSPtr> streams;
    // The depth level in the repartition tree (level-0 = original).
    int level = 0;

    AggSpillPartitionInfo() = default;
    AggSpillPartitionInfo(std::deque<vectorized::SpillStreamSPtr> s, int lvl)
            : streams(std::move(s)), level(lvl) {}

    bool has_data() const {
        for (auto& stream : streams) {
            if (stream && stream->get_written_bytes() > 0) return true;
        }
        return false;
    }

    int64_t total_bytes() const {
        int64_t total = 0;
        for (auto& stream : streams) {
            if (stream) total += stream->get_written_bytes();
        }
        return total;
    }
};

class PartitionedAggLocalState MOCK_REMOVE(final)
        : public PipelineXSpillLocalState<PartitionedAggSharedState> {
public:
    ENABLE_FACTORY_CREATOR(PartitionedAggLocalState);
    using Base = PipelineXSpillLocalState<PartitionedAggSharedState>;
    using Parent = PartitionedAggSourceOperatorX;
    PartitionedAggLocalState(RuntimeState* state, OperatorXBase* parent);
    ~PartitionedAggLocalState() override = default;

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;

    Status recover_blocks_from_disk(RuntimeState* state, bool& has_data);

    /// Recover blocks from a queue partition's streams (reads up to 32MB then returns).
    Status recover_blocks_from_queue_partition(RuntimeState* state,
                                               AggSpillPartitionInfo& partition, bool& has_data);

    /// Repartition a queue partition's streams into FANOUT sub-partitions and push to queue.
    /// If the hash table has partial data, it is drained via get_block() and routed
    /// through the repartitioner as well (single data flow, not two streams).
    Status repartition_agg_partition(RuntimeState* state, AggSpillPartitionInfo& partition);

    /// Flush the current in-memory hash table by draining it as blocks and routing
    /// each block through the repartitioner into the output sub-streams.
    Status flush_hash_table_to_sub_streams(
            RuntimeState* state, std::vector<vectorized::SpillStreamSPtr>& output_streams);

    Status setup_in_memory_agg_op(RuntimeState* state);

    template <bool spilled>
    void update_profile(RuntimeProfile* child_profile);

    bool is_blockable() const override;

private:
    Status _recover_spill_data_from_disk(RuntimeState* state, const UniqueId& query_id);

protected:
    friend class PartitionedAggSourceOperatorX;
    std::unique_ptr<RuntimeState> _runtime_state;

    bool _opened = false;
    std::unique_ptr<std::promise<Status>> _spill_merge_promise;
    std::future<Status> _spill_merge_future;
    bool _current_partition_eos = true;
    bool _need_to_merge_data_for_current_partition = true;

    std::vector<vectorized::Block> _blocks;

    std::unique_ptr<RuntimeProfile> _internal_runtime_profile;

    // Multi-level spill repartition support
    std::deque<AggSpillPartitionInfo> _spill_partition_queue;
    AggSpillPartitionInfo _current_queue_partition;
    SpillRepartitioner _repartitioner;
    // True when processing entries from _spill_partition_queue.
    bool _processing_spill_queue {false};
    // True when the current queue partition needs initial setup.
    bool _need_to_setup_queue_partition {true};
    // Whether the current queue partition has finished reading all streams.
    bool _current_queue_partition_eos {true};
    // Whether the current queue partition still needs to merge data.
    bool _need_to_merge_for_queue_partition {false};
};

class AggSourceOperatorX;
class PartitionedAggSourceOperatorX : public OperatorX<PartitionedAggLocalState> {
public:
    using Base = OperatorX<PartitionedAggLocalState>;
    PartitionedAggSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                                  const DescriptorTbl& descs);
    ~PartitionedAggSourceOperatorX() override = default;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override;

    bool is_source() const override { return true; }

    bool is_serial_operator() const override;
    void update_operator(const TPlanNode& tnode, bool followed_by_shuffled_operator,
                         bool require_bucket_distribution) override;

    DataDistribution required_data_distribution(RuntimeState* state) const override;
    bool is_colocated_operator() const override;
    bool is_shuffled_operator() const override;

private:
    friend class PartitionedAggLocalState;

    /// Process entries from the multi-level _spill_partition_queue.
    Status _pull_from_spill_queue(PartitionedAggLocalState& local_state, RuntimeState* state,
                                  vectorized::Block* block, bool* eos);

    std::unique_ptr<AggSourceOperatorX> _agg_source_operator;
};
} // namespace pipeline
#include "common/compile_check_end.h"
} // namespace doris
