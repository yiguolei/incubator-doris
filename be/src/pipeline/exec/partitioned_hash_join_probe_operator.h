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

#include "common/be_mock_util.h"
#include "common/status.h"
#include "operator.h"
#include "pipeline/dependency.h"
#include "pipeline/exec/hashjoin_build_sink.h"
#include "pipeline/exec/hashjoin_probe_operator.h"
#include "pipeline/exec/join_build_sink_operator.h"
#include "pipeline/exec/spill_utils.h"
#include "vec/spill/spill_repartitioner.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;

namespace pipeline {

class PartitionedHashJoinProbeOperatorX;

/// Represents a spilled partition pair (build + probe streams) that needs to be processed
/// during recovery. For multi-level spill, when a partition is too large to fit in memory,
/// it gets repartitioned into FANOUT sub-partitions, each represented by a new
/// SpillPartitionInfo at level + 1.
struct SpillPartitionInfo {
    vectorized::SpillStreamSPtr build_stream;
    vectorized::SpillStreamSPtr probe_stream;
    int level = 0; // 0 = original level-0 partition, 1+ = repartitioned sub-partition

    SpillPartitionInfo() = default;
    SpillPartitionInfo(vectorized::SpillStreamSPtr build, vectorized::SpillStreamSPtr probe,
                       int lvl)
            : build_stream(std::move(build)), probe_stream(std::move(probe)), level(lvl) {}

    bool has_build_data() const { return build_stream && build_stream->get_written_bytes() > 0; }

    bool has_probe_data() const { return probe_stream && probe_stream->get_written_bytes() > 0; }
};

class PartitionedHashJoinProbeLocalState MOCK_REMOVE(final)
        : public PipelineXSpillLocalState<PartitionedHashJoinSharedState> {
public:
    using Parent = PartitionedHashJoinProbeOperatorX;
    ENABLE_FACTORY_CREATOR(PartitionedHashJoinProbeLocalState);
    PartitionedHashJoinProbeLocalState(RuntimeState* state, OperatorXBase* parent);
    ~PartitionedHashJoinProbeLocalState() override = default;

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;

    Status spill_probe_blocks(RuntimeState* state);

    Status recover_build_blocks_from_disk(RuntimeState* state, uint32_t partition_index,
                                          bool& has_data);
    Status recover_probe_blocks_from_disk(RuntimeState* state, uint32_t partition_index,
                                          bool& has_data);

    /// Recover build blocks from a SpillPartitionInfo's build stream (for multi-level recovery).
    Status recover_build_blocks_from_partition(RuntimeState* state,
                                               SpillPartitionInfo& partition_info, bool& has_data);
    /// Recover probe blocks from a SpillPartitionInfo's probe stream (for multi-level recovery).
    Status recover_probe_blocks_from_partition(RuntimeState* state,
                                               SpillPartitionInfo& partition_info, bool& has_data);

    /// Repartition the current partition's build and probe streams into FANOUT sub-partitions
    /// and push them into _spill_partition_queue for subsequent processing.
    Status repartition_current_partition(RuntimeState* state, SpillPartitionInfo& partition);

    /// Repartition a level-0 partition when its build data is already loaded in memory
    /// but the hash table build failed due to OOM.
    /// @param partition_index The level-0 partition index
    /// @param build_block     The in-memory build data (from partitioned_build_blocks)
    /// @param probe_stream    The probe data on disk (from probe_spilling_streams)
    /// @param probe_blocks    Any in-memory probe blocks
    Status repartition_level0_partition(RuntimeState* state, uint32_t partition_index);

    Status finish_spilling(uint32_t partition_index);

    template <bool spilled>
    void update_build_custom_profile(RuntimeProfile* child_profile);

    template <bool spilled>
    void update_probe_custom_profile(RuntimeProfile* child_profile);

    template <bool spilled>
    void update_build_common_profile(RuntimeProfile* child_profile);

    template <bool spilled>
    void update_probe_common_profile(RuntimeProfile* child_profile);

    std::string debug_string(int indentation_level = 0) const override;

    MOCK_FUNCTION void update_profile_from_inner();

    void init_counters();

    bool is_blockable() const override;

    friend class PartitionedHashJoinProbeOperatorX;

private:
    template <typename LocalStateType>
    friend class StatefulOperatorX;

    // Spill probe blocks to disk
    Status _execute_spill_probe_blocks(RuntimeState* state, const UniqueId& query_id);

    std::shared_ptr<BasicSharedState> _in_mem_shared_state_sptr;
    uint32_t _partition_cursor {0};

    std::unique_ptr<vectorized::Block> _child_block;
    bool _child_eos {false};

    std::vector<std::unique_ptr<vectorized::MutableBlock>> _partitioned_blocks;
    std::unique_ptr<vectorized::MutableBlock> _recovered_build_block;
    std::map<uint32_t, std::vector<vectorized::Block>> _probe_blocks;

    std::vector<vectorized::SpillStreamSPtr> _probe_spilling_streams;

    std::unique_ptr<vectorized::PartitionerBase> _partitioner;
    std::unique_ptr<RuntimeProfile> _internal_runtime_profile;

    bool _need_to_setup_internal_operators {true};

    // ---- Multi-level spill partition state ----
    // Work queue of spilled partition pairs to process. Initially populated from
    // level-0 partitions during the first recovery. When a partition is too large
    // to build a hash table, it is repartitioned and FANOUT new entries are pushed.
    std::deque<SpillPartitionInfo> _spill_partition_queue;
    // The partition currently being processed from _spill_partition_queue.
    SpillPartitionInfo _current_partition;
    // Repartitioner instance (reused across repartition calls)
    SpillRepartitioner _repartitioner;
    // A partitioner with partition_count = FANOUT for use during repartitioning.
    // The main _partitioner uses the original _partition_count (e.g., 32), which
    // is wrong for repartitioning that needs FANOUT (8) sub-partitions.
    std::unique_ptr<vectorized::PartitionerBase> _fanout_partitioner;
    // Whether we're currently processing the multi-level spill partition queue
    // (as opposed to the initial level-0 partitions via _partition_cursor).
    bool _processing_spill_queue {false};
    // Whether internal operators need to be set up for the current queue partition.
    bool _need_to_setup_queue_partition {true};
    // Probe blocks recovered from the current queue partition's probe stream.
    std::vector<vectorized::Block> _queue_probe_blocks;

    RuntimeProfile::Counter* _partition_timer = nullptr;
    RuntimeProfile::Counter* _partition_shuffle_timer = nullptr;
    RuntimeProfile::Counter* _spill_build_rows = nullptr;
    RuntimeProfile::Counter* _spill_build_blocks = nullptr;
    RuntimeProfile::Counter* _spill_build_timer = nullptr;
    RuntimeProfile::Counter* _recovery_build_rows = nullptr;
    RuntimeProfile::Counter* _recovery_build_blocks = nullptr;
    RuntimeProfile::Counter* _recovery_build_timer = nullptr;
    RuntimeProfile::Counter* _spill_probe_rows = nullptr;
    RuntimeProfile::Counter* _spill_probe_blocks = nullptr;
    RuntimeProfile::Counter* _spill_probe_timer = nullptr;
    RuntimeProfile::Counter* _recovery_probe_rows = nullptr;
    RuntimeProfile::Counter* _recovery_probe_blocks = nullptr;
    RuntimeProfile::Counter* _recovery_probe_timer = nullptr;

    RuntimeProfile::Counter* _probe_blocks_bytes = nullptr;
    RuntimeProfile::Counter* _memory_usage_reserved = nullptr;
    RuntimeProfile::Counter* _get_child_next_timer = nullptr;
};

class PartitionedHashJoinProbeOperatorX final
        : public JoinProbeOperatorX<PartitionedHashJoinProbeLocalState> {
public:
    PartitionedHashJoinProbeOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                                      const DescriptorTbl& descs, uint32_t partition_count);
    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;

    [[nodiscard]] Status get_block(RuntimeState* state, vectorized::Block* block,
                                   bool* eos) override;

    Status push(RuntimeState* state, vectorized::Block* input_block, bool eos) const override;
    Status pull(doris::RuntimeState* state, vectorized::Block* output_block,
                bool* eos) const override;

    bool need_more_input_data(RuntimeState* state) const override;
    DataDistribution required_data_distribution(RuntimeState* /*state*/) const override {
        if (_join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
            return {ExchangeType::NOOP};
        }
        return (_join_distribution == TJoinDistributionType::BUCKET_SHUFFLE ||
                                _join_distribution == TJoinDistributionType::COLOCATE
                        ? DataDistribution(ExchangeType::BUCKET_HASH_SHUFFLE,
                                           _distribution_partition_exprs)
                        : DataDistribution(ExchangeType::HASH_SHUFFLE,
                                           _distribution_partition_exprs));
    }

    size_t revocable_mem_size(RuntimeState* state) const override;

    size_t get_reserve_mem_size(RuntimeState* state) override;

    void set_inner_operators(const std::shared_ptr<HashJoinBuildSinkOperatorX>& sink_operator,
                             const std::shared_ptr<HashJoinProbeOperatorX>& probe_operator) {
        _inner_sink_operator = sink_operator;
        _inner_probe_operator = probe_operator;
    }
    bool is_shuffled_operator() const override {
        return _inner_probe_operator->is_shuffled_operator();
    }
    bool is_colocated_operator() const override {
        return _inner_probe_operator->is_colocated_operator();
    }
    bool followed_by_shuffled_operator() const override {
        return _inner_probe_operator->followed_by_shuffled_operator();
    }

    void update_operator(const TPlanNode& tnode, bool followed_by_shuffled_operator,
                         bool require_bucket_distribution) override {
        _inner_probe_operator->update_operator(tnode, followed_by_shuffled_operator,
                                               require_bucket_distribution);
    }

private:
    Status _revoke_memory(RuntimeState* state);

    size_t _revocable_mem_size(RuntimeState* state, bool force = false) const;

    friend class PartitionedHashJoinProbeLocalState;

    [[nodiscard]] Status _setup_internal_operators(PartitionedHashJoinProbeLocalState& local_state,
                                                   RuntimeState* state) const;

    /// Setup internal operators using build data from a SpillPartitionInfo
    /// (for multi-level recovery, where build data comes from repartitioned streams).
    [[nodiscard]] Status _setup_internal_operators_from_partition(
            PartitionedHashJoinProbeLocalState& local_state, RuntimeState* state) const;

    /// Process entries from the multi-level _spill_partition_queue.
    /// Called from pull() when all level-0 partitions have been exhausted.
    [[nodiscard]] Status _pull_from_spill_queue(PartitionedHashJoinProbeLocalState& local_state,
                                                RuntimeState* state,
                                                vectorized::Block* output_block, bool* eos) const;

    bool _should_revoke_memory(RuntimeState* state) const;

    const TJoinDistributionType::type _join_distribution;

    std::shared_ptr<HashJoinBuildSinkOperatorX> _inner_sink_operator;
    std::shared_ptr<HashJoinProbeOperatorX> _inner_probe_operator;

    // probe expr
    std::vector<TExpr> _probe_exprs;

    const std::vector<TExpr> _distribution_partition_exprs;

    const TPlanNode _tnode;
    const DescriptorTbl _descriptor_tbl;

    const uint32_t _partition_count;
    std::unique_ptr<vectorized::PartitionerBase> _partitioner;
};

} // namespace pipeline
#include "common/compile_check_end.h"
} // namespace doris