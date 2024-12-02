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

#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <memory>
#include <queue>
#include <shared_mutex>
#include <string>

#include "common/status.h"

namespace doris {

class MemoryController : public std::enable_shared_from_this<MemoryController> {
    ENABLE_FACTORY_CREATOR(MemoryController);

public:
    // Used to collect memory execution stats.
    class Stats {
    public:
        Stats() = default;
        virtual ~Stats() = default;

        void reset();
        std::string debug_string();
        int64_t revoke_attempts() { return revoke_attempts_; }
        int64_t revoke_wait_time_ms() { return revoke_wait_time_ms_; }
        int64_t revoked_bytes() { return revoked_bytes_; }

        void incr_revoke_attempts(int64_t delta) { revoke_attempts_ + delta; }
        void incr_revoke_wait_time_ms(int64_t delta) { return revoke_wait_time_ms_ + delta; }
        void incr_revoked_bytes(int64_t delta) { return revoked_bytes_ + delta; }

    private:
        // The total number of times that the revoke method is called.
        std::atomic<int64_t> revoke_attempts_ = 0;

        // The time that waiting for revoke finished.
        std::atomic<int64_t> revoke_wait_time_ms_ = 0;

        // The revoked bytes
        std::atomic<int64_t> revoked_bytes_ = 0;
    };

public:
    MemoryController(std::shared_ptr<MemtrackerLimiter> memtracker)
            : memtracker_limiter_(memtracker) {}

    virtual ~MemoryController() = default;

    // Compute the number of bytes could be released.
    virtual int64_t revokable_bytes() { return 0; }

    virtual bool ready_do_revoke() { return true; }

    // Begin to do revoke memory task.
    virtual Status revoke(int64_t bytes) { return Status::OK(); }

    virtual Status enter_arbitration(Status reason) { return Status::OK(); }

    virtual Status leave_arbitration(Status reason) { return Status::OK(); }

    // Return related workload group if exists, maybe return null if not bind
    // to a workload group.
    virtual WorkloadGroupPtr workload_group();

    // Cancel the related task
    virtual Status cancel(Status cancel_reason) { return Status::OK(); }

private:
    // std::weak_ptr<WorkloadGroup> workload_group_;
    std::weak_ptr<MemtrackerLimiter> memtracker_limiter_;
};

} // namespace doris
