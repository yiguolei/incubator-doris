

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

#include "runtime/workload_group/memory_controller.h"

#include <map>
#include <mutex>
#include <ostream>
#include <utility>

#include "common/logging.h"

namespace doris {

void MemoryController::Stats::reset() {
    revoke_attempts_ = 0;
    revoke_wait_time_ms_ = 0;
    revoked_bytes_ = 0;
}

MemoryController::Stats& MemoryController::Stats::operator+=(const MemoryController::Stats& other) {
    revoke_attempts_ += other.revoke_attempts_;
    revoke_wait_time_ms_ += other.revoke_wait_time_ms_;
    revoked_bytes_ += other.revoked_bytes_;
    return *this;
}

MemoryController::Stats MemoryController::Stats::operator-(const Stats& other) const {
    Stats result;
    result.revoke_attempts_ = revoke_attempts_ - other.revoke_attempts_;
    result.revoke_wait_time_ms_ = revoke_wait_time_ms_ - other.revoke_wait_time_ms_;
    result.revoked_bytes_ = revoked_bytes_ - other.revoked_bytes_;
    return result;
}

std::string MemoryController::Stats::debug_string() const {
    return fmt::format("revoke_attempts_ {} revoke_wait_time_ms_ {} revoked_bytes_ {}",
                       revoke_attempts_, revoke_wait_time_ms_,
                       PrettyPrinter::print(revoked_bytes_, TUnit::BYTES));
}

} // namespace doris