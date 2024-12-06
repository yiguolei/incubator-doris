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

// Any task that allow cancel should implement this class.
class ResourceReclaimer {
public:
    virtual Status cancel(Status cancel_reason) { return Status::OK(); }
};

// Every task should have its own resource context. And BE may adjust the resource
// context during running.
// Workload group will hold the resource context and do some control work.
class ResourceContext : public std::enable_shared_from_this<ResourceContext> {
    ENABLE_FACTORY_CREATOR(ResourceContext);

public:
    ResourceContext() = default;
    virtual ~ResourceContext() = default;

    void set_workload_group() {
        // update all child context's workload group property
    }

private:
    // The controller's init value is nullptr, it means the resource context will ignore this controller.
    std::shared_ptr<CPUContext> _cpu_context = nullptr;
    std::shared_ptr<MemoryContext> _memory_context = nullptr;
    std::shared_ptr<IOContext> _io_context = nullptr;
    std::shared_ptr<ResourceReclaimer> _reclaimer = nullptr;
};

} // namespace doris
