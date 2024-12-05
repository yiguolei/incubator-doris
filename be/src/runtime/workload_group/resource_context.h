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

class WorkloadGroupController {
public:
    WorkloadGroupController() = default;
    virtual ~WorkloadGroupController() = default;

    virtual Status bind_workload_group() = 0;
    virtual WorkloadGroupPtr workload_group() = 0;
};

class IOController {
    class IOStats {};
};

class CPUController {
    class CPUStats {};
};

class NetworkController {
    class NetStats {};
};

// Every task should have its own resource context. And BE may adjust the resource
// context during running.
// ResourceContext will bind to the running thread's thread local when the task is running.
class ResourceContext : public std::enable_shared_from_this<ResourceContext> {
    ENABLE_FACTORY_CREATOR(ResourceContext);

public:
    ResourceContext() = default;
    virtual ~ResourceContext() = default;

private:
    // The controller's init value is nullptr, it means the resource context will ignore this controller.
    std::shared_ptr<WorkloadGroupController> _workload_group_controller = nullptr;
    std::shared_ptr<MemoryController> _memory_controller = nullptr;
    std::shared_ptr<WorkloadGroupController> _workload_group_controller = nullptr;
    std::shared_ptr<IOController> _io_controller = nullptr;
};

} // namespace doris
