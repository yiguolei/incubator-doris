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

#include "vec/spill/spill_stream_manager.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <algorithm>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <numeric>
#include <random>
#include <string>

#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "olap/data_dir.h"
#include "olap/olap_define.h"
#include "olap/storage_engine.h"
#include "util/runtime_profile.h"
#include "util/time.h"
#include "vec/spill/spill_stream.h"

namespace doris {

namespace vectorized {

SpillStreamManager::SpillStreamManager(const std::vector<StorePath>& paths) : _store_paths(paths) {}

Status SpillStreamManager::init() {
    int spill_io_thread_count = config::spill_io_thread_pool_per_disk_thread_num;
    if (spill_io_thread_count <= 0) {
        spill_io_thread_count = 2;
    }
    int pool_idx = 0;
    for (const auto& path : _store_paths) {
        auto gc_dir_root_dir = fmt::format("{}/{}", path.path, SPILL_GC_DIR_PREFIX);
        bool exists = true;
        RETURN_IF_ERROR(io::global_local_filesystem()->exists(gc_dir_root_dir, &exists));
        if (!exists) {
            RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(gc_dir_root_dir));
        }

        auto spill_dir = fmt::format("{}/{}", path.path, SPILL_DIR_PREFIX);
        RETURN_IF_ERROR(io::global_local_filesystem()->exists(spill_dir, &exists));
        if (!exists) {
            RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(spill_dir));
        } else {
            auto suffix = ToStringFromUnixMillis(UnixMillis());
            auto gc_dir = fmt::format("{}/{}/{}", path.path, SPILL_GC_DIR_PREFIX, suffix);
            if (std::filesystem::exists(gc_dir)) {
                LOG(WARNING) << "gc dir already exists: " << gc_dir;
            }
            (void)io::global_local_filesystem()->rename(spill_dir, gc_dir);
            RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(spill_dir));
        }

        size_t spill_data_size = 0;
        for (auto const& dir_entry :
             std::filesystem::recursive_directory_iterator {gc_dir_root_dir}) {
            if (dir_entry.is_regular_file()) {
                spill_data_size += dir_entry.file_size();
            }
        }
        path_to_spill_data_size_[path.path] = spill_data_size;

        std::unique_ptr<ThreadPool> io_pool;
        static_cast<void>(ThreadPoolBuilder(fmt::format("SpillIOThreadPool-{}", pool_idx++))
                                  .set_min_threads(spill_io_thread_count)
                                  .set_max_threads(spill_io_thread_count)
                                  .set_max_queue_size(config::spill_io_thread_pool_queue_size)
                                  .build(&io_pool));
        path_to_io_thread_pool_[path.path] = std::move(io_pool);
    }
    static_cast<void>(ThreadPoolBuilder("SpillAsyncTaskThreadPool")
                              .set_min_threads(config::spill_async_task_thread_pool_thread_num)
                              .set_max_threads(config::spill_async_task_thread_pool_thread_num)
                              .set_max_queue_size(config::spill_async_task_thread_pool_queue_size)
                              .build(&async_task_thread_pool_));

    return Status::OK();
}

Status SpillStreamManager::register_spill_stream(SpillStreamSPtr& spill_stream,
                                                 std::string query_id, std::string operator_name,
                                                 int32_t operator_id, /* int32_t task_id,*/
                                                 int32_t batch_rows, size_t batch_bytes,
                                                 RuntimeProfile* profile) {
    auto data_dirs = ExecEnv::GetInstance()->storage_engine().to_local().get_stores_for_spill(
            TStorageMedium::type::SSD);
    if (data_dirs.empty()) {
        data_dirs = ExecEnv::GetInstance()->storage_engine().to_local().get_stores_for_spill(
                TStorageMedium::type::HDD);
    }
    if (data_dirs.empty()) {
        return Status::Error<ErrorCode::NO_AVAILABLE_ROOT_PATH>(
                "no available disk can be used for spill.");
    }

    int64_t id = id_++;
    std::string spill_dir;
    doris::DataDir* data_dir = nullptr;
    for (auto& dir : data_dirs) {
        data_dir = dir;
        std::string spill_root_dir = fmt::format("{}/{}", data_dir->path(), SPILL_DIR_PREFIX);
        // std::string unique_name = boost::uuids::to_string(boost::uuids::random_generator()());
        spill_dir = fmt::format("{}/{}-{}-{}-{}", spill_root_dir, query_id, operator_name,
                                operator_id, id);
        auto st = io::global_local_filesystem()->create_directory(spill_dir);
        if (!st.ok()) {
            continue;
        }
        break;
    }
    if (!data_dir) {
        return Status::Error<ErrorCode::CE_CMD_PARAMS_ERROR>(
                "there is no available disk that can be used to spill.");
    }
    spill_stream = std::make_shared<SpillStream>(id, data_dir, spill_dir, batch_rows, batch_bytes,
                                                 profile);
    RETURN_IF_ERROR(spill_stream->prepare());
    // {
    //     std::lock_guard<std::mutex> l(lock_);
    //     id_to_spill_streams_[id] = spill_stream;
    // }
    return Status::OK();
}

void SpillStreamManager::delete_spill_stream(SpillStreamSPtr stream) {
    stream->close();

    auto gc_dir = fmt::format("{}/{}/{}", stream->get_data_dir()->path(), SPILL_GC_DIR_PREFIX,
                              std::filesystem::path(stream->get_spill_dir()).filename().string());
    (void)io::global_local_filesystem()->rename(stream->get_spill_dir(), gc_dir);
    // std::lock_guard<std::mutex> l(lock_);
    // id_to_spill_streams_.erase(stream->id());
}

void SpillStreamManager::gc(int64_t max_file_count) {
    if (max_file_count < 1) {
        return;
    }

    bool exists = true;
    int64_t count = 0;
    for (const auto& path : _store_paths) {
        std::string gc_root_dir = fmt::format("{}/{}", path.path, SPILL_GC_DIR_PREFIX);

        std::error_code ec;
        exists = std::filesystem::exists(gc_root_dir, ec);
        if (ec || !exists) {
            continue;
        }
        std::vector<io::FileInfo> dirs;
        auto st = io::global_local_filesystem()->list(gc_root_dir, false, &dirs, &exists);
        if (!st.ok()) {
            continue;
        }

        for (const auto& dir : dirs) {
            if (dir.is_file) {
                continue;
            }
            std::string abs_dir = fmt::format("{}/{}", gc_root_dir, dir.file_name);
            std::vector<io::FileInfo> files;
            st = io::global_local_filesystem()->list(abs_dir, true, &files, &exists);
            if (!st.ok()) {
                continue;
            }
            if (files.empty()) {
                static_cast<void>(io::global_local_filesystem()->delete_directory(abs_dir));
                if (count++ == max_file_count) {
                    return;
                }
                continue;
            }

            int64_t data_size = 0;
            Defer defer {[&]() { update_usage(path.path, -data_size); }};

            for (const auto& file : files) {
                auto abs_file_path = fmt::format("{}/{}", abs_dir, file.file_name);
                data_size += file.file_size;
                static_cast<void>(io::global_local_filesystem()->delete_file(abs_file_path));
                if (count++ == max_file_count) {
                    return;
                }
            }
        }
    }
}
} // namespace vectorized
} // namespace doris
