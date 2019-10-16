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

#ifndef DORIS_BE_SRC_OLAP_TABLET_SYNC_SERVICE_H
#define DORIS_BE_SRC_OLAP_TABLET_SYNC_SERVICE_H

#include <future>

#include "gen_cpp/MetaStoreService_types.h"

#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/tablet.h"
#include "util/batch_process_thread_pool.hpp"

#define FETCH_DATA true
#define NOT_FETCH_DATA false

namespace doris {

enum MetaOpType {
    PUSH_META,
    DELETE_META
};

struct GetTabletMetaRespPB {
public:
    OLAPStatus status;
    TabletMetaPB tablet_meta_pb;
    vector<RowsetMetaPB> rowset_meta_pbs;
    int64_t modify_version;
};

struct FetchRowsetMetaTask {
public:
    int priority;
    GetRowsetMetaReq get_rowset_meta_req;
    std::function<void(const RowsetMetaPB&)> after_fetch_callback;
    bool load_data;
    std::shared_ptr<std::promise<RowsetMetaPB>> pro;
    bool operator< (const FetchRowsetMetaTask& o) const {
        return priority < o.priority;
    }

    FetchRowsetMetaTask& operator++() {
        priority += 2;
        return *this;
    }
}; // FetchRowsetMetaTask

struct FetchTabletMetaTask {
public:
    int priority;
    TabletSharedPtr tablet;
    bool load_data;
    std::shared_ptr<std::promise<GetTabletMetaRespPB>> pro;
    bool operator< (const FetchTabletMetaTask& o) const {
        return priority < o.priority;
    }

    FetchTabletMetaTask& operator++() {
        priority += 2;
        return *this;
    }
}; // FetchTabletMetaTask

struct PushTabletMetaTask {
public:
    int priority;
    SaveTabletMetaReq save_tablet_meta_req;
    std::shared_ptr<std::promise<OLAPStatus>> pro;
    bool operator< (const PushTabletMetaTask& o) const {
        return priority < o.priority;
    }

    PushTabletMetaTask& operator++() {
        priority += 2;
        return *this;
    }
}; // PushTabletMetaTask


// sync meta and data from remote meta store to local meta store
// all method should consider dedup duplicate calls
// for example, thread1 call sync meta and thread2 call sync meta, if they are the same
// should not sync twice
class TabletSyncService {

public:
    TabletSyncService();
    ~TabletSyncService();
    // fetch rowset meta and data to local metastore
    // when add a task, should check if the task already exist
    // if the rowset meta is not published, should commit it to local meta store
    // if it is already visible, then just add it to tablet
    // tablet_id + txn_id could find a unique rowset
    // return a future object, caller could using it to wait the task to finished
    // and check the status
    std::future<RowsetMetaPB> fetch_rowset(const TabletSharedPtr& tablet, int64_t txn_id, bool load_data, 
        std::function<void(const RowsetMetaPB&)> after_callback);

    // fetch rowset meta and data using version
    std::future<RowsetMetaPB> fetch_rowset(const TabletSharedPtr& tablet, const Version& version, bool load_data, 
        std::function<void(const RowsetMetaPB&)> after_callback);

    // save the rowset meta pb to remote meta store
    // !!!! the caller should not own tablet map lock or tablet lock because 
    // this method will call tablet manager to get tablet info
    std::future<OLAPStatus> push_rowset_meta(const RowsetMetaPB& rowset_meta, int64_t expected_version, int64_t new_version);

    // add a synchronized method full push 
    OLAPStatus sync_push_rowset_meta(const RowsetMetaPB& rowset_meta, int64_t expected_version, int64_t new_version);

    std::future<OLAPStatus> delete_rowset_meta(const RowsetId& rowset_id, int64_t expected_version, int64_t new_version);

    // add a synchronized method full push 
    OLAPStatus sync_delete_rowset_meta(const RowsetId& rowset_id, int64_t expected_version, int64_t new_version);

    // fetch both tablet meta and all rowset meta
    // when create a tablet, if it's eco_mode and term > 1 then should fetch
    // all rowset and tablet meta from remote meta store
    // Maybe, it's better to add a callback function here
    std::future<GetTabletMetaRespPB> fetch_tablet_meta(const TabletSharedPtr& tablet, bool load_data);

    // a synchronized method to fetch tablet meta from meta store
    OLAPStatus sync_fetch_tablet_meta(const TabletSharedPtr& tablet, bool load_data, GetTabletMetaRespPB* result);

    // save the tablet meta pb to remote meta store
    std::future<OLAPStatus> push_tablet_meta(const TabletMetaPB& tablet_meta, int64_t expected_version, int64_t new_version);

    // add a synchronized method full push 
    OLAPStatus sync_push_tablet_meta(const TabletMetaPB& tablet_meta, int64_t expected_version, int64_t new_version);

private:
    void _fetch_rowset_meta_thread(std::vector<FetchRowsetMetaTask> tasks); 
    void _fetch_tablet_meta_thread(std::vector<FetchTabletMetaTask> tasks); 
    void _push_tablet_meta_thread(std::vector<PushTabletMetaTask> tasks); 

    void _convert_to_save_rowset_req(const RowsetMetaPB& rowset_meta_pb, 
        SaveRowsetMetaReq* save_rowset_meta_req);
    void _convert_to_get_rowset_req(const TabletSharedPtr& tablet, int64_t txn_id, 
        int64_t start_version, int64_t end_version, GetRowsetMetaReq* get_rowset_meta_req);

private:
    BatchProcessThreadPool<FetchRowsetMetaTask>* _fetch_rowset_pool = nullptr;
    BatchProcessThreadPool<FetchTabletMetaTask>* _fetch_tablet_pool = nullptr;
    BatchProcessThreadPool<PushTabletMetaTask>* _push_tablet_pool = nullptr;
    ExecEnv* _env;
}; // TabletSyncService



} // doris
#endif // DORIS_BE_SRC_OLAP_TABLET_SYNC_SERVICE_H