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

#include "olap/tablet_sync_service.h"

using namespace std;

namespace doris {

TabletSyncService::TabletSyncService() {
    // TODO(ygl): add new config
    _fetch_rowset_pool = new BatchProcessThreadPool<FetchRowsetMetaTask>(
        3,  // thread num
        10000,  // queue size
        10, // batch size
        std::bind<void>(std::mem_fn(&TabletSyncService::_fetch_rowset_meta_thread), this, std::placeholders::_1));

    _push_rowset_pool = new BatchProcessThreadPool<PushRowsetMetaTask>(
        3,  // thread num
        10000,  // queue size
        10, // batch size
        std::bind<void>(std::mem_fn(&TabletSyncService::_push_rowset_meta_thread), this, std::placeholders::_1));
    _fetch_tablet_pool = new BatchProcessThreadPool<FetchTabletMetaTask>(
        3,  // thread num
        10000,  // queue size
        10, // batch size
        std::bind<void>(std::mem_fn(&TabletSyncService::_fetch_tablet_meta_thread), this, std::placeholders::_1));
    _push_tablet_pool = new BatchProcessThreadPool<PushTabletMetaTask>(
        3,  // thread num
        10000,  // queue size
        10, // batch size
        std::bind<void>(std::mem_fn(&TabletSyncService::_push_tablet_meta_thread), this, std::placeholders::_1));
}


TabletSyncService::~TabletSyncService() {
    if (_fetch_rowset_pool != nullptr) {
        delete _fetch_rowset_pool;
    }
    if (_push_rowset_pool != nullptr) {
        delete _push_rowset_pool;
    }
    if (_fetch_tablet_pool != nullptr) {
        delete _fetch_tablet_pool;
    }
    if (_push_tablet_pool != nullptr) {
        delete _push_tablet_pool;
    }
}

// fetch rowset meta and data to local metastore
// when add a task, should check if the task already exist
// if the rowset meta is not published, should commit it to local meta store
// if it is already visible, then just add it to tablet
// tablet_id + txn_id could find a unique rowset
// return a future object, caller could using it to wait the task to finished
// and check the status
std::future<OLAPStatus> TabletSyncService::fetch_rowset(const TabletSharedPtr& tablet, int64_t txn_id, bool load_data, 
    std::function<void(const RowsetMetaPB&)> const& after_callback) {
    GetRowsetMetaReq get_rowset_meta_req;
    _convert_to_get_rowset_req(tablet, txn_id, 0, 0, &get_rowset_meta_req);
    auto pro = make_shared<promise<OLAPStatus>>();
    FetchRowsetMetaTask fetch_task;
    fetch_task.get_rowset_meta_req = get_rowset_meta_req;
    fetch_task.load_data = load_data;
    fetch_task.pro = pro;
    _fetch_rowset_pool->offer(fetch_task);
    return pro->get_future();
}

// fetch rowset meta and data using version
std::future<OLAPStatus> TabletSyncService::fetch_rowset(const TabletSharedPtr& tablet, const Version& version, bool load_data, 
    std::function<void(const RowsetMetaPB&)> const& after_callback) {
    GetRowsetMetaReq get_rowset_meta_req;
    _convert_to_get_rowset_req(tablet, 0, version.first, version.second, &get_rowset_meta_req);
    auto pro = make_shared<promise<OLAPStatus>>();
    FetchRowsetMetaTask fetch_task;
    fetch_task.get_rowset_meta_req = get_rowset_meta_req;
    fetch_task.load_data = load_data;
    fetch_task.after_fetch_callback = after_callback;
    fetch_task.pro = pro;
    _fetch_rowset_pool->offer(fetch_task);
    return pro->get_future();
}

std::future<OLAPStatus> TabletSyncService::push_rowset_meta(const RowsetMetaPB& rowset_meta_pb, int64_t expected_version, int64_t new_version) {
    CHECK(rowset_meta_pb.has_tablet_id());
    CHECK(rowset_meta_pb.has_tablet_schema_hash());
    SaveRowsetMetaReq save_rowset_meta_req;
    _convert_to_save_rowset_req(rowset_meta_pb, &save_rowset_meta_req);
    SaveTabletMetaReq save_tablet_meta_req;
    vector<SaveTabletMetaReq> save_rowset_meta_reqs;
    save_rowset_meta_reqs.push_back(save_rowset_meta_req);
    save_tablet_meta_req.__set_rowsets_to_save(save_rowset_meta_reqs);
    if (expected_version > 0) {
        save_tablet_meta_req.__set_expected_modify_version(expected_version);
        save_tablet_meta_req.__set_new_modify_version(new_version);
    }
    auto pro = make_shared<promise<OLAPStatus>>();
    PushTabletMetaTask push_task;
    push_task.save_tablet_meta_req = save_tablet_meta_req;
    push_task.pro = pro;
    _push_tablet_pool->offer(push_task);
    return pro->get_future();
}

 // add a synchronized method full push 
OLAPStatus TabletSyncService::sync_push_rowset_meta(const RowsetMetaPB& rowset_meta, int64_t expected_version, int64_t new_version) {
    std::future<OLAPStatus> res_future = push_rowset_meta(rowset_meta, expected_version, new_version);
    return res_future.get();
}

std::future<OLAPStatus> TabletSyncService::delete_rowset_meta(const RowsetId& rowset_id, int64_t expected_version, int64_t new_version) {
    SaveTabletMetaReq save_tablet_meta_req;
    vector<string> rowsets_to_delete;
    rowsets_to_delete.push_back(rowset_id.to_string());
    save_tablet_meta_req.__set_rowsets_to_delete(rowsets_to_delete);
    if (expected_version > 0) {
        save_tablet_meta_req.__set_expected_modify_version(expected_version);
        save_tablet_meta_req.__set_new_modify_version(new_version);
    }
    auto pro = make_shared<promise<OLAPStatus>>();
    PushTabletMetaTask push_task;
    push_task.save_tablet_meta_req = save_tablet_meta_req;
    push_task.pro = pro;
    _push_tablet_pool->offer(push_task);
    return pro->get_future();
}

// add a synchronized method full push 
OLAPStatus TabletSyncService::sync_delete_rowset_meta(const RowsetId& rowset_id, int64_t expected_version, int64_t new_version) {
    std::future<OLAPStatus> res_future = delete_rowset_meta(rowset_id, expected_version, new_version);
    return res_future.get();
}

// fetch both tablet meta and all rowset meta
// when create a tablet, if it's eco_mode and term > 1 then should fetch
// all rowset and tablet meta from remote meta store
// Maybe, it's better to add a callback function here
std::future<GetTabletMetaRespPB> TabletSyncService::fetch_tablet_meta(const TabletSharedPtr& tablet, bool load_data) {
    auto pro = make_shared<promise<GetTabletMetaRespPB>>();
    FetchTabletMetaTask fetch_task;
    fetch_task.tablet = tablet;
    fetch_task.load_data = load_data;
    fetch_task.pro = pro;
    _fetch_tablet_pool->offer(fetch_task);
    return pro->get_future();
}

GetTabletMetaRespPB TabletSyncService::sync_fetch_tablet_meta(const TabletSharedPtr& tablet, bool load_data) {
    std::future<GetTabletMetaRespPB> res_future = fetch_tablet_meta(tablet, load_data);
    return res_future.get();
}

std::future<OLAPStatus> TabletSyncService::push_tablet_meta(const TabletMetaPB& tablet_meta, 
    int64_t expected_version, int64_t new_version) {
    CHECK(expected_version > 0) << "during update tablet meta, expected version should > 0";
    SaveTabletMetaReq save_tablet_meta_req;
    save_tablet_meta_req.__set_tablet_id(tablet_meta.tablet_id());
    save_tablet_meta_req.__set_schema_hash(tablet_meta.schema_hash());
    string meta_binary;
    bool serialize_success = tablet_meta.SerializeToString(&meta_binary);
    if (!serialize_success) {
        LOG(FATAL) << "failed to serialize meta " << tablet_meta.tablet_id();
    }
    save_tablet_meta_req.__set_tablet_meta_binary(meta_binary);
    save_tablet_meta_req.__set_expected_modify_version(expected_version);
    save_tablet_meta_req.__set_new_modify_version(new_version);
    auto pro = make_shared<promise<OLAPStatus>>();
    PushTabletMetaTask push_task;
    push_task.save_tablet_meta_req = save_tablet_meta_req;
    push_task.pro = pro;
    _push_tablet_pool->offer(push_task);
    return pro->get_future();
}

OLAPStatus TabletSyncService::sync_push_tablet_meta(const TabletMetaPB& tablet_meta, 
    int64_t expected_version, int64_t new_version) {
    std::future<OLAPStatus> res_future = push_tablet_meta(tablet_meta);
    return res_future.get();
}

// add a synchronized method full push 
OLAPStatus TabletSyncService::sync_push_rowset_meta(const RowsetMetaPB& rowset_meta) {
    std::future<OLAPStatus> res_future = push_rowset_meta(rowset_meta);
    return res_future.get();
}

void TabletSyncService::_fetch_rowset_meta_thread(std::vector<FetchRowsetMetaTask> tasks) {
    // should get tablet info from tablet mgr, it will not dead lock as far as i know
    return;
}

void TabletSyncService::_fetch_tablet_meta_thread(std::vector<FetchTabletMetaTask> tasks) {
    return;
}

void TabletSyncService::_push_tablet_meta_thread(std::vector<PushTabletMetaTask> tasks) {
    
    return;
}

void TabletSyncService::_convert_to_get_rowset_req(const TabletSharedPtr& tablet, int64_t txn_id, 
    int64_t start_version, int64_t end_version, GetRowsetMetaReq* get_rowset_meta_req) {
    get_rowset_meta_req->__set_tablet_id(tablet->tablet_id());
    get_rowset_meta_req->__set_schema_hash(tablet->schema_hash());
    if (txn_id > 0) {
        CHECK(start_version == 0 && end_version == 0);
        get_rowset_meta_req->__set_txn_id(txn_id);
    } else {
        CHECK(txn_id == 0);
        get_rowset_meta_req->__set_start_version(start_version);
        get_rowset_meta_req->__set_end_version(end_version);
    }
}

void TabletSyncService::_convert_to_save_rowset_req(const RowsetMetaPB& rowset_meta_pb, 
    SaveRowsetMetaReq* save_rowset_meta_req) {
    RowsetMeta rowset_meta;
    rowset_meta.init_from_pb(rowset_meta_pb);
    save_rowset_meta_req->__set_rowset_id(rowset_meta.rowset_id().to_string());
    if (rowset_meta_pb.has_start_version() && rowset_meta_pb.start_version() > 0
        && rowset_meta_pb.has_end_version() && rowset_meta_pb.end_version() > 0) {
        CHECK(rowset_meta_pb.rowset_state() == RowsetStatePB::VISIBLE);
        // it is not a singleton delta
        if (rowset_meta_pb.start_version() != rowset_meta_pb.end_version()) {
            CHECK(rowset_meta_pb.has_txn_id() == false || rowset_meta_pb.txn_id() == 0);
            save_rowset_meta_req->__set_txn_id(0);
        } else {
            CHECK(rowset_meta_pb.has_txn_id() && rowset_meta_pb.txn_id() > 0);
            save_rowset_meta_req->__set_txn_id(rowset_meta_pb.txn_id());
        }
        save_rowset_meta_req->__set_start_version(rowset_meta_pb.start_version());
        save_rowset_meta_req->__set_end_version(rowset_meta_pb.end_version());
        if (rowset_meta_pb.has_version_hash()) {
            save_rowset_meta_req->__set_version_hash(rowset_meta_pb.version_hash());
        }
    } else {
        CHECK(rowset_meta_pb.rowset_state() == RowsetStatePB::COMMITTED);
        CHECK(rowset_meta_pb.has_start_version() == false && rowset_meta_pb.has_end_version() == false);
        CHECK(rowset_meta_pb.has_txn_id() && rowset_meta_pb.txn_id() > 0);
        save_rowset_meta_req->__set_txn_id(rowset_meta_pb.txn_id());
    }
    string meta_binary;
    bool serialize_success = rowset_meta_pb.SerializeToString(&meta_binary);
    if (!serialize_success) {
        LOG(FATAL) << "failed to serialize rowset meta";
    }
    save_tablet_meta_req.__set_meta_binary(meta_binary);
}

} // doris