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
#include "gen_cpp/HeartbeatService.h"
#include "gen_cpp/TMetaStoreService.h"
#include "util/thrift_rpc_helper.h"

using namespace std;

namespace doris {

TabletSyncService::TabletSyncService(ExecEnv* env) {
    // TODO(ygl): add new config
    _fetch_rowset_pool = new BatchProcessThreadPool<FetchRowsetMetaTask>(
        config::meta_store_request_thread_num,  // thread num
        config::meta_store_request_queue_size,  // queue size
        config::meta_store_request_batch_size, // batch size
        std::bind<void>(std::mem_fn(&TabletSyncService::_fetch_rowset_meta_thread), this, std::placeholders::_1));

    _fetch_tablet_pool = new BatchProcessThreadPool<FetchTabletMetaTask>(
        config::meta_store_request_thread_num,  // thread num
        config::meta_store_request_queue_size,  // queue size
        config::meta_store_request_batch_size, // batch size
        std::bind<void>(std::mem_fn(&TabletSyncService::_fetch_tablet_meta_thread), this, std::placeholders::_1));
    _push_tablet_pool = new BatchProcessThreadPool<PushTabletMetaTask>(
        config::meta_store_request_thread_num,  // thread num
        config::meta_store_request_queue_size,  // queue size
        config::meta_store_request_batch_size, // batch size
        std::bind<void>(std::mem_fn(&TabletSyncService::_push_tablet_meta_thread), this, std::placeholders::_1));
    
    _env = env;
}


TabletSyncService::~TabletSyncService() {
    if (_fetch_rowset_pool != nullptr) {
        delete _fetch_rowset_pool;
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
std::future<RowsetMetaPB> TabletSyncService::fetch_rowset(const TabletSharedPtr& tablet, int64_t txn_id, bool load_data, 
    std::function<void(const RowsetMetaPB&)> after_callback) {
    GetRowsetMetaReq get_rowset_meta_req;
    _convert_to_get_rowset_req(tablet, txn_id, 0, 0, &get_rowset_meta_req);
    auto pro = make_shared<promise<RowsetMetaPB>>();
    FetchRowsetMetaTask fetch_task;
    fetch_task.get_rowset_meta_req = get_rowset_meta_req;
    fetch_task.load_data = load_data;
    fetch_task.pro = pro;
    _fetch_rowset_pool->offer(fetch_task);
    return pro->get_future();
}

// fetch rowset meta and data using version
std::future<RowsetMetaPB> TabletSyncService::fetch_rowset(const TabletSharedPtr& tablet, const Version& version, bool load_data, 
    std::function<void(const RowsetMetaPB&)> after_callback) {
    GetRowsetMetaReq get_rowset_meta_req;
    _convert_to_get_rowset_req(tablet, 0, version.first, version.second, &get_rowset_meta_req);
    auto pro = make_shared<promise<RowsetMetaPB>>();
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
    vector<SaveRowsetMetaReq> save_rowset_meta_reqs;
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
    return _parse_return_value(res_future);
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
    return _parse_return_value(res_future);
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

OLAPStatus TabletSyncService::sync_fetch_tablet_meta(const TabletSharedPtr& tablet, bool load_data, GetTabletMetaRespPB* result) {
    std::future<GetTabletMetaRespPB> res_future = fetch_tablet_meta(tablet, load_data);
    std::future_status status = res_future.wait_for(std::chrono::seconds(config::meta_store_timeout_secs));
    if (status == std::future_status::deferred) {
        return OLAP_ERR_REMOTE_META_TIMEOUT;
    } else if (status == std::future_status::timeout) {
        return OLAP_ERR_REMOTE_META_TIMEOUT;
    } else if (status == std::future_status::ready) {
        *result = res_future.get();
        return OLAP_SUCCESS;
    }
    return OLAP_ERR_REMOTE_META_TIMEOUT;
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
    std::future<OLAPStatus> res_future = push_tablet_meta(tablet_meta, expected_version, new_version);
    return _parse_return_value(res_future);
}

void TabletSyncService::_fetch_rowset_meta_thread(std::vector<FetchRowsetMetaTask> tasks) {
    // should get tablet info from tablet mgr, it will not dead lock as far as i know
    BatchGetRowsetMetaResponse b_response;
    BatchGetRowsetMetaReq b_req;
    vector<GetRowsetMetaReq> get_rowset_reqs;
    for (auto task : tasks) {
        get_rowset_reqs.push_back(task.get_rowset_meta_req);
    }
    b_req.__set_reqs(get_rowset_reqs);
    TNetworkAddress meta_store_addr = _env->master_info()->network_address;
    Status rpc_st = ThriftRpcHelper::rpc<TMetaStoreServiceClient>(
        meta_store_addr.hostname, meta_store_addr.port,
        [&b_req, &b_response] (MetaStoreServiceConnection& client) {
            client->get_rowset_meta(b_response, b_req);
        }, config::meta_store_timeout_secs);
    OLAPStatus res = _check_rpc_return_value(rpc_st, tasks, b_response);
    if (res != OLAP_SUCCESS) {
        return;
    }
    for (int i = 0; i < tasks.size(); ++i) {
        auto& get_rowset_resp = b_response.resps[i];
        if (get_rowset_resp.status.status_code != TStatusCode::OK) {
            LOG(WARNING) << "failed to get rowset meta, res=" << get_rowset_resp.status.status_code;
            tasks[i].pro->set_exception(std::make_exception_ptr(std::runtime_error(std::to_string(get_rowset_resp.status.status_code))));
        } else {
            RowsetMetaPB rowset_meta_pb;
            bool parsed = rowset_meta_pb.ParseFromString(get_rowset_resp.rowset_meta);
            if (parsed) {
                if (tasks[i].after_fetch_callback != nullptr) {
                    tasks[i].after_fetch_callback(rowset_meta_pb);
                }
                tasks[i].pro->set_value(rowset_meta_pb);
            } else {
                tasks[i].pro->set_exception(std::make_exception_ptr(std::runtime_error("parse rowset meta pb error")));
            }
        }
    }
    return;
}

void TabletSyncService::_fetch_tablet_meta_thread(std::vector<FetchTabletMetaTask> tasks) {
    BatchGetTabletMetaResponse b_response;
    BatchGetTabletMetaReq b_req;
    vector<GetTabletMetaReq> get_tablet_reqs;
    for (auto task : tasks) {
        GetTabletMetaReq get_tablet_req;
        get_tablet_req.__set_tablet_id(task.tablet->tablet_id());
        get_tablet_req.__set_schema_hash(task.tablet->schema_hash());
        get_tablet_req.__set_include_rowsets(true);
        get_tablet_reqs.push_back(get_tablet_req);
    }
    b_req.__set_reqs(get_tablet_reqs);
    TNetworkAddress meta_store_addr = _env->master_info()->network_address;
    Status rpc_st = ThriftRpcHelper::rpc<TMetaStoreServiceClient>(
        meta_store_addr.hostname, meta_store_addr.port,
        [&b_req, &b_response] (MetaStoreServiceConnection& client) {
            client->get_tablet_meta(b_response, b_req);
        }, config::meta_store_timeout_secs);
    OLAPStatus res = _check_rpc_return_value(rpc_st, tasks, b_response);
    if (res != OLAP_SUCCESS) {
        return;
    }
    for (int i = 0; i < tasks.size(); ++i) {
        auto& get_tablet_resp = b_response.resps[i];
        if (get_tablet_resp.status.status_code != TStatusCode::OK) {
            LOG(WARNING) << "failed to get rowset meta, res=" << get_tablet_resp.status.status_code;
            tasks[i].pro->set_exception(std::make_exception_ptr(std::runtime_error(std::to_string(get_tablet_resp.status.status_code))));
        } else {
            GetTabletMetaRespPB get_tablet_meta_pb;
            OLAPStatus res = _convert_to_get_tablet_meta_pb(get_tablet_resp, &get_tablet_meta_pb);
            if (res != OLAP_SUCCESS) {
                tasks[i].pro->set_exception(std::make_exception_ptr(std::runtime_error(std::to_string(res))));
            } else {
                tasks[i].pro->set_value(get_tablet_meta_pb);
            }
        }
    }
    return;
}

void TabletSyncService::_push_tablet_meta_thread(std::vector<PushTabletMetaTask> tasks) {
    BatchSaveTabletMetaResponse b_response;
    BatchSaveTabletMetaReq b_req;
    vector<SaveTabletMetaReq> save_tablet_reqs;
    for (auto task : tasks) {
        save_tablet_reqs.push_back(task.save_tablet_meta_req);
    }
    b_req.__set_reqs(save_tablet_reqs);
    TNetworkAddress meta_store_addr = _env->master_info()->network_address;
    Status rpc_st = ThriftRpcHelper::rpc<TMetaStoreServiceClient>(
        meta_store_addr.hostname, meta_store_addr.port,
        [&b_req, &b_response] (MetaStoreServiceConnection& client) {
            client->save_tablet_meta(b_response, b_req);
        }, config::meta_store_timeout_secs);
    OLAPStatus res = _check_rpc_return_value(rpc_st, tasks, b_response);
    if (res != OLAP_SUCCESS) {
        return;
    }
    for (int i = 0; i < tasks.size(); ++i) {
        auto& save_tablet_resp = b_response.resps[i];
        if (save_tablet_resp.status.status_code != TStatusCode::OK) {
            LOG(WARNING) << "failed to get rowset meta, res=" << save_tablet_resp.status.status_code;
            tasks[i].pro->set_exception(std::make_exception_ptr(std::runtime_error(std::to_string(save_tablet_resp.status.status_code))));
        } else {
            tasks[i].pro->set_value(OLAP_SUCCESS);
        }
    }
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
    save_rowset_meta_req->__set_meta_binary(meta_binary);
}

OLAPStatus TabletSyncService::_convert_to_get_tablet_meta_pb(const GetTabletMetaResponse& resp, GetTabletMetaRespPB* get_tablet_meta_resp_pb) {
    if (resp.status.status_code != TStatusCode::OK) {
        LOG(WARNING) << "fetch tablet meta response is " << resp.status.status_code << "not parse the response";
        return OLAP_ERR_REMOTE_META_PARSE_RESP;
    }
    if (!resp.__isset.tablet_meta || !resp.__isset.rowset_metas || !resp.__isset.modify_version) {
        LOG(WARNING) << "some field in reponse not set, not parse the response";
        return OLAP_ERR_REMOTE_META_PARSE_RESP;
    }
    TabletMetaPB tablet_meta_pb;
    bool parsed = tablet_meta_pb.ParseFromString(resp.tablet_meta);
    if (!parsed) {
        LOG(WARNING) << "failed to parse tablet meta pb from string";
        return OLAP_ERR_REMOTE_META_PARSE_RESP;
    }
    get_tablet_meta_resp_pb->tablet_meta_pb = tablet_meta_pb;
    for (auto rowset_meta : resp.rowset_metas) {
        RowsetMetaPB rowset_meta_pb;
        parsed = rowset_meta_pb.ParseFromString(rowset_meta);
        if (!parsed) {
            LOG(WARNING) << "failed to parse rowset meta pb from string";
            return OLAP_ERR_REMOTE_META_PARSE_RESP;
        }
        get_tablet_meta_resp_pb->rowset_meta_pbs.push_back(rowset_meta_pb);
    }
    return OLAP_SUCCESS;

}

OLAPStatus TabletSyncService::_parse_return_value(std::future<OLAPStatus>& res_future) {
    std::future_status status = res_future.wait_for(std::chrono::seconds(config::meta_store_timeout_secs));
    if (status == std::future_status::deferred) {
        return OLAP_ERR_REMOTE_META_TIMEOUT;
    } else if (status == std::future_status::timeout) {
        return OLAP_ERR_REMOTE_META_TIMEOUT;
    } else if (status == std::future_status::ready) {
        return res_future.get();
    }
    return OLAP_ERR_REMOTE_META_TIMEOUT;
}


template<typename T1, typename T2, typename T3>
OLAPStatus TabletSyncService::_check_rpc_return_value(T1 rpc_st, T2 tasks, T3 b_response) {
    if (!rpc_st.ok()) {
        LOG(WARNING) << "failed to call meta store service, res=" << rpc_st.to_string();
        for (auto task : tasks) {
            task.pro->set_exception(std::make_exception_ptr(std::runtime_error(rpc_st.to_string())));
        }
        return OLAP_ERR_REMOTE_META_PARSE_RESP;
    }
    if (b_response.__isset.resps) {
        LOG(WARNING) << "response field not set, it's a bad response";
        for (auto task : tasks) {
            task.pro->set_exception(std::make_exception_ptr(std::runtime_error("response filed not set")));
        }
        return OLAP_ERR_REMOTE_META_PARSE_RESP;
    }
    if (tasks.size() != b_response.resps.size()) {
        LOG(WARNING) << "response size=" << b_response.resps.size() 
                     << ", while task num=" << tasks.size() << " not equal, it's a bad response";
        for (auto task : tasks) {
            task.pro->set_exception(std::make_exception_ptr(std::runtime_error("response size not match task size")));
        }
        return OLAP_ERR_REMOTE_META_PARSE_RESP;
    }
    return OLAP_SUCCESS;
}

} // doris