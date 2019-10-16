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

namespace cpp doris
namespace java org.apache.doris.thrift

include "AgentService.thrift"
include "PaloInternalService.thrift"
include "Status.thrift"
include "Types.thrift"

// Two or more BEs will try to modify tablet meta or rowset meta
// snowflake using a transaction mechanism to avoid concurrency or conflict
// but our meta store service may not have transaction. We only assume that the meta store
// service has CAS mechanism. It depends on CAS mechanism to solve concurrency problem.
// Only tablet meta has CAS mechanism, rowset meta does not have it. If rowset meta related request has expected version
// it means the service should check tablet meta's expect version.
// 
// Meta store service should provide batch interface to allow BEs getting or saving meta batch by batch
// since batch is more efficient.
//
// Every response has a status field to indicate whether the request is successful or failed
struct SaveRowsetMetaReq {
    1: optional string rowset_id
    // rowset's start version
    2: optional Types.TVersion start_version
    // rowset's end version
    3: optional Types.TVersion end_version
    // rowset's version hash
    4: optional Types.TVersionHash version_hash
    // the txn that the rowset belongs to, it maybe 0 because the rowset maybe generated during compaction
    5: optional i64 txn_id
    6: optional binary meta_binary
}

struct SaveTabletMetaReq {
    // the tablet id that the rowset belongs to
    1: optional Types.TTabletId tablet_id
    // tablet's schema hash
    2: optional Types.TSchemaHash schema_hash
    // tablet_meta_binary maybe not set if tablet meta need not updated
    3: optional binary tablet_meta_binary
    4: optional list<SaveRowsetMetaReq> rowsets_to_save
    // contains rowsetids that to delete
    5: optional list<string> rowsets_to_delete 
    // if is_delete_req = true, it means user want to delete the tablet
    6: optional bool is_delete_req = false
    // if expected modify version is set then the service should check
    // if current version == exp version. it indicates the service should use
    // CAS mechanism
    7: optional i64 expected_modify_version
    8: optional i64 new_modify_version
}

struct BatchSaveTabletMetaReq {
    1: optional list<SaveTabletMetaReq> reqs
}

struct GetTabletMetaReq {
    // the tablet id that the rowset belongs to
    1: optional Types.TTabletId tablet_id
    // tablet's schema hash
    2: optional Types.TSchemaHash schema_hash
    // indicate whether to get the increment rowsets
    3: optional bool include_rowsets
}

struct BatchGetTabletMetaReq {
    1: list<GetTabletMetaReq> reqs
}

// could use txn id to query rowset or using version to query rowset
struct GetRowsetMetaReq {
    // the tablet id that the rowset belongs to
    1: optional Types.TTabletId tablet_id
    // tablet's schema hash
    2: optional Types.TSchemaHash schema_hash
    // rowset's start version
    3: optional Types.TVersion start_version
    // rowset's end version
    4: optional Types.TVersion end_version
    5: optional Types.TVersionHash version_hash
    // rowset related txn id
    6: optional i64 txn_id
}

struct BatchGetRowsetMetaReq {
    1: optional list<GetRowsetMetaReq> reqs
}

struct SaveTabletMetaResponse {
    1: optional Status.TStatus status
}

struct BatchSaveTabletMetaResponse {
    1: optional list<SaveTabletMetaResponse> resps
}

struct GetTabletMetaResponse {
    1: optional Status.TStatus status
    2: optional binary tablet_meta
    3: optional list<binary> rowset_metas
    4: optional i64 modify_version
}

struct BatchGetTabletMetaResponse {
    1: optional list<GetTabletMetaResponse> resps
}

struct GetRowsetMetaResponse {
    1: optional Status.TStatus status
    2: optional binary rowset_meta
}

struct BatchGetRowsetMetaResponse {
    1: optional list<GetRowsetMetaResponse> resps
}

// currently there are only batch interfaces
service TMetaStoreService {
    // for rowset meta, if tablet_id, txn_id, start_version, end_version should unique
    BatchSaveTabletMetaResponse save_tablet_meta(1: BatchSaveTabletMetaReq save_tablet_meta_req)
    BatchGetTabletMetaResponse get_tablet_meta(1: BatchGetTabletMetaReq get_tablet_meta_req)
    BatchGetRowsetMetaResponse get_rowset_meta(1: BatchGetRowsetMetaReq get_rowset_meta_req)
}