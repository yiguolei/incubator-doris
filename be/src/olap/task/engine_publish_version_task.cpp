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

#include "olap/task/engine_publish_version_task.h"
#include "olap/data_dir.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/tablet_manager.h"
#include <map>

namespace doris {

using std::map;

EnginePublishVersionTask::EnginePublishVersionTask(TPublishVersionRequest& publish_version_req, 
                                                   vector<TTabletId>* error_tablet_ids)
    : _publish_version_req(publish_version_req), 
      _error_tablet_ids(error_tablet_ids) {}

OLAPStatus EnginePublishVersionTask::finish() {
    LOG(INFO) << "begin to process publish version. transaction_id="
              << _publish_version_req.transaction_id;

    int64_t transaction_id = _publish_version_req.transaction_id;
    OLAPStatus res = OLAP_SUCCESS;

    // each partition
    for (auto& partitionVersionInfo
         : _publish_version_req.partition_version_infos) {

        int64_t partition_id = partitionVersionInfo.partition_id;
        // get all partition related tablets and check whether the tablet have the related version
        std::set<TabletInfo> partition_related_tablet_infos;
        StorageEngine::instance()->tablet_manager()->get_partition_related_tablets(partition_id, 
            &partition_related_tablet_infos);
        if (_publish_version_req.strict_mode && partition_related_tablet_infos.empty()) {
            LOG(INFO) << "could not find related tablet for partition " << partition_id
                      << ", skip publish version";
            continue;
        }

        map<TabletInfo, RowsetSharedPtr> tablet_related_rs;
        StorageEngine::instance()->txn_manager()->get_txn_related_tablets(transaction_id, partition_id, &tablet_related_rs);

        Version version(partitionVersionInfo.version, partitionVersionInfo.version);
        VersionHash version_hash = partitionVersionInfo.version_hash;

        // each tablet
        for (auto& tablet_rs : tablet_related_rs) {
            OLAPStatus publish_status = OLAP_SUCCESS;
            TabletInfo tablet_info = tablet_rs.first;
            RowsetSharedPtr rowset = tablet_rs.second;
            LOG(INFO) << "begin to publish version on tablet. "
                    << "tablet_id=" << tablet_info.tablet_id
                    << ", schema_hash=" << tablet_info.schema_hash
                    << ", version=" << version.first
                    << ", version_hash=" << version_hash
                    << ", transaction_id=" << transaction_id;
            // if rowset is null, it means this be received write task, but failed during write
            // and receive fe's publish version task
            // this be must return as an error tablet
            if (rowset == nullptr) {
                LOG(WARNING) << "could not find related rowset for tablet " << tablet_info.tablet_id
                             << " txn id " << transaction_id;
                _error_tablet_ids->push_back(tablet_info.tablet_id);
                res = OLAP_ERR_PUSH_ROWSET_NOT_FOUND;
                continue;
            }
            TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_info.tablet_id, 
                tablet_info.schema_hash, tablet_info.tablet_uid);

            if (tablet == nullptr) {
                LOG(WARNING) << "can't get tablet when publish version. tablet_id=" << tablet_info.tablet_id
                             << " schema_hash=" << tablet_info.schema_hash;
                _error_tablet_ids->push_back(tablet_info.tablet_id);
                res = OLAP_ERR_PUSH_TABLE_NOT_EXIST;
                continue;
            }

            publish_status = StorageEngine::instance()->txn_manager()->publish_txn(partition_id, tablet, 
                transaction_id, version, version_hash);
            
            if (publish_status != OLAP_SUCCESS) {
                LOG(WARNING) << "failed to publish for rowset_id:" << rowset->rowset_id()
                             << "tablet id: " << tablet_info.tablet_id
                             << "txn id:" << transaction_id;
                _error_tablet_ids->push_back(tablet_info.tablet_id);
                res = publish_status;
                continue;
            }
            // add visible rowset to tablet
            publish_status = tablet->add_rowset(rowset, false);
            if (publish_status != OLAP_SUCCESS && publish_status != OLAP_ERR_PUSH_VERSION_ALREADY_EXIST) {
                LOG(WARNING) << "add visible rowset to tablet failed rowset_id:" << rowset->rowset_id()
                             << "tablet id: " << tablet_info.tablet_id
                             << "txn id:" << transaction_id
                             << "res:" << publish_status;
                _error_tablet_ids->push_back(tablet_info.tablet_id);
                res = publish_status;
                continue;
            }
            partition_related_tablet_infos.erase(tablet_info);
            LOG(INFO) << "publish version successfully on tablet. tablet=" << tablet->full_name()
                      << ", transaction_id=" << transaction_id << ", version=" << version.first
                      << ", res=" << publish_status;
        }

        // check if the related tablet remained all have the version
        for (auto& tablet_info : partition_related_tablet_infos) {
            // has to use strict mode to check if check all tablets
            if (!_publish_version_req.strict_mode) {
                break;
            }
            TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(
                tablet_info.tablet_id, tablet_info.schema_hash);
            if (tablet == nullptr) {
                _error_tablet_ids->push_back(tablet_info.tablet_id);
            } else {
                // check if the version exist, if not exist, then set publish failed
                if (!tablet->check_version_exist(version)) {
                    _error_tablet_ids->push_back(tablet_info.tablet_id);
                    // generate a pull rowset meta task to pull rowset from remote meta store and storage
                    // pull rowset meta using tablet_id + txn_id
                    // it depends on the tablet type to download file or only meta
                    if (tablet->in_econ_mode()) {
                        // both primary and shadow replica should fetch the meta using txn id
                        // it will fetch the rowset to meta store
                        // will be published in next publish version task using callback method
                        auto fetch_rowset_callback = [tablet, transaction_id, version, version_hash](const RowsetMetaPB& rowset_meta_pb) -> void {
                            // this function not own any lock
                            // check if rowset meta alreay with version
                            RowsetSharedPtr rowset;
                            OLAPStatus create_status = RowsetFactory::load_rowset(tablet->tablet_schema(), 
                                                             tablet->tablet_path(), 
                                                             tablet->data_dir(), 
                                                             rowset_meta_pb, &rowset);
                            if (create_status != OLAP_SUCCESS) {
                                LOG(WARNING) << "could not create rowset from rowsetmetapb"
                                             << "tablet:" << tablet->full_name();
                                return;
                            }
                            if (rowset->rowset_meta()->tablet_uid() != tablet->tablet_uid()) {
                                // it is just a check, it should not happen
                                LOG(WARNING) << "rowset's tablet uid is not equal to tablet's uid"
                                             << "rowset's uid:" << rowset->rowset_meta()->tablet_uid().to_string()
                                             << "tablet:" << tablet->full_name();
                                return;
                            }   
                            RowsetMetaSharedPtr rowset_meta = rowset->rowset_meta();
                            // if the rowset is not visible, then publish version
                            bool sync_to_remote = false;
                            if (rowset->rowset_meta()->rowset_state() == RowsetStatePB::COMMITTED) {
                                // the remote rowset is not visible, then should publish it
                                rowset->make_visible(version, version_hash);
                                sync_to_remote = true;
                            }
                            RowsetMetaPB new_rowset_meta_pb;
                            rowset->rowset_meta()->to_rowset_pb(&new_rowset_meta_pb);
                            // not check local or remote, just override, because it is visible operation
                            OLAPStatus save_status = RowsetMetaManager::save(tablet->data_dir()->get_meta(), tablet->tablet_uid(), 
                                    rowset->rowset_id(), new_rowset_meta_pb, 0, 0, sync_to_remote);
                            if (save_status != OLAP_SUCCESS) {
                                LOG(WARNING) << "save visible rowset failed. after fetch from remote:"
                                             << rowset->rowset_id()
                                             << ", tablet: " << tablet->full_name()
                                             << ", txn id:" << transaction_id;
                                return;
                            }
                            OLAPStatus publish_status = tablet->add_rowset(rowset, false);
                            if (publish_status != OLAP_SUCCESS && publish_status != OLAP_ERR_PUSH_VERSION_ALREADY_EXIST) {
                                LOG(WARNING) << "add visilbe rowset to tablet failed rowset_id:" << rowset->rowset_id()
                                             << " tablet id: " << rowset_meta->tablet_id()
                                             << " txn id:" << rowset_meta->txn_id()
                                             << " start_version: " << rowset_meta->version().first
                                             << " end_version: " << rowset_meta->version().second;
                            } else {
                                LOG(INFO) << "successfully to add visible rowset: " << rowset_meta->rowset_id()
                                          << " to tablet: " << rowset_meta->tablet_id()
                                          << " txn id:" << rowset_meta->txn_id()
                                          << " start_version: " << rowset_meta->version().first
                                          << " end_version: " << rowset_meta->version().second;
                            }
                            return;
                        };
                        StorageEngine::instance()->tablet_sync_service()->fetch_rowset(tablet, transaction_id, false, fetch_rowset_callback);
                    }
                }
            }
        }
    }

    LOG(INFO) << "finish to publish version on transaction."
              << "transaction_id=" << transaction_id
              << ", error_tablet_size=" << _error_tablet_ids->size();
    return res;
}

} // doris
