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

#include <memory>
#include "olap/rowset/rowset_factory.h"
#include "gen_cpp/olap_file.pb.h"
#include "olap/rowset/alpha_rowset.h"
#include "olap/rowset/alpha_rowset_meta.h"
#include "olap/rowset/alpha_rowset_writer.h"
#include "beta_rowset.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset_writer.h"

namespace doris {

OLAPStatus RowsetFactory::create_rowset(const TabletSchema* schema,
                                      const std::string& rowset_path,
                                      DataDir* data_dir,
                                      RowsetMetaSharedPtr rowset_meta, 
                                      RowsetSharedPtr* rowset) {

    if (rowset_meta->rowset_type() == ALPHA_ROWSET) {
        rowset->reset(new AlphaRowset(schema, rowset_path, data_dir, rowset_meta));
        return (*rowset)->init();
    }
    if (rowset_meta->rowset_type() == BETA_ROWSET)  {
        rowset->reset(new BetaRowset(schema, rowset_path, data_dir, rowset_meta));
        return (*rowset)->init();
    }
    return OLAP_ERR_ROWSET_TYPE_NOT_FOUND; // should never happen
}

OLAPStatus RowsetFactory::create_rowset_writer(const RowsetWriterContext& context,
                                               std::unique_ptr<RowsetWriter>* output) {
    if (context.rowset_type == ALPHA_ROWSET) {
        output->reset(new AlphaRowsetWriter);
        return (*output)->init(context);
    }
    if (context.rowset_type == BETA_ROWSET) {
        output->reset(new BetaRowsetWriter);
        return (*output)->init(context);
    }
    return OLAP_ERR_ROWSET_TYPE_NOT_FOUND;
}

OLAPStatus RowsetFactory::load_rowset(const TabletSchema& schema,
                                      const std::string& rowset_path,
                                      DataDir* data_dir,
                                      const RowsetMetaPB& rowset_meta_pb,
                                      RowsetSharedPtr* rowset) {
    if (!rowset_meta_pb.has_rowset_type()) {
        LOG(FATAL) << "could not find rowset type info";
        return OLAP_ERR_ROWSET_INVALID;
    }
    if (rowset_meta_pb.rowset_type() == RowsetTypePB::ALPHA_ROWSET) {
        RowsetMetaSharedPtr rowset_meta(new AlphaRowsetMeta());
        bool parsed = rowset_meta->init_from_pb(rowset_meta_pb);
        if (!parsed) {
            LOG(WARNING) << "parse rowset from pb failed";
            // return false will break meta iterator, return true to skip this error
            return OLAP_ERR_ROWSET_INVALID;
        }
        rowset->reset(new AlphaRowset(&schema, rowset_path, data_dir, rowset_meta));
        return (*rowset)->init();
    } else {
        return OLAP_ERR_ROWSET_TYPE_NOT_FOUND;
    }                       
}

OLAPStatus RowsetFactory::create_rowset_meta(const RowsetMetaPB& rowset_meta_pb, RowsetMetaSharedPtr* rowset_meta) {
    if (!rowset_meta_pb.has_rowset_type()) {
        LOG(FATAL) << "could not find rowset type info";
        return OLAP_ERR_ROWSET_INVALID;
    }
    if (rowset_meta_pb.rowset_type() == RowsetTypePB::ALPHA_ROWSET) {
        rowset_meta->reset(new AlphaRowsetMeta());
        bool parsed = (*rowset_meta)->init_from_pb(rowset_meta_pb);
        if (!parsed) {
            LOG(WARNING) << "parse rowset from pb failed";
            // return false will break meta iterator, return true to skip this error
            return OLAP_ERR_ROWSET_INVALID;
        }
        return OLAP_SUCCESS;
    } else {
        LOG(FATAL) << "unsupported rowset type:" << rowset_meta_pb.rowset_type();
        return OLAP_ERR_ROWSET_TYPE_NOT_FOUND;
    }     
}

} // namespace doris
