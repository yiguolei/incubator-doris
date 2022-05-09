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

#include "vec/sink/arrow_result_writer.h"

#include "runtime/buffer_control_block.h"
#include "runtime/runtime_state.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
namespace vectorized {
VArrowResultWriter::VArrowResultWriter(const RowDescriptor& row_desc, BufferControlBlock* sinker,
                                       const std::vector<VExprContext*>& output_vexpr_ctxs,
                                       RuntimeProfile* parent_profile)
        : VResultWriter(),
          _row_desc(row_desc) _sinker(sinker),
          _output_vexpr_ctxs(output_vexpr_ctxs),
          _parent_profile(parent_profile) {}

Status VArrowResultWriter::init(RuntimeState* state) {
    _init_profile();
    if (nullptr == _sinker) {
        return Status::InternalError("sinker is NULL pointer.");
    }
    // generate the arrow schema
    RETURN_IF_ERROR(convert_to_arrow_schema(_row_desc, &_arrow_schema));
    return Status::OK();
}

void VArrowResultWriter::_init_profile() {
    _convert_tuple_timer = ADD_CHILD_TIMER(_parent_profile, "TupleConvertTime", "AppendBatchTime");
    _result_send_timer = ADD_CHILD_TIMER(_parent_profile, "ResultRendTime", "AppendBatchTime");
    _sent_rows_counter = ADD_COUNTER(_parent_profile, "NumSentRows", TUnit::UNIT);
}

Status VArrowResultWriter::append_row_batch(const RowBatch* batch) {
    return Status::RuntimeError("Not Implemented MysqlResultWriter::append_row_batch scalar");
}

Status VArrowResultWriter::append_block(Block& input_block) {
    // serialize the record batch to binary string and send to FE
    std::string serialized_record_batch;
    {
        SCOPED_TIMER(_convert_tuple_timer);
        // convert block to arrow recordbatch
        std::shared_ptr<arrow::RecordBatch> record_batch;
        RETURN_IF_ERROR(convert_to_arrow_record_batch(input_block, _arrow_schema,
                                                      arrow::default_memory_pool(), &record_batch));
        RETURN_IF_ERROR(serialize_record_batch(*record_batch, &serialized_record_batch));
    }
    SCOPED_TIMER(_result_send_timer);
    auto result = std::make_unique<TFetchDataResult>();
    // all arrow record batch is saved in a single row as a binary
    result->result_batch.rows.resize(1);
    result->result_batch.rows[0] = serialized_record_batch;
    // push this batch to back
    Status status = _sinker->add_batch(result);

    if (status.ok()) {
        _written_rows += input_block.rows();
    } else {
        LOG(WARNING) << "append result batch to sink failed.";
    }
    return status;
}

Status VArrowResultWriter::close() {
    COUNTER_SET(_sent_rows_counter, _written_rows);
    return Status::OK();
}

} // namespace vectorized
} // namespace doris
