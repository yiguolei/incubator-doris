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

#include "vec/utils/arrow_util.h"

#include <arrow/array.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/buffer.h>
#include <arrow/builder.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/memory_pool.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/visit_array_inline.h>
#include <arrow/visit_type_inline.h>
#include <arrow/visitor.h>

#include <cstdlib>
#include <ctime>
#include <memory>

#include "common/logging.h"
#include "exprs/slot_ref.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/row_batch.h"
#include "util/arrow/utils.h"
#include "util/types.h"

namespace doris::vectorized::arrow {

Status convert_to_arrow_type(const TypeDescriptor& type, std::shared_ptr<arrow::DataType>* result) {
    switch (type.type) {
    case TYPE_TINYINT:
        *result = arrow::int8();
        break;
    case TYPE_SMALLINT:
        *result = arrow::int16();
        break;
    case TYPE_INT:
        *result = arrow::int32();
        break;
    case TYPE_BIGINT:
        *result = arrow::int64();
        break;
    case TYPE_FLOAT:
        *result = arrow::float32();
        break;
    case TYPE_DOUBLE:
        *result = arrow::float64();
        break;
    case TYPE_TIME:
        *result = arrow::float64();
        break;
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_HLL:
    case TYPE_LARGEINT:
    case TYPE_DATE:
    case TYPE_DATETIME:
    case TYPE_STRING:
        *result = arrow::utf8();
        break;
    case TYPE_DECIMALV2:
        *result = std::make_shared<arrow::Decimal128Type>(27, 9);
        break;
    case TYPE_BOOLEAN:
        *result = arrow::boolean();
        break;
    default:
        return Status::InvalidArgument("Unknown primitive type");
    }
    return Status::OK();
}

Status convert_to_arrow_field(SlotDescriptor* desc, std::shared_ptr<arrow::Field>* field) {
    std::shared_ptr<arrow::DataType> type;
    RETURN_IF_ERROR(convert_to_arrow_type(desc->type(), &type));
    *field = arrow::field(desc->col_name(), type, desc->is_nullable());
    return Status::OK();
}

// Convert Block to an Arrow::Array
// We should keep this function to keep compatible with arrow's type visitor
// Now we inherit TypeVisitor to use default Visit implementation
class FromBlockConverter : public arrow::TypeVisitor {
public:
    FromBlockConverter(const Block& batch, const std::shared_ptr<arrow::Schema>& schema,
                       arrow::MemoryPool* pool)
            : _block(block), _schema(schema), _pool(pool), _cur_field_idx(-1) {}

    ~FromBlockConverter() override {}

    // Use base class function
    using arrow::TypeVisitor::Visit;
    // process string-transformable field
    arrow::Status Visit(const arrow::StringType& type) override {
        arrow::StringBuilder builder(_pool);
        size_t num_rows = _block.num_rows();
        ARROW_RETURN_NOT_OK(builder.Reserve(num_rows));
        auto column = _block.get_by_position(_cur_field_idx);
        for (size_t i = 0; i < num_rows; ++i) {
            // TODO yiguolei: Not care about nullable here, the column maybe a nullable column
            const auto string_val = column->get_data_at(i);
            if (string_val.data == nullptr) {
                ARROW_RETURN_NOT_OK(builder.Append(""));
            } else {
                ARROW_RETURN_NOT_OK(builder.Append(string_val.data, string_val.size));
            }
        }
        return builder.Finish(&_arrays[_cur_field_idx]);
    }

    Status convert(std::shared_ptr<arrow::RecordBatch>* out);

private:
    const Block& _block;
    const std::shared_ptr<arrow::Schema>& _schema;
    arrow::MemoryPool* _pool;
    size_t _cur_field_idx;
    std::vector<std::shared_ptr<arrow::Array>> _arrays;
};

Status FromBlockConverter::convert(std::shared_ptr<arrow::RecordBatch>* out) {
    size_t num_fields = _schema->num_fields();
    if (_block.columns() != num_fields) {
        return Status::InvalidArgument("number fields not match");
    }

    _arrays.resize(num_fields);
    for (int idx = 0; idx < num_fields; ++idx) {
        _cur_field_idx = idx;
        auto arrow_st = arrow::VisitTypeInline(*_schema->field(idx)->type(), this);
        if (!arrow_st.ok()) {
            return to_status(arrow_st);
        }
    }
    *out = arrow::RecordBatch::Make(_schema, _block.num_rows(), std::move(_arrays));
    return Status::OK();
}

Status convert_to_arrow_batch(const Block& batch, const std::shared_ptr<arrow::Schema>& schema,
                              arrow::MemoryPool* pool,
                              std::shared_ptr<arrow::RecordBatch>* result) {
    FromBlockConverter converter(batch, schema, pool);
    return converter.convert(result);
}

Status serialize_record_batch(const arrow::RecordBatch& record_batch, std::string* result) {
    // create sink memory buffer outputstream with the computed capacity
    int64_t capacity;
    arrow::Status a_st = arrow::ipc::GetRecordBatchSize(record_batch, &capacity);
    if (!a_st.ok()) {
        std::stringstream msg;
        msg << "GetRecordBatchSize failure, reason: " << a_st.ToString();
        return Status::InternalError(msg.str());
    }
    auto sink_res = arrow::io::BufferOutputStream::Create(capacity, arrow::default_memory_pool());
    if (!sink_res.ok()) {
        std::stringstream msg;
        msg << "create BufferOutputStream failure, reason: " << sink_res.status().ToString();
        return Status::InternalError(msg.str());
    }
    std::shared_ptr<arrow::io::BufferOutputStream> sink = sink_res.ValueOrDie();
    // create RecordBatch Writer
    auto res = arrow::ipc::MakeStreamWriter(sink.get(), record_batch.schema());
    if (!res.ok()) {
        std::stringstream msg;
        msg << "open RecordBatchStreamWriter failure, reason: " << res.status().ToString();
        return Status::InternalError(msg.str());
    }
    // write RecordBatch to memory buffer outputstream
    std::shared_ptr<arrow::ipc::RecordBatchWriter> record_batch_writer = res.ValueOrDie();
    a_st = record_batch_writer->WriteRecordBatch(record_batch);
    if (!a_st.ok()) {
        std::stringstream msg;
        msg << "write record batch failure, reason: " << a_st.ToString();
        return Status::InternalError(msg.str());
    }
    a_st = record_batch_writer->Close();
    if (!a_st.ok()) {
        std::stringstream msg;
        msg << "Close failed, reason: " << a_st.ToString();
        return Status::InternalError(msg.str());
    }
    auto finish_res = sink->Finish();
    if (!finish_res.ok()) {
        std::stringstream msg;
        msg << "allocate result buffer failure, reason: " << finish_res.status().ToString();
        return Status::InternalError(msg.str());
    }
    *result = finish_res.ValueOrDie()->ToString();
    // close the sink
    a_st = sink->Close();
    if (!a_st.ok()) {
        std::stringstream msg;
        msg << "Close failed, reason: " << a_st.ToString();
        return Status::InternalError(msg.str());
    }
    return Status::OK();
}

} // namespace doris::vectorized::arrow
