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

#include "arrow/ipc/metadata-internal.h"

#include <cstdint>
#include <memory>
#include <sstream>
#include <unordered_map>
#include <utility>

#include <flatbuffers/flatbuffers.h>

#include "arrow/array.h"
#include "arrow/extension_type.h"
#include "arrow/io/interfaces.h"
#include "arrow/ipc/File_generated.h"  // IWYU pragma: keep
#include "arrow/ipc/Message_generated.h"
#include "arrow/ipc/SparseTensor_generated.h"  // IWYU pragma: keep
#include "arrow/ipc/Tensor_generated.h"        // IWYU pragma: keep
#include "arrow/ipc/message.h"
#include "arrow/ipc/util.h"
#include "arrow/sparse_tensor.h"
#include "arrow/status.h"
#include "arrow/tensor.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/visitor_inline.h"

namespace arrow {

namespace flatbuf = org::apache::arrow::flatbuf;
using internal::checked_cast;

namespace ipc {
namespace internal {

using FBB = flatbuffers::FlatBufferBuilder;
using DictionaryOffset = flatbuffers::Offset<flatbuf::DictionaryEncoding>;
using FieldOffset = flatbuffers::Offset<flatbuf::Field>;
using KeyValueOffset = flatbuffers::Offset<flatbuf::KeyValue>;
using RecordBatchOffset = flatbuffers::Offset<flatbuf::RecordBatch>;
using SparseTensorOffset = flatbuffers::Offset<flatbuf::SparseTensor>;
using Offset = flatbuffers::Offset<void>;
using FBString = flatbuffers::Offset<flatbuffers::String>;
using KVVector = flatbuffers::Vector<KeyValueOffset>;

static const char kExtensionTypeKeyName[] = "arrow_extension_name";
static const char kExtensionDataKeyName[] = "arrow_extension_data";

MetadataVersion GetMetadataVersion(flatbuf::MetadataVersion version) {
  switch (version) {
    case flatbuf::MetadataVersion_V1:
      // Arrow 0.1
      return MetadataVersion::V1;
    case flatbuf::MetadataVersion_V2:
      // Arrow 0.2
      return MetadataVersion::V2;
    case flatbuf::MetadataVersion_V3:
      // Arrow 0.3 to 0.7.1
      return MetadataVersion::V4;
    case flatbuf::MetadataVersion_V4:
      // Arrow >= 0.8
      return MetadataVersion::V4;
    // Add cases as other versions become available
    default:
      return MetadataVersion::V4;
  }
}

static Status IntFromFlatbuffer(const flatbuf::Int* int_data,
                                std::shared_ptr<DataType>* out) {
  if (int_data->bitWidth() > 64) {
    return Status::NotImplemented("Integers with more than 64 bits not implemented");
  }
  if (int_data->bitWidth() < 8) {
    return Status::NotImplemented("Integers with less than 8 bits not implemented");
  }

  switch (int_data->bitWidth()) {
    case 8:
      *out = int_data->is_signed() ? int8() : uint8();
      break;
    case 16:
      *out = int_data->is_signed() ? int16() : uint16();
      break;
    case 32:
      *out = int_data->is_signed() ? int32() : uint32();
      break;
    case 64:
      *out = int_data->is_signed() ? int64() : uint64();
      break;
    default:
      return Status::NotImplemented("Integers not in cstdint are not implemented");
  }
  return Status::OK();
}

static Status FloatFromFlatbuffer(const flatbuf::FloatingPoint* float_data,
                                  std::shared_ptr<DataType>* out) {
  if (float_data->precision() == flatbuf::Precision_HALF) {
    *out = float16();
  } else if (float_data->precision() == flatbuf::Precision_SINGLE) {
    *out = float32();
  } else {
    *out = float64();
  }
  return Status::OK();
}

// Forward declaration
static Status FieldToFlatbuffer(FBB& fbb, const Field& field,
                                DictionaryMemo* dictionary_memo, FieldOffset* offset);

static Offset IntToFlatbuffer(FBB& fbb, int bitWidth, bool is_signed) {
  return flatbuf::CreateInt(fbb, bitWidth, is_signed).Union();
}

static Offset FloatToFlatbuffer(FBB& fbb, flatbuf::Precision precision) {
  return flatbuf::CreateFloatingPoint(fbb, precision).Union();
}

static Status AppendChildFields(FBB& fbb, const DataType& type,
                                std::vector<FieldOffset>* out_children,
                                DictionaryMemo* dictionary_memo) {
  FieldOffset field;
  for (int i = 0; i < type.num_children(); ++i) {
    RETURN_NOT_OK(FieldToFlatbuffer(fbb, *type.child(i), dictionary_memo, &field));
    out_children->push_back(field);
  }
  return Status::OK();
}

// ----------------------------------------------------------------------
// Union implementation

static Status UnionFromFlatbuffer(const flatbuf::Union* union_data,
                                  const std::vector<std::shared_ptr<Field>>& children,
                                  std::shared_ptr<DataType>* out) {
  UnionMode::type mode =
      (union_data->mode() == flatbuf::UnionMode_Sparse ? UnionMode::SPARSE
                                                       : UnionMode::DENSE);

  std::vector<uint8_t> type_codes;

  const flatbuffers::Vector<int32_t>* fb_type_ids = union_data->typeIds();
  if (fb_type_ids == nullptr) {
    for (uint8_t i = 0; i < children.size(); ++i) {
      type_codes.push_back(i);
    }
  } else {
    for (int32_t id : (*fb_type_ids)) {
      // TODO(wesm): can these values exceed 255?
      type_codes.push_back(static_cast<uint8_t>(id));
    }
  }

  *out = union_(children, type_codes, mode);
  return Status::OK();
}

#define INT_TO_FB_CASE(BIT_WIDTH, IS_SIGNED)            \
  *out_type = flatbuf::Type_Int;                        \
  *offset = IntToFlatbuffer(fbb, BIT_WIDTH, IS_SIGNED); \
  break;

static inline flatbuf::TimeUnit ToFlatbufferUnit(TimeUnit::type unit) {
  switch (unit) {
    case TimeUnit::SECOND:
      return flatbuf::TimeUnit_SECOND;
    case TimeUnit::MILLI:
      return flatbuf::TimeUnit_MILLISECOND;
    case TimeUnit::MICRO:
      return flatbuf::TimeUnit_MICROSECOND;
    case TimeUnit::NANO:
      return flatbuf::TimeUnit_NANOSECOND;
    default:
      break;
  }
  return flatbuf::TimeUnit_MIN;
}

static inline TimeUnit::type FromFlatbufferUnit(flatbuf::TimeUnit unit) {
  switch (unit) {
    case flatbuf::TimeUnit_SECOND:
      return TimeUnit::SECOND;
    case flatbuf::TimeUnit_MILLISECOND:
      return TimeUnit::MILLI;
    case flatbuf::TimeUnit_MICROSECOND:
      return TimeUnit::MICRO;
    case flatbuf::TimeUnit_NANOSECOND:
      return TimeUnit::NANO;
    default:
      break;
  }
  // cannot reach
  return TimeUnit::SECOND;
}

static Status ConcreteTypeFromFlatbuffer(
    flatbuf::Type type, const void* type_data,
    const std::vector<std::shared_ptr<Field>>& children, std::shared_ptr<DataType>* out) {
  switch (type) {
    case flatbuf::Type_NONE:
      return Status::Invalid("Type metadata cannot be none");
    case flatbuf::Type_Null:
      *out = null();
      return Status::OK();
    case flatbuf::Type_Int:
      return IntFromFlatbuffer(static_cast<const flatbuf::Int*>(type_data), out);
    case flatbuf::Type_FloatingPoint:
      return FloatFromFlatbuffer(static_cast<const flatbuf::FloatingPoint*>(type_data),
                                 out);
    case flatbuf::Type_Binary:
      *out = binary();
      return Status::OK();
    case flatbuf::Type_FixedSizeBinary: {
      auto fw_binary = static_cast<const flatbuf::FixedSizeBinary*>(type_data);
      *out = fixed_size_binary(fw_binary->byteWidth());
      return Status::OK();
    }
    case flatbuf::Type_Utf8:
      *out = utf8();
      return Status::OK();
    case flatbuf::Type_Bool:
      *out = boolean();
      return Status::OK();
    case flatbuf::Type_Decimal: {
      auto dec_type = static_cast<const flatbuf::Decimal*>(type_data);
      *out = decimal(dec_type->precision(), dec_type->scale());
      return Status::OK();
    }
    case flatbuf::Type_Date: {
      auto date_type = static_cast<const flatbuf::Date*>(type_data);
      if (date_type->unit() == flatbuf::DateUnit_DAY) {
        *out = date32();
      } else {
        *out = date64();
      }
      return Status::OK();
    }
    case flatbuf::Type_Time: {
      auto time_type = static_cast<const flatbuf::Time*>(type_data);
      TimeUnit::type unit = FromFlatbufferUnit(time_type->unit());
      int32_t bit_width = time_type->bitWidth();
      switch (unit) {
        case TimeUnit::SECOND:
        case TimeUnit::MILLI:
          if (bit_width != 32) {
            return Status::Invalid("Time is 32 bits for second/milli unit");
          }
          *out = time32(unit);
          break;
        default:
          if (bit_width != 64) {
            return Status::Invalid("Time is 64 bits for micro/nano unit");
          }
          *out = time64(unit);
          break;
      }
      return Status::OK();
    }
    case flatbuf::Type_Timestamp: {
      auto ts_type = static_cast<const flatbuf::Timestamp*>(type_data);
      TimeUnit::type unit = FromFlatbufferUnit(ts_type->unit());
      if (ts_type->timezone() != 0 && ts_type->timezone()->Length() > 0) {
        *out = timestamp(unit, ts_type->timezone()->str());
      } else {
        *out = timestamp(unit);
      }
      return Status::OK();
    }
    case flatbuf::Type_Interval:
      return Status::NotImplemented("Interval");
    case flatbuf::Type_List:
      if (children.size() != 1) {
        return Status::Invalid("List must have exactly 1 child field");
      }
      *out = std::make_shared<ListType>(children[0]);
      return Status::OK();
    case flatbuf::Type_Struct_:
      *out = std::make_shared<StructType>(children);
      return Status::OK();
    case flatbuf::Type_Union:
      return UnionFromFlatbuffer(static_cast<const flatbuf::Union*>(type_data), children,
                                 out);
    default:
      return Status::Invalid("Unrecognized type");
  }
}

static Status TypeFromFlatbuffer(const flatbuf::Field* field,
                                 const std::vector<std::shared_ptr<Field>>& children,
                                 const KeyValueMetadata* field_metadata,
                                 std::shared_ptr<DataType>* out) {
  RETURN_NOT_OK(
      ConcreteTypeFromFlatbuffer(field->type_type(), field->type(), children, out));

  // Look for extension metadata in custom_metadata field
  // TODO(wesm): Should this be part of the Field Flatbuffers table?
  if (field_metadata != nullptr) {
    int name_index = field_metadata->FindKey(kExtensionTypeKeyName);
    if (name_index == -1) {
      return Status::OK();
    }
    std::string type_name = field_metadata->value(name_index);
    int data_index = field_metadata->FindKey(kExtensionDataKeyName);
    std::string type_data = data_index == -1 ? "" : field_metadata->value(data_index);

    std::shared_ptr<ExtensionType> type = GetExtensionType(type_name);
    if (type == nullptr) {
      // TODO(wesm): Extension type is unknown; we do not raise here and simply
      // return the raw data
      return Status::OK();
    }
    RETURN_NOT_OK(type->Deserialize(*out, type_data, out));
  }
  return Status::OK();
}

static Status TensorTypeToFlatbuffer(FBB& fbb, const DataType& type,
                                     flatbuf::Type* out_type, Offset* offset) {
  switch (type.id()) {
    case Type::UINT8:
      INT_TO_FB_CASE(8, false);
    case Type::INT8:
      INT_TO_FB_CASE(8, true);
    case Type::UINT16:
      INT_TO_FB_CASE(16, false);
    case Type::INT16:
      INT_TO_FB_CASE(16, true);
    case Type::UINT32:
      INT_TO_FB_CASE(32, false);
    case Type::INT32:
      INT_TO_FB_CASE(32, true);
    case Type::UINT64:
      INT_TO_FB_CASE(64, false);
    case Type::INT64:
      INT_TO_FB_CASE(64, true);
    case Type::HALF_FLOAT:
      *out_type = flatbuf::Type_FloatingPoint;
      *offset = FloatToFlatbuffer(fbb, flatbuf::Precision_HALF);
      break;
    case Type::FLOAT:
      *out_type = flatbuf::Type_FloatingPoint;
      *offset = FloatToFlatbuffer(fbb, flatbuf::Precision_SINGLE);
      break;
    case Type::DOUBLE:
      *out_type = flatbuf::Type_FloatingPoint;
      *offset = FloatToFlatbuffer(fbb, flatbuf::Precision_DOUBLE);
      break;
    default:
      *out_type = flatbuf::Type_NONE;  // Make clang-tidy happy
      return Status::NotImplemented("Unable to convert type: ", type.ToString());
  }
  return Status::OK();
}

static DictionaryOffset GetDictionaryEncoding(FBB& fbb, const DictionaryType& type,
                                              DictionaryMemo* memo) {
  int64_t dictionary_id = memo->GetId(type.dictionary());

  // We assume that the dictionary index type (as an integer) has already been
  // validated elsewhere, and can safely assume we are dealing with signed
  // integers
  const auto& fw_index_type = checked_cast<const FixedWidthType&>(*type.index_type());

  auto index_type_offset = flatbuf::CreateInt(fbb, fw_index_type.bit_width(), true);

  // TODO(wesm): ordered dictionaries
  return flatbuf::CreateDictionaryEncoding(fbb, dictionary_id, index_type_offset,
                                           type.ordered());
}

KeyValueOffset AppendKeyValue(FBB& fbb, const std::string& key,
                              const std::string& value) {
  return flatbuf::CreateKeyValue(fbb, fbb.CreateString(key), fbb.CreateString(value));
}

void AppendKeyValueMetadata(FBB& fbb, const KeyValueMetadata& metadata,
                            std::vector<KeyValueOffset>* key_values) {
  key_values->reserve(metadata.size());
  for (int i = 0; i < metadata.size(); ++i) {
    key_values->push_back(AppendKeyValue(fbb, metadata.key(i), metadata.value(i)));
  }
}

static Status KeyValueMetadataFromFlatbuffer(const KVVector* fb_metadata,
                                             std::shared_ptr<KeyValueMetadata>* out) {
  auto metadata = std::make_shared<KeyValueMetadata>();

  metadata->reserve(fb_metadata->size());
  for (const auto& pair : *fb_metadata) {
    if (pair->key() == nullptr) {
      return Status::IOError(
          "Key-pointer in custom metadata of flatbuffer-encoded Schema is null.");
    }
    if (pair->value() == nullptr) {
      return Status::IOError(
          "Value-pointer in custom metadata of flatbuffer-encoded Schema is null.");
    }
    metadata->Append(pair->key()->str(), pair->value()->str());
  }

  *out = metadata;

  return Status::OK();
}

class FieldToFlatbufferVisitor {
 public:
  FieldToFlatbufferVisitor(FBB& fbb, DictionaryMemo* dictionary_memo)
      : fbb_(fbb), dictionary_memo_(dictionary_memo) {}

  Status VisitType(const DataType& type) { return VisitTypeInline(type, this); }

  Status Visit(const NullType& type) {
    fb_type_ = flatbuf::Type_Null;
    type_offset_ = flatbuf::CreateNull(fbb_).Union();
    return Status::OK();
  }

  Status Visit(const BooleanType& type) {
    fb_type_ = flatbuf::Type_Bool;
    type_offset_ = flatbuf::CreateBool(fbb_).Union();
    return Status::OK();
  }

  template <int BitWidth, bool IsSigned, typename T>
  Status Visit(const T& type) {
    fb_type_ = flatbuf::Type_Int;
    type_offset_ = IntToFlatbuffer(fbb_, BitWidth, IsSigned);
    return Status::OK();
  }

  template <typename T>
  typename std::enable_if<IsInteger<T>::value, Status>::type Visit(const T& type) {
    return Visit<sizeof(typename T::c_type) * 8, IsSignedInt<T>::value>(type);
  }

  Status Visit(const HalfFloatType& type) {
    fb_type_ = flatbuf::Type_FloatingPoint;
    type_offset_ = FloatToFlatbuffer(fbb_, flatbuf::Precision_HALF);
    return Status::OK();
  }

  Status Visit(const FloatType& type) {
    fb_type_ = flatbuf::Type_FloatingPoint;
    type_offset_ = FloatToFlatbuffer(fbb_, flatbuf::Precision_SINGLE);
    return Status::OK();
  }

  Status Visit(const DoubleType& type) {
    fb_type_ = flatbuf::Type_FloatingPoint;
    type_offset_ = FloatToFlatbuffer(fbb_, flatbuf::Precision_DOUBLE);
    return Status::OK();
  }

  Status Visit(const FixedSizeBinaryType& type) {
    const auto& fw_type = checked_cast<const FixedSizeBinaryType&>(type);
    fb_type_ = flatbuf::Type_FixedSizeBinary;
    type_offset_ = flatbuf::CreateFixedSizeBinary(fbb_, fw_type.byte_width()).Union();
    return Status::OK();
  }

  Status Visit(const BinaryType& type) {
    fb_type_ = flatbuf::Type_Binary;
    type_offset_ = flatbuf::CreateBinary(fbb_).Union();
    return Status::OK();
  }

  Status Visit(const StringType& type) {
    fb_type_ = flatbuf::Type_Utf8;
    type_offset_ = flatbuf::CreateUtf8(fbb_).Union();
    return Status::OK();
  }

  Status Visit(const Date32Type& type) {
    fb_type_ = flatbuf::Type_Date;
    type_offset_ = flatbuf::CreateDate(fbb_, flatbuf::DateUnit_DAY).Union();
    return Status::OK();
  }

  Status Visit(const Date64Type& type) {
    fb_type_ = flatbuf::Type_Date;
    type_offset_ = flatbuf::CreateDate(fbb_, flatbuf::DateUnit_MILLISECOND).Union();
    return Status::OK();
  }

  Status Visit(const Time32Type& type) {
    const auto& time_type = checked_cast<const Time32Type&>(type);
    fb_type_ = flatbuf::Type_Time;
    type_offset_ =
        flatbuf::CreateTime(fbb_, ToFlatbufferUnit(time_type.unit()), 32).Union();
    return Status::OK();
  }

  Status Visit(const Time64Type& type) {
    const auto& time_type = checked_cast<const Time64Type&>(type);
    fb_type_ = flatbuf::Type_Time;
    type_offset_ =
        flatbuf::CreateTime(fbb_, ToFlatbufferUnit(time_type.unit()), 64).Union();
    return Status::OK();
  }

  Status Visit(const TimestampType& type) {
    const auto& ts_type = checked_cast<const TimestampType&>(type);
    fb_type_ = flatbuf::Type_Timestamp;
    flatbuf::TimeUnit fb_unit = ToFlatbufferUnit(ts_type.unit());
    FBString fb_timezone = 0;
    if (ts_type.timezone().size() > 0) {
      fb_timezone = fbb_.CreateString(ts_type.timezone());
    }
    type_offset_ = flatbuf::CreateTimestamp(fbb_, fb_unit, fb_timezone).Union();
    return Status::OK();
  }

  Status Visit(const DecimalType& type) {
    const auto& dec_type = checked_cast<const Decimal128Type&>(type);
    fb_type_ = flatbuf::Type_Decimal;
    type_offset_ =
        flatbuf::CreateDecimal(fbb_, dec_type.precision(), dec_type.scale()).Union();
    return Status::OK();
  }

  Status Visit(const ListType& type) {
    fb_type_ = flatbuf::Type_List;
    RETURN_NOT_OK(AppendChildFields(fbb_, type, &children_, dictionary_memo_));
    type_offset_ = flatbuf::CreateList(fbb_).Union();
    return Status::OK();
  }

  Status Visit(const StructType& type) {
    fb_type_ = flatbuf::Type_Struct_;
    RETURN_NOT_OK(AppendChildFields(fbb_, type, &children_, dictionary_memo_));
    type_offset_ = flatbuf::CreateStruct_(fbb_).Union();
    return Status::OK();
  }

  Status Visit(const UnionType& type) {
    fb_type_ = flatbuf::Type_Union;
    RETURN_NOT_OK(AppendChildFields(fbb_, type, &children_, dictionary_memo_));

    const auto& union_type = checked_cast<const UnionType&>(type);

    flatbuf::UnionMode mode = union_type.mode() == UnionMode::SPARSE
                                  ? flatbuf::UnionMode_Sparse
                                  : flatbuf::UnionMode_Dense;

    std::vector<int32_t> type_ids;
    type_ids.reserve(union_type.type_codes().size());
    for (uint8_t code : union_type.type_codes()) {
      type_ids.push_back(code);
    }

    auto fb_type_ids = fbb_.CreateVector(type_ids);

    type_offset_ = flatbuf::CreateUnion(fbb_, mode, fb_type_ids).Union();
    return Status::OK();
  }

  Status Visit(const DictionaryType& type) {
    // In this library, the dictionary "type" is a logical construct. Here we
    // pass through to the value type, as we've already captured the index
    // type in the DictionaryEncoding metadata in the parent field
    return VisitType(*checked_cast<const DictionaryType&>(type).dictionary()->type());
  }

  Status Visit(const ExtensionType& type) {
    RETURN_NOT_OK(VisitType(*type.storage_type()));
    extra_type_metadata_[kExtensionTypeKeyName] = type.extension_name();
    extra_type_metadata_[kExtensionDataKeyName] = type.Serialize();
    return Status::OK();
  }

  Status GetResult(const Field& field, FieldOffset* offset) {
    auto fb_name = fbb_.CreateString(field.name());
    RETURN_NOT_OK(VisitType(*field.type()));
    auto fb_children = fbb_.CreateVector(children_);

    DictionaryOffset dictionary = 0;
    if (field.type()->id() == Type::DICTIONARY) {
      dictionary = GetDictionaryEncoding(
          fbb_, checked_cast<const DictionaryType&>(*field.type()), dictionary_memo_);
    }

    auto metadata = field.metadata();

    flatbuffers::Offset<KVVector> fb_custom_metadata;
    std::vector<KeyValueOffset> key_values;
    if (metadata != nullptr) {
      AppendKeyValueMetadata(fbb_, *metadata, &key_values);
    }

    for (auto it : extra_type_metadata_) {
      key_values.push_back(AppendKeyValue(fbb_, it.first, it.second));
    }

    if (key_values.size() > 0) {
      fb_custom_metadata = fbb_.CreateVector(key_values);
    }
    *offset =
        flatbuf::CreateField(fbb_, fb_name, field.nullable(), fb_type_, type_offset_,
                             dictionary, fb_children, fb_custom_metadata);
    return Status::OK();
  }

 private:
  FBB& fbb_;
  DictionaryMemo* dictionary_memo_;
  flatbuf::Type fb_type_;
  Offset type_offset_;
  std::vector<FieldOffset> children_;
  std::unordered_map<std::string, std::string> extra_type_metadata_;
};

static Status FieldToFlatbuffer(FBB& fbb, const Field& field,
                                DictionaryMemo* dictionary_memo, FieldOffset* offset) {
  FieldToFlatbufferVisitor field_visitor(fbb, dictionary_memo);
  return field_visitor.GetResult(field, offset);
}

static Status GetFieldMetadata(const flatbuf::Field* field,
                               std::shared_ptr<KeyValueMetadata>* metadata) {
  auto fb_metadata = field->custom_metadata();
  if (fb_metadata != nullptr) {
    RETURN_NOT_OK(KeyValueMetadataFromFlatbuffer(fb_metadata, metadata));
  }
  return Status::OK();
}

static Status FieldFromFlatbuffer(const flatbuf::Field* field,
                                  const DictionaryMemo& dictionary_memo,
                                  std::shared_ptr<Field>* out) {
  std::shared_ptr<DataType> type;

  const flatbuf::DictionaryEncoding* encoding = field->dictionary();

  std::shared_ptr<KeyValueMetadata> metadata;
  RETURN_NOT_OK(GetFieldMetadata(field, &metadata));

  if (encoding == nullptr) {
    // The field is not dictionary encoded. We must potentially visit its
    // children to fully reconstruct the data type
    auto children = field->children();
    std::vector<std::shared_ptr<Field>> child_fields(children->size());
    for (int i = 0; i < static_cast<int>(children->size()); ++i) {
      RETURN_NOT_OK(
          FieldFromFlatbuffer(children->Get(i), dictionary_memo, &child_fields[i]));
    }
    RETURN_NOT_OK(TypeFromFlatbuffer(field, child_fields, metadata.get(), &type));
  } else {
    // The field is dictionary encoded. The type of the dictionary values has
    // been determined elsewhere, and is stored in the DictionaryMemo. Here we
    // construct the logical DictionaryType object

    std::shared_ptr<Array> dictionary;
    RETURN_NOT_OK(dictionary_memo.GetDictionary(encoding->id(), &dictionary));

    std::shared_ptr<DataType> index_type;
    RETURN_NOT_OK(IntFromFlatbuffer(encoding->indexType(), &index_type));
    type = ::arrow::dictionary(index_type, dictionary, encoding->isOrdered());
  }

  *out = std::make_shared<Field>(field->name()->str(), type, field->nullable(), metadata);

  return Status::OK();
}

static Status FieldFromFlatbufferDictionary(const flatbuf::Field* field,
                                            std::shared_ptr<Field>* out) {
  // Need an empty memo to pass down for constructing children
  DictionaryMemo dummy_memo;

  // Any DictionaryEncoding set is ignored here

  std::shared_ptr<DataType> type;
  auto children = field->children();
  std::vector<std::shared_ptr<Field>> child_fields(children->size());
  for (int i = 0; i < static_cast<int>(children->size()); ++i) {
    RETURN_NOT_OK(FieldFromFlatbuffer(children->Get(i), dummy_memo, &child_fields[i]));
  }

  std::shared_ptr<KeyValueMetadata> metadata;
  RETURN_NOT_OK(GetFieldMetadata(field, &metadata));

  RETURN_NOT_OK(TypeFromFlatbuffer(field, child_fields, metadata.get(), &type));
  *out = std::make_shared<Field>(field->name()->str(), type, field->nullable(), metadata);
  return Status::OK();
}

// will return the endianness of the system we are running on
// based the NUMPY_API function. See NOTICE.txt
flatbuf::Endianness endianness() {
  union {
    uint32_t i;
    char c[4];
  } bint = {0x01020304};

  return bint.c[0] == 1 ? flatbuf::Endianness_Big : flatbuf::Endianness_Little;
}

static Status SchemaToFlatbuffer(FBB& fbb, const Schema& schema,
                                 DictionaryMemo* dictionary_memo,
                                 flatbuffers::Offset<flatbuf::Schema>* out) {
  /// Fields
  std::vector<FieldOffset> field_offsets;
  for (int i = 0; i < schema.num_fields(); ++i) {
    FieldOffset offset;
    RETURN_NOT_OK(FieldToFlatbuffer(fbb, *schema.field(i), dictionary_memo, &offset));
    field_offsets.push_back(offset);
  }

  auto fb_offsets = fbb.CreateVector(field_offsets);

  /// Custom metadata
  auto metadata = schema.metadata();

  flatbuffers::Offset<KVVector> fb_custom_metadata;
  std::vector<KeyValueOffset> key_values;
  if (metadata != nullptr) {
    AppendKeyValueMetadata(fbb, *metadata, &key_values);
    fb_custom_metadata = fbb.CreateVector(key_values);
  }
  *out = flatbuf::CreateSchema(fbb, endianness(), fb_offsets, fb_custom_metadata);
  return Status::OK();
}

static Status WriteFBMessage(FBB& fbb, flatbuf::MessageHeader header_type,
                             flatbuffers::Offset<void> header, int64_t body_length,
                             std::shared_ptr<Buffer>* out) {
  auto message = flatbuf::CreateMessage(fbb, kCurrentMetadataVersion, header_type, header,
                                        body_length);
  fbb.Finish(message);
  return WriteFlatbufferBuilder(fbb, out);
}

Status WriteSchemaMessage(const Schema& schema, DictionaryMemo* dictionary_memo,
                          std::shared_ptr<Buffer>* out) {
  FBB fbb;
  flatbuffers::Offset<flatbuf::Schema> fb_schema;
  RETURN_NOT_OK(SchemaToFlatbuffer(fbb, schema, dictionary_memo, &fb_schema));
  return WriteFBMessage(fbb, flatbuf::MessageHeader_Schema, fb_schema.Union(), 0, out);
}

using FieldNodeVector =
    flatbuffers::Offset<flatbuffers::Vector<const flatbuf::FieldNode*>>;
using BufferVector = flatbuffers::Offset<flatbuffers::Vector<const flatbuf::Buffer*>>;

static Status WriteFieldNodes(FBB& fbb, const std::vector<FieldMetadata>& nodes,
                              FieldNodeVector* out) {
  std::vector<flatbuf::FieldNode> fb_nodes;
  fb_nodes.reserve(nodes.size());

  for (size_t i = 0; i < nodes.size(); ++i) {
    const FieldMetadata& node = nodes[i];
    if (node.offset != 0) {
      return Status::Invalid("Field metadata for IPC must have offset 0");
    }
    fb_nodes.emplace_back(node.length, node.null_count);
  }
  *out = fbb.CreateVectorOfStructs(fb_nodes);
  return Status::OK();
}

static Status WriteBuffers(FBB& fbb, const std::vector<BufferMetadata>& buffers,
                           BufferVector* out) {
  std::vector<flatbuf::Buffer> fb_buffers;
  fb_buffers.reserve(buffers.size());

  for (size_t i = 0; i < buffers.size(); ++i) {
    const BufferMetadata& buffer = buffers[i];
    fb_buffers.emplace_back(buffer.offset, buffer.length);
  }
  *out = fbb.CreateVectorOfStructs(fb_buffers);
  return Status::OK();
}

static Status MakeRecordBatch(FBB& fbb, int64_t length, int64_t body_length,
                              const std::vector<FieldMetadata>& nodes,
                              const std::vector<BufferMetadata>& buffers,
                              RecordBatchOffset* offset) {
  FieldNodeVector fb_nodes;
  BufferVector fb_buffers;

  RETURN_NOT_OK(WriteFieldNodes(fbb, nodes, &fb_nodes));
  RETURN_NOT_OK(WriteBuffers(fbb, buffers, &fb_buffers));

  *offset = flatbuf::CreateRecordBatch(fbb, length, fb_nodes, fb_buffers);
  return Status::OK();
}

Status WriteRecordBatchMessage(int64_t length, int64_t body_length,
                               const std::vector<FieldMetadata>& nodes,
                               const std::vector<BufferMetadata>& buffers,
                               std::shared_ptr<Buffer>* out) {
  FBB fbb;
  RecordBatchOffset record_batch;
  RETURN_NOT_OK(MakeRecordBatch(fbb, length, body_length, nodes, buffers, &record_batch));
  return WriteFBMessage(fbb, flatbuf::MessageHeader_RecordBatch, record_batch.Union(),
                        body_length, out);
}

Status WriteTensorMessage(const Tensor& tensor, int64_t buffer_start_offset,
                          std::shared_ptr<Buffer>* out) {
  using TensorDimOffset = flatbuffers::Offset<flatbuf::TensorDim>;
  using TensorOffset = flatbuffers::Offset<flatbuf::Tensor>;

  FBB fbb;

  const auto& type = checked_cast<const FixedWidthType&>(*tensor.type());
  const int elem_size = type.bit_width() / 8;

  flatbuf::Type fb_type_type;
  Offset fb_type;
  RETURN_NOT_OK(TensorTypeToFlatbuffer(fbb, *tensor.type(), &fb_type_type, &fb_type));

  std::vector<TensorDimOffset> dims;
  for (int i = 0; i < tensor.ndim(); ++i) {
    FBString name = fbb.CreateString(tensor.dim_name(i));
    dims.push_back(flatbuf::CreateTensorDim(fbb, tensor.shape()[i], name));
  }

  auto fb_shape = fbb.CreateVector(dims);
  auto fb_strides = fbb.CreateVector(tensor.strides());

  int64_t body_length = tensor.size() * elem_size;
  flatbuf::Buffer buffer(buffer_start_offset, body_length);

  TensorOffset fb_tensor =
      flatbuf::CreateTensor(fbb, fb_type_type, fb_type, fb_shape, fb_strides, &buffer);

  return WriteFBMessage(fbb, flatbuf::MessageHeader_Tensor, fb_tensor.Union(),
                        body_length, out);
}

Status MakeSparseTensorIndexCOO(FBB& fbb, const SparseCOOIndex& sparse_index,
                                const std::vector<BufferMetadata>& buffers,
                                flatbuf::SparseTensorIndex* fb_sparse_index_type,
                                Offset* fb_sparse_index, size_t* num_buffers) {
  *fb_sparse_index_type = flatbuf::SparseTensorIndex_SparseTensorIndexCOO;
  const BufferMetadata& indices_metadata = buffers[0];
  flatbuf::Buffer indices(indices_metadata.offset, indices_metadata.length);
  *fb_sparse_index = flatbuf::CreateSparseTensorIndexCOO(fbb, &indices).Union();
  *num_buffers = 1;
  return Status::OK();
}

Status MakeSparseMatrixIndexCSR(FBB& fbb, const SparseCSRIndex& sparse_index,
                                const std::vector<BufferMetadata>& buffers,
                                flatbuf::SparseTensorIndex* fb_sparse_index_type,
                                Offset* fb_sparse_index, size_t* num_buffers) {
  *fb_sparse_index_type = flatbuf::SparseTensorIndex_SparseMatrixIndexCSR;
  const BufferMetadata& indptr_metadata = buffers[0];
  const BufferMetadata& indices_metadata = buffers[1];
  flatbuf::Buffer indptr(indptr_metadata.offset, indptr_metadata.length);
  flatbuf::Buffer indices(indices_metadata.offset, indices_metadata.length);
  *fb_sparse_index = flatbuf::CreateSparseMatrixIndexCSR(fbb, &indptr, &indices).Union();
  *num_buffers = 2;
  return Status::OK();
}

Status MakeSparseTensorIndex(FBB& fbb, const SparseIndex& sparse_index,
                             const std::vector<BufferMetadata>& buffers,
                             flatbuf::SparseTensorIndex* fb_sparse_index_type,
                             Offset* fb_sparse_index, size_t* num_buffers) {
  switch (sparse_index.format_id()) {
    case SparseTensorFormat::COO:
      RETURN_NOT_OK(MakeSparseTensorIndexCOO(
          fbb, checked_cast<const SparseCOOIndex&>(sparse_index), buffers,
          fb_sparse_index_type, fb_sparse_index, num_buffers));
      break;

    case SparseTensorFormat::CSR:
      RETURN_NOT_OK(MakeSparseMatrixIndexCSR(
          fbb, checked_cast<const SparseCSRIndex&>(sparse_index), buffers,
          fb_sparse_index_type, fb_sparse_index, num_buffers));
      break;

    default:
      std::stringstream ss;
      ss << "Unsupporoted sparse tensor format:: " << sparse_index.ToString()
         << std::endl;
      return Status::NotImplemented(ss.str());
  }

  return Status::OK();
}

Status MakeSparseTensor(FBB& fbb, const SparseTensor& sparse_tensor, int64_t body_length,
                        const std::vector<BufferMetadata>& buffers,
                        SparseTensorOffset* offset) {
  flatbuf::Type fb_type_type;
  Offset fb_type;
  RETURN_NOT_OK(
      TensorTypeToFlatbuffer(fbb, *sparse_tensor.type(), &fb_type_type, &fb_type));

  using TensorDimOffset = flatbuffers::Offset<flatbuf::TensorDim>;
  std::vector<TensorDimOffset> dims;
  for (int i = 0; i < sparse_tensor.ndim(); ++i) {
    FBString name = fbb.CreateString(sparse_tensor.dim_name(i));
    dims.push_back(flatbuf::CreateTensorDim(fbb, sparse_tensor.shape()[i], name));
  }

  auto fb_shape = fbb.CreateVector(dims);

  flatbuf::SparseTensorIndex fb_sparse_index_type;
  Offset fb_sparse_index;
  size_t num_index_buffers = 0;
  RETURN_NOT_OK(MakeSparseTensorIndex(fbb, *sparse_tensor.sparse_index(), buffers,
                                      &fb_sparse_index_type, &fb_sparse_index,
                                      &num_index_buffers));

  const BufferMetadata& data_metadata = buffers[num_index_buffers];
  flatbuf::Buffer data(data_metadata.offset, data_metadata.length);

  const int64_t non_zero_length = sparse_tensor.non_zero_length();

  *offset =
      flatbuf::CreateSparseTensor(fbb, fb_type_type, fb_type, fb_shape, non_zero_length,
                                  fb_sparse_index_type, fb_sparse_index, &data);

  return Status::OK();
}

Status WriteSparseTensorMessage(const SparseTensor& sparse_tensor, int64_t body_length,
                                const std::vector<BufferMetadata>& buffers,
                                std::shared_ptr<Buffer>* out) {
  FBB fbb;
  SparseTensorOffset fb_sparse_tensor;
  RETURN_NOT_OK(
      MakeSparseTensor(fbb, sparse_tensor, body_length, buffers, &fb_sparse_tensor));
  return WriteFBMessage(fbb, flatbuf::MessageHeader_SparseTensor,
                        fb_sparse_tensor.Union(), body_length, out);
}

Status WriteDictionaryMessage(int64_t id, int64_t length, int64_t body_length,
                              const std::vector<FieldMetadata>& nodes,
                              const std::vector<BufferMetadata>& buffers,
                              std::shared_ptr<Buffer>* out) {
  FBB fbb;
  RecordBatchOffset record_batch;
  RETURN_NOT_OK(MakeRecordBatch(fbb, length, body_length, nodes, buffers, &record_batch));
  auto dictionary_batch = flatbuf::CreateDictionaryBatch(fbb, id, record_batch).Union();
  return WriteFBMessage(fbb, flatbuf::MessageHeader_DictionaryBatch, dictionary_batch,
                        body_length, out);
}

static flatbuffers::Offset<flatbuffers::Vector<const flatbuf::Block*>>
FileBlocksToFlatbuffer(FBB& fbb, const std::vector<FileBlock>& blocks) {
  std::vector<flatbuf::Block> fb_blocks;

  for (const FileBlock& block : blocks) {
    fb_blocks.emplace_back(block.offset, block.metadata_length, block.body_length);
  }

  return fbb.CreateVectorOfStructs(fb_blocks);
}

Status WriteFileFooter(const Schema& schema, const std::vector<FileBlock>& dictionaries,
                       const std::vector<FileBlock>& record_batches,
                       io::OutputStream* out) {
  FBB fbb;

  flatbuffers::Offset<flatbuf::Schema> fb_schema;
  DictionaryMemo dictionary_memo;  // unused
  RETURN_NOT_OK(SchemaToFlatbuffer(fbb, schema, &dictionary_memo, &fb_schema));

#ifndef NDEBUG
  for (size_t i = 0; i < dictionaries.size(); ++i) {
    DCHECK(BitUtil::IsMultipleOf8(dictionaries[i].offset)) << i;
    DCHECK(BitUtil::IsMultipleOf8(dictionaries[i].metadata_length)) << i;
    DCHECK(BitUtil::IsMultipleOf8(dictionaries[i].body_length)) << i;
  }

  for (size_t i = 0; i < record_batches.size(); ++i) {
    DCHECK(BitUtil::IsMultipleOf8(record_batches[i].offset)) << i;
    DCHECK(BitUtil::IsMultipleOf8(record_batches[i].metadata_length)) << i;
    DCHECK(BitUtil::IsMultipleOf8(record_batches[i].body_length)) << i;
  }
#endif

  auto fb_dictionaries = FileBlocksToFlatbuffer(fbb, dictionaries);
  auto fb_record_batches = FileBlocksToFlatbuffer(fbb, record_batches);

  auto footer = flatbuf::CreateFooter(fbb, kCurrentMetadataVersion, fb_schema,
                                      fb_dictionaries, fb_record_batches);

  fbb.Finish(footer);

  int32_t size = fbb.GetSize();

  return out->Write(fbb.GetBufferPointer(), size);
}

// ----------------------------------------------------------------------

static Status VisitField(const flatbuf::Field* field, DictionaryTypeMap* id_to_field) {
  const flatbuf::DictionaryEncoding* dict_metadata = field->dictionary();
  if (dict_metadata == nullptr) {
    // Field is not dictionary encoded. Visit children
    auto children = field->children();
    if (children == nullptr) {
      return Status::IOError("Children-pointer of flatbuffer-encoded Field is null.");
    }
    for (flatbuffers::uoffset_t i = 0; i < children->size(); ++i) {
      RETURN_NOT_OK(VisitField(children->Get(i), id_to_field));
    }
  } else {
    // Field is dictionary encoded. Construct the data type for the
    // dictionary (no descendents can be dictionary encoded)
    std::shared_ptr<Field> dictionary_field;
    RETURN_NOT_OK(FieldFromFlatbufferDictionary(field, &dictionary_field));
    (*id_to_field)[dict_metadata->id()] = dictionary_field;
  }
  return Status::OK();
}

Status GetDictionaryTypes(const void* opaque_schema, DictionaryTypeMap* id_to_field) {
  auto schema = static_cast<const flatbuf::Schema*>(opaque_schema);
  if (schema->fields() == nullptr) {
    return Status::IOError("Fields-pointer of flatbuffer-encoded Schema is null.");
  }
  int num_fields = static_cast<int>(schema->fields()->size());
  for (int i = 0; i < num_fields; ++i) {
    auto field = schema->fields()->Get(i);
    if (field == nullptr) {
      return Status::IOError("Field-pointer of flatbuffer-encoded Schema is null.");
    }
    RETURN_NOT_OK(VisitField(field, id_to_field));
  }
  return Status::OK();
}

Status GetSchema(const void* opaque_schema, const DictionaryMemo& dictionary_memo,
                 std::shared_ptr<Schema>* out) {
  auto schema = static_cast<const flatbuf::Schema*>(opaque_schema);
  if (schema->fields() == nullptr) {
    return Status::IOError("Fields-pointer of flatbuffer-encoded Schema is null.");
  }
  int num_fields = static_cast<int>(schema->fields()->size());

  std::vector<std::shared_ptr<Field>> fields(num_fields);
  for (int i = 0; i < num_fields; ++i) {
    const flatbuf::Field* field = schema->fields()->Get(i);
    RETURN_NOT_OK(FieldFromFlatbuffer(field, dictionary_memo, &fields[i]));
  }

  auto fb_metadata = schema->custom_metadata();
  std::shared_ptr<KeyValueMetadata> metadata;

  if (fb_metadata != nullptr) {
    RETURN_NOT_OK(KeyValueMetadataFromFlatbuffer(fb_metadata, &metadata));
  }

  *out = ::arrow::schema(std::move(fields), metadata);

  return Status::OK();
}

Status GetTensorMetadata(const Buffer& metadata, std::shared_ptr<DataType>* type,
                         std::vector<int64_t>* shape, std::vector<int64_t>* strides,
                         std::vector<std::string>* dim_names) {
  auto message = flatbuf::GetMessage(metadata.data());
  auto tensor = reinterpret_cast<const flatbuf::Tensor*>(message->header());

  int ndim = static_cast<int>(tensor->shape()->size());

  for (int i = 0; i < ndim; ++i) {
    auto dim = tensor->shape()->Get(i);

    shape->push_back(dim->size());
    auto fb_name = dim->name();
    if (fb_name == 0) {
      dim_names->push_back("");
    } else {
      dim_names->push_back(fb_name->str());
    }
  }

  if (tensor->strides()->size() > 0) {
    for (int i = 0; i < ndim; ++i) {
      strides->push_back(tensor->strides()->Get(i));
    }
  }

  return ConcreteTypeFromFlatbuffer(tensor->type_type(), tensor->type(), {}, type);
}

Status GetSparseTensorMetadata(const Buffer& metadata, std::shared_ptr<DataType>* type,
                               std::vector<int64_t>* shape,
                               std::vector<std::string>* dim_names,
                               int64_t* non_zero_length,
                               SparseTensorFormat::type* sparse_tensor_format_id) {
  auto message = flatbuf::GetMessage(metadata.data());
  if (message->header_type() != flatbuf::MessageHeader_SparseTensor) {
    return Status::IOError("Header of flatbuffer-encoded Message is not SparseTensor.");
  }
  if (message->header() == nullptr) {
    return Status::IOError("Header-pointer of flatbuffer-encoded Message is null.");
  }

  auto sparse_tensor = reinterpret_cast<const flatbuf::SparseTensor*>(message->header());
  int ndim = static_cast<int>(sparse_tensor->shape()->size());

  for (int i = 0; i < ndim; ++i) {
    auto dim = sparse_tensor->shape()->Get(i);

    shape->push_back(dim->size());
    auto fb_name = dim->name();
    if (fb_name == 0) {
      dim_names->push_back("");
    } else {
      dim_names->push_back(fb_name->str());
    }
  }

  *non_zero_length = sparse_tensor->non_zero_length();

  switch (sparse_tensor->sparseIndex_type()) {
    case flatbuf::SparseTensorIndex_SparseTensorIndexCOO:
      *sparse_tensor_format_id = SparseTensorFormat::COO;
      break;

    case flatbuf::SparseTensorIndex_SparseMatrixIndexCSR:
      *sparse_tensor_format_id = SparseTensorFormat::CSR;
      break;

    default:
      return Status::Invalid("Unrecognized sparse index type");
  }

  return ConcreteTypeFromFlatbuffer(sparse_tensor->type_type(), sparse_tensor->type(), {},
                                    type);
}

// ----------------------------------------------------------------------
// Implement message writing

Status WriteMessage(const Buffer& message, int32_t alignment, io::OutputStream* file,
                    int32_t* message_length) {
  // ARROW-3212: We do not make assumptions that the output stream is aligned
  int32_t padded_message_length = static_cast<int32_t>(message.size()) + 4;
  const int32_t remainder = padded_message_length % alignment;
  if (remainder != 0) {
    padded_message_length += alignment - remainder;
  }

  // The returned message size includes the length prefix, the flatbuffer,
  // plus padding
  *message_length = padded_message_length;

  // Write the flatbuffer size prefix including padding
  int32_t flatbuffer_size = padded_message_length - 4;
  RETURN_NOT_OK(file->Write(&flatbuffer_size, sizeof(int32_t)));

  // Write the flatbuffer
  RETURN_NOT_OK(file->Write(message.data(), message.size()));

  // Write any padding
  int32_t padding = padded_message_length - static_cast<int32_t>(message.size()) - 4;
  if (padding > 0) {
    RETURN_NOT_OK(file->Write(kPaddingBytes, padding));
  }

  return Status::OK();
}

}  // namespace internal
}  // namespace ipc
}  // namespace arrow
