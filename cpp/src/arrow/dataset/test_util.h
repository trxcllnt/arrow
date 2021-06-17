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

#pragma once

#include <algorithm>
#include <ciso646>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/compute/exec/expression.h"
#include "arrow/dataset/dataset_internal.h"
#include "arrow/dataset/discovery.h"
#include "arrow/dataset/file_base.h"
#include "arrow/dataset/scanner_internal.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/mockfs.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/io_util.h"
#include "arrow/util/iterator.h"
#include "arrow/util/logging.h"
#include "arrow/util/make_unique.h"
#include "arrow/util/thread_pool.h"

namespace arrow {
namespace dataset {

using compute::call;
using compute::field_ref;
using compute::literal;

using compute::and_;
using compute::equal;
using compute::greater;
using compute::greater_equal;
using compute::is_null;
using compute::is_valid;
using compute::less;
using compute::less_equal;
using compute::not_;
using compute::not_equal;
using compute::or_;
using compute::project;

using fs::internal::GetAbstractPathExtension;
using internal::checked_cast;
using internal::checked_pointer_cast;
using internal::TemporaryDir;

class FileSourceFixtureMixin : public ::testing::Test {
 public:
  std::unique_ptr<FileSource> GetSource(std::shared_ptr<Buffer> buffer) {
    return internal::make_unique<FileSource>(std::move(buffer));
  }
};

template <typename Gen>
class GeneratedRecordBatch : public RecordBatchReader {
 public:
  GeneratedRecordBatch(std::shared_ptr<Schema> schema, Gen gen)
      : schema_(std::move(schema)), gen_(gen) {}

  std::shared_ptr<Schema> schema() const override { return schema_; }

  Status ReadNext(std::shared_ptr<RecordBatch>* batch) override { return gen_(batch); }

 private:
  std::shared_ptr<Schema> schema_;
  Gen gen_;
};

template <typename Gen>
std::unique_ptr<GeneratedRecordBatch<Gen>> MakeGeneratedRecordBatch(
    std::shared_ptr<Schema> schema, Gen&& gen) {
  return internal::make_unique<GeneratedRecordBatch<Gen>>(schema, std::forward<Gen>(gen));
}

std::unique_ptr<RecordBatchReader> MakeGeneratedRecordBatch(
    std::shared_ptr<Schema> schema, int64_t batch_size, int64_t batch_repetitions) {
  auto batch = random::GenerateBatch(schema->fields(), batch_size, /*seed=*/0);
  int64_t i = 0;
  return MakeGeneratedRecordBatch(
      schema, [batch, i, batch_repetitions](std::shared_ptr<RecordBatch>* out) mutable {
        *out = i++ < batch_repetitions ? batch : nullptr;
        return Status::OK();
      });
}

void EnsureRecordBatchReaderDrained(RecordBatchReader* reader) {
  ASSERT_OK_AND_ASSIGN(auto batch, reader->Next());
  EXPECT_EQ(batch, nullptr);
}

/// Test dataset that returns one or more fragments.
class FragmentDataset : public Dataset {
 public:
  FragmentDataset(std::shared_ptr<Schema> schema, FragmentVector fragments)
      : Dataset(std::move(schema)), fragments_(std::move(fragments)) {}

  std::string type_name() const override { return "fragment"; }

  Result<std::shared_ptr<Dataset>> ReplaceSchema(std::shared_ptr<Schema>) const override {
    return Status::NotImplemented("");
  }

 protected:
  Result<FragmentIterator> GetFragmentsImpl(compute::Expression predicate) override {
    return MakeVectorIterator(fragments_);
  }
  FragmentVector fragments_;
};

class DatasetFixtureMixin : public ::testing::Test {
 public:
  /// \brief Ensure that record batches found in reader are equals to the
  /// record batches yielded by the data fragment.
  void AssertScanTaskEquals(RecordBatchReader* expected, ScanTask* task,
                            bool ensure_drained = true) {
    ASSERT_OK_AND_ASSIGN(auto it, task->Execute());
    ARROW_EXPECT_OK(it.Visit([expected](std::shared_ptr<RecordBatch> rhs) -> Status {
      std::shared_ptr<RecordBatch> lhs;
      RETURN_NOT_OK(expected->ReadNext(&lhs));
      EXPECT_NE(lhs, nullptr);
      AssertBatchesEqual(*lhs, *rhs);
      return Status::OK();
    }));

    if (ensure_drained) {
      EnsureRecordBatchReaderDrained(expected);
    }
  }

  /// \brief Assert the value of the next batch yielded by the reader
  void AssertBatchEquals(RecordBatchReader* expected, const RecordBatch& batch) {
    std::shared_ptr<RecordBatch> lhs;
    ASSERT_OK(expected->ReadNext(&lhs));
    EXPECT_NE(lhs, nullptr);
    AssertBatchesEqual(*lhs, batch);
  }

  /// \brief Ensure that record batches found in reader are equals to the
  /// record batches yielded by the data fragment.
  void AssertFragmentEquals(RecordBatchReader* expected, Fragment* fragment,
                            bool ensure_drained = true) {
    ASSERT_OK_AND_ASSIGN(auto it, fragment->Scan(options_));

    ARROW_EXPECT_OK(it.Visit([&](std::shared_ptr<ScanTask> task) -> Status {
      AssertScanTaskEquals(expected, task.get(), false);
      return Status::OK();
    }));

    if (ensure_drained) {
      EnsureRecordBatchReaderDrained(expected);
    }
  }

  /// \brief Ensure that record batches found in reader are equals to the
  /// record batches yielded by the data fragments of a dataset.
  void AssertDatasetFragmentsEqual(RecordBatchReader* expected, Dataset* dataset,
                                   bool ensure_drained = true) {
    ASSERT_OK_AND_ASSIGN(auto predicate, options_->filter.Bind(*dataset->schema()));
    ASSERT_OK_AND_ASSIGN(auto it, dataset->GetFragments(predicate));

    ARROW_EXPECT_OK(it.Visit([&](std::shared_ptr<Fragment> fragment) -> Status {
      AssertFragmentEquals(expected, fragment.get(), false);
      return Status::OK();
    }));

    if (ensure_drained) {
      EnsureRecordBatchReaderDrained(expected);
    }
  }

  /// \brief Ensure that record batches found in reader are equals to the
  /// record batches yielded by a scanner.
  void AssertScannerEquals(RecordBatchReader* expected, Scanner* scanner,
                           bool ensure_drained = true) {
    ASSERT_OK_AND_ASSIGN(auto it, scanner->ScanBatches());

    ARROW_EXPECT_OK(it.Visit([&](TaggedRecordBatch batch) -> Status {
      std::shared_ptr<RecordBatch> lhs;
      RETURN_NOT_OK(expected->ReadNext(&lhs));
      EXPECT_NE(lhs, nullptr);
      AssertBatchesEqual(*lhs, *batch.record_batch);
      return Status::OK();
    }));

    if (ensure_drained) {
      EnsureRecordBatchReaderDrained(expected);
    }
  }

  /// \brief Ensure that record batches found in reader are equals to the
  /// record batches yielded by a scanner.
  void AssertScanBatchesEquals(RecordBatchReader* expected, Scanner* scanner,
                               bool ensure_drained = true) {
    ASSERT_OK_AND_ASSIGN(auto it, scanner->ScanBatches());

    ARROW_EXPECT_OK(it.Visit([&](TaggedRecordBatch batch) -> Status {
      AssertBatchEquals(expected, *batch.record_batch);
      return Status::OK();
    }));

    if (ensure_drained) {
      EnsureRecordBatchReaderDrained(expected);
    }
  }

  /// \brief Ensure that record batches found in reader are equals to the
  /// record batches yielded by a scanner.
  void AssertScanBatchesUnorderedEquals(RecordBatchReader* expected, Scanner* scanner,
                                        int expected_batches_per_fragment,
                                        bool ensure_drained = true) {
    ASSERT_OK_AND_ASSIGN(auto it, scanner->ScanBatchesUnordered());

    int fragment_counter = 0;
    bool saw_last_fragment = false;
    int batch_counter = 0;
    auto visitor = [&](EnumeratedRecordBatch batch) -> Status {
      if (batch_counter == 0) {
        EXPECT_FALSE(saw_last_fragment);
      }
      EXPECT_EQ(batch_counter++, batch.record_batch.index);
      auto last_batch = batch_counter == expected_batches_per_fragment;
      EXPECT_EQ(last_batch, batch.record_batch.last);
      EXPECT_EQ(fragment_counter, batch.fragment.index);
      if (last_batch) {
        fragment_counter++;
        batch_counter = 0;
      }
      saw_last_fragment = batch.fragment.last;
      AssertBatchEquals(expected, *batch.record_batch.value);
      return Status::OK();
    };
    ARROW_EXPECT_OK(it.Visit(visitor));

    if (ensure_drained) {
      EnsureRecordBatchReaderDrained(expected);
    }
  }

  /// \brief Ensure that record batches found in reader are equals to the
  /// record batches yielded by a dataset.
  void AssertDatasetEquals(RecordBatchReader* expected, Dataset* dataset,
                           bool ensure_drained = true) {
    ASSERT_OK_AND_ASSIGN(auto builder, dataset->NewScan());
    ASSERT_OK_AND_ASSIGN(auto scanner, builder->Finish());
    AssertScannerEquals(expected, scanner.get());

    if (ensure_drained) {
      EnsureRecordBatchReaderDrained(expected);
    }
  }

 protected:
  void SetSchema(std::vector<std::shared_ptr<Field>> fields) {
    schema_ = schema(std::move(fields));
    options_ = std::make_shared<ScanOptions>();
    options_->dataset_schema = schema_;
    ASSERT_OK(SetProjection(options_.get(), schema_->field_names()));
    SetFilter(literal(true));
  }

  void SetFilter(compute::Expression filter) {
    ASSERT_OK_AND_ASSIGN(options_->filter, filter.Bind(*schema_));
  }

  void SetProjectedColumns(std::vector<std::string> column_names) {
    ASSERT_OK(SetProjection(options_.get(), std::move(column_names)));
  }

  std::shared_ptr<Schema> schema_;
  std::shared_ptr<ScanOptions> options_;
};

template <typename P>
class DatasetFixtureMixinWithParam : public DatasetFixtureMixin,
                                     public ::testing::WithParamInterface<P> {};

struct TestFormatParams {
  bool use_async;
  bool use_threads;
  int num_batches;
  int items_per_batch;

  int64_t expected_rows() const { return num_batches * items_per_batch; }

  std::string ToString() const {
    // GTest requires this to be alphanumeric
    std::stringstream ss;
    ss << (use_async ? "Async" : "Sync") << (use_threads ? "Threaded" : "Serial")
       << num_batches << "b" << items_per_batch << "r";
    return ss.str();
  }

  static std::string ToTestNameString(
      const ::testing::TestParamInfo<TestFormatParams>& info) {
    return std::to_string(info.index) + info.param.ToString();
  }

  static std::vector<TestFormatParams> Values() {
    std::vector<TestFormatParams> values;
    for (const bool async : std::vector<bool>{true, false}) {
      for (const bool use_threads : std::vector<bool>{true, false}) {
        values.push_back(TestFormatParams{async, use_threads, 16, 1024});
      }
    }
    return values;
  }
};

std::ostream& operator<<(std::ostream& out, const TestFormatParams& params) {
  out << params.ToString();
  return out;
}

class FileFormatWriterMixin {
  virtual std::shared_ptr<Buffer> Write(RecordBatchReader* reader) = 0;
  virtual std::shared_ptr<Buffer> Write(const Table& table) = 0;
};

/// FormatHelper should be a class with these static methods:
/// std::shared_ptr<Buffer> Write(RecordBatchReader* reader);
/// std::shared_ptr<FileFormat> MakeFormat();
template <typename FormatHelper>
class FileFormatFixtureMixin : public ::testing::Test {
 public:
  constexpr static int64_t kBatchSize = 1UL << 12;
  constexpr static int64_t kBatchRepetitions = 1 << 5;

  FileFormatFixtureMixin()
      : format_(FormatHelper::MakeFormat()), opts_(std::make_shared<ScanOptions>()) {}

  int64_t expected_batches() const { return kBatchRepetitions; }
  int64_t expected_rows() const { return kBatchSize * kBatchRepetitions; }

  std::shared_ptr<FileFragment> MakeFragment(const FileSource& source) {
    EXPECT_OK_AND_ASSIGN(auto fragment, format_->MakeFragment(source));
    return fragment;
  }

  std::shared_ptr<FileFragment> MakeFragment(const FileSource& source,
                                             compute::Expression partition_expression) {
    EXPECT_OK_AND_ASSIGN(auto fragment,
                         format_->MakeFragment(source, partition_expression));
    return fragment;
  }

  std::shared_ptr<FileSource> GetFileSource(RecordBatchReader* reader) {
    EXPECT_OK_AND_ASSIGN(auto buffer, FormatHelper::Write(reader));
    return std::make_shared<FileSource>(std::move(buffer));
  }

  virtual std::shared_ptr<RecordBatchReader> GetRecordBatchReader(
      std::shared_ptr<Schema> schema) {
    return MakeGeneratedRecordBatch(schema, kBatchSize, kBatchRepetitions);
  }

  Result<std::shared_ptr<io::BufferOutputStream>> GetFileSink() {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ResizableBuffer> buffer,
                          AllocateResizableBuffer(0));
    return std::make_shared<io::BufferOutputStream>(buffer);
  }

  void SetSchema(std::vector<std::shared_ptr<Field>> fields) {
    opts_->dataset_schema = schema(std::move(fields));
    ASSERT_OK(SetProjection(opts_.get(), opts_->dataset_schema->field_names()));
  }

  void SetFilter(compute::Expression filter) {
    ASSERT_OK_AND_ASSIGN(opts_->filter, filter.Bind(*opts_->dataset_schema));
  }

  void Project(std::vector<std::string> names) {
    ASSERT_OK(SetProjection(opts_.get(), std::move(names)));
  }

  // Shared test cases
  void TestInspectFailureWithRelevantError(StatusCode code) {
    std::shared_ptr<Buffer> buf = std::make_shared<Buffer>(util::string_view(""));
    auto result = format_->Inspect(FileSource(buf));
    EXPECT_EQ(code, result.status().code());
    EXPECT_THAT(result.status().ToString(), testing::HasSubstr("<Buffer>"));

    constexpr auto file_name = "herp/derp";
    ASSERT_OK_AND_ASSIGN(
        auto fs, fs::internal::MockFileSystem::Make(fs::kNoTime, {fs::File(file_name)}));
    result = format_->Inspect({file_name, fs});
    EXPECT_EQ(code, result.status().code());
    EXPECT_THAT(result.status().ToString(), testing::HasSubstr(file_name));
  }
  void TestInspect() {
    auto reader = GetRecordBatchReader(schema({field("f64", float64())}));
    auto source = GetFileSource(reader.get());

    ASSERT_OK_AND_ASSIGN(auto actual, format_->Inspect(*source.get()));
    AssertSchemaEqual(*actual, *reader->schema(), /*check_metadata=*/false);
  }
  void TestIsSupported() {
    auto reader = GetRecordBatchReader(schema({field("f64", float64())}));
    auto source = GetFileSource(reader.get());

    bool supported = false;

    std::shared_ptr<Buffer> buf = std::make_shared<Buffer>(util::string_view(""));
    ASSERT_OK_AND_ASSIGN(supported, format_->IsSupported(FileSource(buf)));
    ASSERT_EQ(supported, false);

    buf = std::make_shared<Buffer>(util::string_view("corrupted"));
    ASSERT_OK_AND_ASSIGN(supported, format_->IsSupported(FileSource(buf)));
    ASSERT_EQ(supported, false);

    ASSERT_OK_AND_ASSIGN(supported, format_->IsSupported(*source));
    EXPECT_EQ(supported, true);
  }
  std::shared_ptr<Buffer> WriteToBuffer(
      std::shared_ptr<Schema> schema,
      std::shared_ptr<FileWriteOptions> options = nullptr) {
    auto format = format_;
    SetSchema(schema->fields());
    EXPECT_OK_AND_ASSIGN(auto sink, GetFileSink());

    if (!options) options = format->DefaultWriteOptions();
    EXPECT_OK_AND_ASSIGN(auto writer, format->MakeWriter(sink, schema, options));
    ARROW_EXPECT_OK(writer->Write(GetRecordBatchReader(schema).get()));
    ARROW_EXPECT_OK(writer->Finish());
    EXPECT_OK_AND_ASSIGN(auto written, sink->Finish());
    return written;
  }
  void TestWrite() {
    auto reader = this->GetRecordBatchReader(schema({field("f64", float64())}));
    auto source = this->GetFileSource(reader.get());
    auto written = this->WriteToBuffer(reader->schema());
    AssertBufferEqual(*written, *source->buffer());
  }
  void TestCountRows() {
    auto options = std::make_shared<ScanOptions>();
    auto reader = this->GetRecordBatchReader(schema({field("f64", float64())}));
    auto full_schema = schema({field("f64", float64()), field("part", int64())});
    auto source = this->GetFileSource(reader.get());

    auto fragment = this->MakeFragment(*source);
    ASSERT_FINISHES_OK_AND_EQ(util::make_optional<int64_t>(expected_rows()),
                              fragment->CountRows(literal(true), options));

    fragment = this->MakeFragment(*source, equal(field_ref("part"), literal(2)));
    ASSERT_FINISHES_OK_AND_EQ(util::make_optional<int64_t>(expected_rows()),
                              fragment->CountRows(literal(true), options));

    auto predicate = equal(field_ref("part"), literal(1));
    ASSERT_OK_AND_ASSIGN(predicate, predicate.Bind(*full_schema));
    ASSERT_FINISHES_OK_AND_EQ(util::make_optional<int64_t>(0),
                              fragment->CountRows(predicate, options));

    predicate = equal(field_ref("part"), literal(2));
    ASSERT_OK_AND_ASSIGN(predicate, predicate.Bind(*full_schema));
    ASSERT_FINISHES_OK_AND_EQ(util::make_optional<int64_t>(expected_rows()),
                              fragment->CountRows(predicate, options));

    predicate = equal(call("add", {field_ref("f64"), literal(3)}), literal(2));
    ASSERT_OK_AND_ASSIGN(predicate, predicate.Bind(*full_schema));
    ASSERT_FINISHES_OK_AND_EQ(util::nullopt, fragment->CountRows(predicate, options));
  }

 protected:
  std::shared_ptr<typename FormatHelper::FormatType> format_;
  std::shared_ptr<ScanOptions> opts_;
};

template <typename FormatHelper>
class FileFormatScanMixin : public FileFormatFixtureMixin<FormatHelper>,
                            public ::testing::WithParamInterface<TestFormatParams> {
 public:
  int64_t expected_batches() const { return GetParam().num_batches; }
  int64_t expected_rows() const { return GetParam().expected_rows(); }

  std::shared_ptr<RecordBatchReader> GetRecordBatchReader(
      std::shared_ptr<Schema> schema) override {
    return MakeGeneratedRecordBatch(schema, GetParam().items_per_batch,
                                    GetParam().num_batches);
  }

  // Scan the fragment through the scanner.
  RecordBatchIterator Batches(std::shared_ptr<Fragment> fragment) {
    EXPECT_OK_AND_ASSIGN(auto schema, fragment->ReadPhysicalSchema());
    auto dataset = std::make_shared<FragmentDataset>(schema, FragmentVector{fragment});
    ScannerBuilder builder(dataset, opts_);
    ARROW_EXPECT_OK(builder.UseAsync(GetParam().use_async));
    ARROW_EXPECT_OK(builder.UseThreads(GetParam().use_threads));
    EXPECT_OK_AND_ASSIGN(auto scanner, builder.Finish());
    EXPECT_OK_AND_ASSIGN(auto batch_it, scanner->ScanBatches());
    return MakeMapIterator([](TaggedRecordBatch tagged) { return tagged.record_batch; },
                           std::move(batch_it));
  }

  // Scan the fragment directly, without using the scanner.
  RecordBatchIterator PhysicalBatches(std::shared_ptr<Fragment> fragment) {
    opts_->use_threads = GetParam().use_threads;
    if (GetParam().use_async) {
      EXPECT_OK_AND_ASSIGN(auto batch_gen, fragment->ScanBatchesAsync(opts_));
      EXPECT_OK_AND_ASSIGN(auto batch_it, MakeGeneratorIterator(std::move(batch_gen)));
      return batch_it;
    }
    EXPECT_OK_AND_ASSIGN(auto scan_task_it, fragment->Scan(opts_));
    return MakeFlattenIterator(MakeMaybeMapIterator(
        [](std::shared_ptr<ScanTask> scan_task) { return scan_task->Execute(); },
        std::move(scan_task_it)));
  }

  // Shared test cases
  void TestScan() {
    auto reader = GetRecordBatchReader(schema({field("f64", float64())}));
    auto source = this->GetFileSource(reader.get());

    this->SetSchema(reader->schema()->fields());
    auto fragment = this->MakeFragment(*source);

    int64_t row_count = 0;
    for (auto maybe_batch : Batches(fragment)) {
      ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
      row_count += batch->num_rows();
    }
    ASSERT_EQ(row_count, GetParam().expected_rows());
  }
  // Ensure file formats only return columns needed to fulfill filter/projection
  void TestScanProjected() {
    auto f32 = field("f32", float32());
    auto f64 = field("f64", float64());
    auto i32 = field("i32", int32());
    auto i64 = field("i64", int64());
    this->SetSchema({f64, i64, f32, i32});
    this->Project({"f64"});
    this->SetFilter(equal(field_ref("i32"), literal(0)));

    // NB: projection is applied by the scanner; FileFragment does not evaluate it so
    // we will not drop "i32" even though it is not projected since we need it for
    // filtering
    auto expected_schema = schema({f64, i32});

    auto reader = this->GetRecordBatchReader(opts_->dataset_schema);
    auto source = this->GetFileSource(reader.get());
    auto fragment = this->MakeFragment(*source);

    int64_t row_count = 0;

    for (auto maybe_batch : PhysicalBatches(fragment)) {
      ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
      row_count += batch->num_rows();
      AssertSchemaEqual(*batch->schema(), *expected_schema,
                        /*check_metadata=*/false);
    }

    ASSERT_EQ(row_count, expected_rows());
  }
  void TestScanProjectedMissingCols() {
    auto f32 = field("f32", float32());
    auto f64 = field("f64", float64());
    auto i32 = field("i32", int32());
    auto i64 = field("i64", int64());
    this->SetSchema({f64, i64, f32, i32});
    this->Project({"f64"});
    this->SetFilter(equal(field_ref("i32"), literal(0)));

    auto reader_without_i32 = this->GetRecordBatchReader(schema({f64, i64, f32}));
    auto reader_without_f64 = this->GetRecordBatchReader(schema({i64, f32, i32}));
    auto reader = this->GetRecordBatchReader(schema({f64, i64, f32, i32}));

    auto readers = {reader.get(), reader_without_i32.get(), reader_without_f64.get()};
    for (auto reader : readers) {
      SCOPED_TRACE(reader->schema()->ToString());
      auto source = this->GetFileSource(reader);
      auto fragment = this->MakeFragment(*source);

      // NB: projection is applied by the scanner; FileFragment does not evaluate it so
      // we will not drop "i32" even though it is not projected since we need it for
      // filtering
      //
      // in the case where a file doesn't contain a referenced field, we won't
      // materialize it as nulls later
      std::shared_ptr<Schema> expected_schema;
      if (reader == reader_without_i32.get()) {
        expected_schema = schema({f64});
      } else if (reader == reader_without_f64.get()) {
        expected_schema = schema({i32});
      } else {
        expected_schema = schema({f64, i32});
      }

      int64_t row_count = 0;
      for (auto maybe_batch : PhysicalBatches(fragment)) {
        ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
        row_count += batch->num_rows();
        AssertSchemaEqual(*batch->schema(), *expected_schema,
                          /*check_metadata=*/false);
      }
      ASSERT_EQ(row_count, expected_rows());
    }
  }
  void TestScanWithVirtualColumn() {
    auto reader = this->GetRecordBatchReader(schema({field("f64", float64())}));
    auto source = this->GetFileSource(reader.get());
    // NB: dataset_schema includes a column not present in the file
    this->SetSchema({reader->schema()->field(0), field("virtual", int32())});
    auto fragment = this->MakeFragment(*source);

    ASSERT_OK_AND_ASSIGN(auto physical_schema, fragment->ReadPhysicalSchema());
    AssertSchemaEqual(Schema({field("f64", float64())}), *physical_schema);
    {
      int64_t row_count = 0;
      for (auto maybe_batch : Batches(fragment)) {
        ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
        AssertSchemaEqual(*batch->schema(), *opts_->projected_schema);
        row_count += batch->num_rows();
      }
      ASSERT_EQ(row_count, expected_rows());
    }
    {
      int64_t row_count = 0;
      for (auto maybe_batch : PhysicalBatches(fragment)) {
        ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
        AssertSchemaEqual(*batch->schema(), *physical_schema);
        row_count += batch->num_rows();
      }
      ASSERT_EQ(row_count, expected_rows());
    }
  }

 protected:
  using FileFormatFixtureMixin<FormatHelper>::opts_;
};

/// \brief A dummy FileFormat implementation
class DummyFileFormat : public FileFormat {
 public:
  explicit DummyFileFormat(std::shared_ptr<Schema> schema = NULLPTR)
      : schema_(std::move(schema)) {}

  std::string type_name() const override { return "dummy"; }

  bool Equals(const FileFormat& other) const override {
    return type_name() == other.type_name() &&
           schema_->Equals(checked_cast<const DummyFileFormat&>(other).schema_);
  }

  Result<bool> IsSupported(const FileSource& source) const override { return true; }

  Result<std::shared_ptr<Schema>> Inspect(const FileSource& source) const override {
    return schema_;
  }

  /// \brief Open a file for scanning (always returns an empty iterator)
  Result<ScanTaskIterator> ScanFile(
      const std::shared_ptr<ScanOptions>& options,
      const std::shared_ptr<FileFragment>& fragment) const override {
    return MakeEmptyIterator<std::shared_ptr<ScanTask>>();
  }

  Result<std::shared_ptr<FileWriter>> MakeWriter(
      std::shared_ptr<io::OutputStream> destination, std::shared_ptr<Schema> schema,
      std::shared_ptr<FileWriteOptions> options) const override {
    return Status::NotImplemented("writing fragment of DummyFileFormat");
  }

  std::shared_ptr<FileWriteOptions> DefaultWriteOptions() override { return nullptr; }

 protected:
  std::shared_ptr<Schema> schema_;
};

class JSONRecordBatchFileFormat : public FileFormat {
 public:
  using SchemaResolver = std::function<std::shared_ptr<Schema>(const FileSource&)>;

  explicit JSONRecordBatchFileFormat(std::shared_ptr<Schema> schema)
      : resolver_([schema](const FileSource&) { return schema; }) {}

  explicit JSONRecordBatchFileFormat(SchemaResolver resolver)
      : resolver_(std::move(resolver)) {}

  bool Equals(const FileFormat& other) const override { return this == &other; }

  std::string type_name() const override { return "json_record_batch"; }

  /// \brief Return true if the given file extension
  Result<bool> IsSupported(const FileSource& source) const override { return true; }

  Result<std::shared_ptr<Schema>> Inspect(const FileSource& source) const override {
    return resolver_(source);
  }

  /// \brief Open a file for scanning
  Result<ScanTaskIterator> ScanFile(
      const std::shared_ptr<ScanOptions>& options,
      const std::shared_ptr<FileFragment>& fragment) const override {
    ARROW_ASSIGN_OR_RAISE(auto file, fragment->source().Open());
    ARROW_ASSIGN_OR_RAISE(int64_t size, file->GetSize());
    ARROW_ASSIGN_OR_RAISE(auto buffer, file->Read(size));

    util::string_view view{*buffer};

    ARROW_ASSIGN_OR_RAISE(auto schema, Inspect(fragment->source()));
    std::shared_ptr<RecordBatch> batch = RecordBatchFromJSON(schema, view);
    return ScanTaskIteratorFromRecordBatch({batch}, std::move(options));
  }

  Result<std::shared_ptr<FileWriter>> MakeWriter(
      std::shared_ptr<io::OutputStream> destination, std::shared_ptr<Schema> schema,
      std::shared_ptr<FileWriteOptions> options) const override {
    return Status::NotImplemented("writing fragment of JSONRecordBatchFileFormat");
  }

  std::shared_ptr<FileWriteOptions> DefaultWriteOptions() override { return nullptr; }

 protected:
  SchemaResolver resolver_;
};

struct MakeFileSystemDatasetMixin {
  std::vector<fs::FileInfo> ParsePathList(const std::string& pathlist) {
    std::vector<fs::FileInfo> infos;

    std::stringstream ss(pathlist);
    std::string line;
    while (std::getline(ss, line)) {
      auto start = line.find_first_not_of(" \n\r\t");
      if (start == std::string::npos) {
        continue;
      }
      line.erase(0, start);

      if (line.front() == '#') {
        continue;
      }

      if (line.back() == '/') {
        infos.push_back(fs::Dir(line));
        continue;
      }

      infos.push_back(fs::File(line));
    }

    return infos;
  }

  void MakeFileSystem(const std::vector<fs::FileInfo>& infos) {
    ASSERT_OK_AND_ASSIGN(fs_, fs::internal::MockFileSystem::Make(fs::kNoTime, infos));
  }

  void MakeFileSystem(const std::vector<std::string>& paths) {
    std::vector<fs::FileInfo> infos{paths.size()};
    std::transform(paths.cbegin(), paths.cend(), infos.begin(),
                   [](const std::string& p) { return fs::File(p); });

    ASSERT_OK_AND_ASSIGN(fs_, fs::internal::MockFileSystem::Make(fs::kNoTime, infos));
  }

  void MakeDataset(const std::vector<fs::FileInfo>& infos,
                   compute::Expression root_partition = literal(true),
                   std::vector<compute::Expression> partitions = {},
                   std::shared_ptr<Schema> s = schema({})) {
    auto n_fragments = infos.size();
    if (partitions.empty()) {
      partitions.resize(n_fragments, literal(true));
    }

    MakeFileSystem(infos);
    auto format = std::make_shared<DummyFileFormat>(s);

    std::vector<std::shared_ptr<FileFragment>> fragments;
    for (size_t i = 0; i < n_fragments; i++) {
      const auto& info = infos[i];
      if (!info.IsFile()) {
        continue;
      }

      ASSERT_OK_AND_ASSIGN(partitions[i], partitions[i].Bind(*s));
      ASSERT_OK_AND_ASSIGN(auto fragment,
                           format->MakeFragment({info, fs_}, partitions[i]));
      fragments.push_back(std::move(fragment));
    }

    ASSERT_OK_AND_ASSIGN(root_partition, root_partition.Bind(*s));
    ASSERT_OK_AND_ASSIGN(dataset_, FileSystemDataset::Make(s, root_partition, format, fs_,
                                                           std::move(fragments)));
  }

  std::shared_ptr<fs::FileSystem> fs_;
  std::shared_ptr<Dataset> dataset_;
  std::shared_ptr<ScanOptions> options_;
};

static const std::string& PathOf(const std::shared_ptr<Fragment>& fragment) {
  EXPECT_NE(fragment, nullptr);
  EXPECT_THAT(fragment->type_name(), "dummy");
  return internal::checked_cast<const FileFragment&>(*fragment).source().path();
}

class TestFileSystemDataset : public ::testing::Test,
                              public MakeFileSystemDatasetMixin {};

static std::vector<std::string> PathsOf(const FragmentVector& fragments) {
  std::vector<std::string> paths(fragments.size());
  std::transform(fragments.begin(), fragments.end(), paths.begin(), PathOf);
  return paths;
}

void AssertFilesAre(const std::shared_ptr<Dataset>& dataset,
                    std::vector<std::string> expected) {
  auto fs_dataset = internal::checked_cast<FileSystemDataset*>(dataset.get());
  EXPECT_THAT(fs_dataset->files(), testing::UnorderedElementsAreArray(expected));
}

void AssertFragmentsAreFromPath(FragmentIterator it, std::vector<std::string> expected) {
  // Ordering is not guaranteed.
  EXPECT_THAT(PathsOf(IteratorToVector(std::move(it))),
              testing::UnorderedElementsAreArray(expected));
}

static std::vector<compute::Expression> PartitionExpressionsOf(
    const FragmentVector& fragments) {
  std::vector<compute::Expression> partition_expressions;
  std::transform(fragments.begin(), fragments.end(),
                 std::back_inserter(partition_expressions),
                 [](const std::shared_ptr<Fragment>& fragment) {
                   return fragment->partition_expression();
                 });
  return partition_expressions;
}

void AssertFragmentsHavePartitionExpressions(std::shared_ptr<Dataset> dataset,
                                             std::vector<compute::Expression> expected) {
  ASSERT_OK_AND_ASSIGN(auto fragment_it, dataset->GetFragments());
  for (auto& expr : expected) {
    ASSERT_OK_AND_ASSIGN(expr, expr.Bind(*dataset->schema()));
  }
  // Ordering is not guaranteed.
  EXPECT_THAT(PartitionExpressionsOf(IteratorToVector(std::move(fragment_it))),
              testing::UnorderedElementsAreArray(expected));
}

struct ArithmeticDatasetFixture {
  static std::shared_ptr<Schema> schema() {
    return ::arrow::schema({
        field("i64", int64()),
        // ARROW-1644: Parquet can't write complex level
        // field("struct", struct_({
        //                     // ARROW-2587: Parquet can't write struct with more
        //                     // than one field.
        //                     // field("i32", int32()),
        //                     field("str", utf8()),
        //                 })),
        field("u8", uint8()),
        field("list", list(int32())),
        field("bool", boolean()),
    });
  }

  /// \brief Creates a single JSON record templated with n as follow.
  ///
  /// {"i64": n, "struct": {"i32": n, "str": "n"}, "u8": n "list": [n,n], "bool": n %
  /// 2},
  static std::string JSONRecordFor(int64_t n) {
    std::stringstream ss;
    auto n_i32 = static_cast<int32_t>(n);

    ss << "{";
    ss << "\"i64\": " << n << ", ";
    // ss << "\"struct\": {";
    // {
    //   // ss << "\"i32\": " << n_i32 << ", ";
    //   ss << "\"str\": \"" << std::to_string(n) << "\"";
    // }
    // ss << "}, ";
    ss << "\"u8\": " << static_cast<int32_t>(n) << ", ";
    ss << "\"list\": [" << n_i32 << ", " << n_i32 << "], ";
    ss << "\"bool\": " << (static_cast<bool>(n % 2) ? "true" : "false");
    ss << "}";

    return ss.str();
  }

  /// \brief Creates a JSON RecordBatch
  static std::string JSONRecordBatch(int64_t n) {
    DCHECK_GT(n, 0);

    auto record = JSONRecordFor(n);

    std::stringstream ss;
    ss << "[\n";
    for (int64_t i = 1; i <= n; i++) {
      if (i != 1) {
        ss << "\n,";
      }
      ss << record;
    }
    ss << "]\n";
    return ss.str();
  }

  static std::shared_ptr<RecordBatch> GetRecordBatch(int64_t n) {
    return RecordBatchFromJSON(ArithmeticDatasetFixture::schema(), JSONRecordBatch(n));
  }

  static std::unique_ptr<RecordBatchReader> GetRecordBatchReader(int64_t n) {
    DCHECK_GT(n, 0);

    // Functor which generates `n` RecordBatch
    struct {
      Status operator()(std::shared_ptr<RecordBatch>* out) {
        *out = i++ < count ? GetRecordBatch(i) : nullptr;
        return Status::OK();
      }
      int64_t i;
      int64_t count;
    } generator{0, n};

    return MakeGeneratedRecordBatch(schema(), std::move(generator));
  }
};

class WriteFileSystemDatasetMixin : public MakeFileSystemDatasetMixin {
 public:
  using PathAndContent = std::unordered_map<std::string, std::string>;

  void MakeSourceDataset() {
    PathAndContent source_files;

    source_files["/dataset/year=2018/month=01/dat0.json"] = R"([
        {"region": "NY", "model": "3", "sales": 742.0, "country": "US"},
        {"region": "NY", "model": "S", "sales": 304.125, "country": "US"},
        {"region": "NY", "model": "Y", "sales": 27.5, "country": "US"}
      ])";
    source_files["/dataset/year=2018/month=01/dat1.json"] = R"([
        {"region": "QC", "model": "3", "sales": 512, "country": "CA"},
        {"region": "QC", "model": "S", "sales": 978, "country": "CA"},
        {"region": "NY", "model": "X", "sales": 136.25, "country": "US"},
        {"region": "QC", "model": "X", "sales": 1.0, "country": "CA"},
        {"region": "QC", "model": "Y", "sales": 69, "country": "CA"}
      ])";
    source_files["/dataset/year=2019/month=01/dat0.json"] = R"([
        {"region": "CA", "model": "3", "sales": 273.5, "country": "US"},
        {"region": "CA", "model": "S", "sales": 13, "country": "US"},
        {"region": "CA", "model": "X", "sales": 54, "country": "US"},
        {"region": "QC", "model": "S", "sales": 10, "country": "CA"},
        {"region": "CA", "model": "Y", "sales": 21, "country": "US"}
      ])";
    source_files["/dataset/year=2019/month=01/dat1.json"] = R"([
        {"region": "QC", "model": "3", "sales": 152.25, "country": "CA"},
        {"region": "QC", "model": "X", "sales": 42, "country": "CA"},
        {"region": "QC", "model": "Y", "sales": 37, "country": "CA"}
      ])";
    source_files["/dataset/.pesky"] = "garbage content";

    auto mock_fs = std::make_shared<fs::internal::MockFileSystem>(fs::kNoTime);
    for (const auto& f : source_files) {
      ARROW_EXPECT_OK(mock_fs->CreateFile(f.first, f.second, /* recursive */ true));
    }
    fs_ = mock_fs;

    /// schema for the whole dataset (both source and destination)
    source_schema_ = schema({
        field("region", utf8()),
        field("model", utf8()),
        field("sales", float64()),
        field("year", int32()),
        field("month", int32()),
        field("country", utf8()),
    });

    /// Dummy file format for source dataset. Note that it isn't partitioned on country
    auto source_format = std::make_shared<JSONRecordBatchFileFormat>(
        SchemaFromColumnNames(source_schema_, {"region", "model", "sales", "country"}));

    fs::FileSelector s;
    s.base_dir = "/dataset";
    s.recursive = true;

    FileSystemFactoryOptions options;
    options.selector_ignore_prefixes = {"."};
    options.partitioning = std::make_shared<HivePartitioning>(
        SchemaFromColumnNames(source_schema_, {"year", "month"}));
    ASSERT_OK_AND_ASSIGN(auto factory,
                         FileSystemDatasetFactory::Make(fs_, s, source_format, options));
    ASSERT_OK_AND_ASSIGN(dataset_, factory->Finish());

    scan_options_ = std::make_shared<ScanOptions>();
    scan_options_->dataset_schema = source_schema_;
    ASSERT_OK(SetProjection(scan_options_.get(), source_schema_->field_names()));
  }

  void SetWriteOptions(std::shared_ptr<FileWriteOptions> file_write_options) {
    write_options_.file_write_options = file_write_options;
    write_options_.filesystem = fs_;
    write_options_.base_dir = "new_root/";
    write_options_.basename_template = "dat_{i}";
  }

  void DoWrite(std::shared_ptr<Partitioning> desired_partitioning) {
    write_options_.partitioning = desired_partitioning;
    auto scanner_builder = ScannerBuilder(dataset_, scan_options_);
    ASSERT_OK_AND_ASSIGN(auto scanner, scanner_builder.Finish());
    ASSERT_OK(FileSystemDataset::Write(write_options_, scanner));

    // re-discover the written dataset
    fs::FileSelector s;
    s.recursive = true;
    s.base_dir = "/new_root";

    FileSystemFactoryOptions factory_options;
    factory_options.partitioning = desired_partitioning;
    ASSERT_OK_AND_ASSIGN(
        auto factory, FileSystemDatasetFactory::Make(fs_, s, format_, factory_options));
    ASSERT_OK_AND_ASSIGN(written_, factory->Finish());
  }

  void TestWriteWithIdenticalPartitioningSchema() {
    DoWrite(std::make_shared<DirectoryPartitioning>(
        SchemaFromColumnNames(source_schema_, {"year", "month"})));

    expected_files_["/new_root/2018/1/dat_0"] = R"([
        {"region": "NY", "model": "3", "sales": 742.0, "country": "US"},
        {"region": "NY", "model": "S", "sales": 304.125, "country": "US"},
        {"region": "NY", "model": "Y", "sales": 27.5, "country": "US"},
        {"region": "QC", "model": "3", "sales": 512, "country": "CA"},
        {"region": "QC", "model": "S", "sales": 978, "country": "CA"},
        {"region": "NY", "model": "X", "sales": 136.25, "country": "US"},
        {"region": "QC", "model": "X", "sales": 1.0, "country": "CA"},
        {"region": "QC", "model": "Y", "sales": 69, "country": "CA"}
      ])";
    expected_files_["/new_root/2019/1/dat_1"] = R"([
        {"region": "CA", "model": "3", "sales": 273.5, "country": "US"},
        {"region": "CA", "model": "S", "sales": 13, "country": "US"},
        {"region": "CA", "model": "X", "sales": 54, "country": "US"},
        {"region": "QC", "model": "S", "sales": 10, "country": "CA"},
        {"region": "CA", "model": "Y", "sales": 21, "country": "US"},
        {"region": "QC", "model": "3", "sales": 152.25, "country": "CA"},
        {"region": "QC", "model": "X", "sales": 42, "country": "CA"},
        {"region": "QC", "model": "Y", "sales": 37, "country": "CA"}
      ])";
    expected_physical_schema_ =
        SchemaFromColumnNames(source_schema_, {"region", "model", "sales", "country"});

    AssertWrittenAsExpected();
  }

  void TestWriteWithUnrelatedPartitioningSchema() {
    DoWrite(std::make_shared<DirectoryPartitioning>(
        SchemaFromColumnNames(source_schema_, {"country", "region"})));

    // XXX first thing a user will be annoyed by: we don't support left
    // padding the month field with 0.
    expected_files_["/new_root/US/NY/dat_0"] = R"([
        {"year": 2018, "month": 1, "model": "3", "sales": 742.0},
        {"year": 2018, "month": 1, "model": "S", "sales": 304.125},
        {"year": 2018, "month": 1, "model": "Y", "sales": 27.5},
        {"year": 2018, "month": 1, "model": "X", "sales": 136.25}
  ])";
    expected_files_["/new_root/CA/QC/dat_1"] = R"([
        {"year": 2018, "month": 1, "model": "3", "sales": 512},
        {"year": 2018, "month": 1, "model": "S", "sales": 978},
        {"year": 2018, "month": 1, "model": "X", "sales": 1.0},
        {"year": 2018, "month": 1, "model": "Y", "sales": 69},
        {"year": 2019, "month": 1, "model": "S", "sales": 10},
        {"year": 2019, "month": 1, "model": "3", "sales": 152.25},
        {"year": 2019, "month": 1, "model": "X", "sales": 42},
        {"year": 2019, "month": 1, "model": "Y", "sales": 37}
  ])";
    expected_files_["/new_root/US/CA/dat_2"] = R"([
        {"year": 2019, "month": 1, "model": "3", "sales": 273.5},
        {"year": 2019, "month": 1, "model": "S", "sales": 13},
        {"year": 2019, "month": 1, "model": "X", "sales": 54},
        {"year": 2019, "month": 1, "model": "Y", "sales": 21}
  ])";
    expected_physical_schema_ =
        SchemaFromColumnNames(source_schema_, {"model", "sales", "year", "month"});

    AssertWrittenAsExpected();
  }

  void TestWriteWithSupersetPartitioningSchema() {
    DoWrite(std::make_shared<DirectoryPartitioning>(
        SchemaFromColumnNames(source_schema_, {"year", "month", "country", "region"})));

    // XXX first thing a user will be annoyed by: we don't support left
    // padding the month field with 0.
    expected_files_["/new_root/2018/1/US/NY/dat_0"] = R"([
        {"model": "3", "sales": 742.0},
        {"model": "S", "sales": 304.125},
        {"model": "Y", "sales": 27.5},
        {"model": "X", "sales": 136.25}
  ])";
    expected_files_["/new_root/2018/1/CA/QC/dat_1"] = R"([
        {"model": "3", "sales": 512},
        {"model": "S", "sales": 978},
        {"model": "X", "sales": 1.0},
        {"model": "Y", "sales": 69}
  ])";
    expected_files_["/new_root/2019/1/US/CA/dat_2"] = R"([
        {"model": "3", "sales": 273.5},
        {"model": "S", "sales": 13},
        {"model": "X", "sales": 54},
        {"model": "Y", "sales": 21}
  ])";
    expected_files_["/new_root/2019/1/CA/QC/dat_3"] = R"([
        {"model": "S", "sales": 10},
        {"model": "3", "sales": 152.25},
        {"model": "X", "sales": 42},
        {"model": "Y", "sales": 37}
  ])";
    expected_physical_schema_ = SchemaFromColumnNames(source_schema_, {"model", "sales"});

    AssertWrittenAsExpected();
  }

  void TestWriteWithEmptyPartitioningSchema() {
    DoWrite(std::make_shared<DirectoryPartitioning>(
        SchemaFromColumnNames(source_schema_, {})));

    expected_files_["/new_root/dat_0"] = R"([
        {"country": "US", "region": "NY", "year": 2018, "month": 1, "model": "3", "sales": 742.0},
        {"country": "US", "region": "NY", "year": 2018, "month": 1, "model": "S", "sales": 304.125},
        {"country": "US", "region": "NY", "year": 2018, "month": 1, "model": "Y", "sales": 27.5},
        {"country": "CA", "region": "QC", "year": 2018, "month": 1, "model": "3", "sales": 512},
        {"country": "CA", "region": "QC", "year": 2018, "month": 1, "model": "S", "sales": 978},
        {"country": "US", "region": "NY", "year": 2018, "month": 1, "model": "X", "sales": 136.25},
        {"country": "CA", "region": "QC", "year": 2018, "month": 1, "model": "X", "sales": 1.0},
        {"country": "CA", "region": "QC", "year": 2018, "month": 1, "model": "Y", "sales": 69},
        {"country": "US", "region": "CA", "year": 2019, "month": 1, "model": "3", "sales": 273.5},
        {"country": "US", "region": "CA", "year": 2019, "month": 1, "model": "S", "sales": 13},
        {"country": "US", "region": "CA", "year": 2019, "month": 1, "model": "X", "sales": 54},
        {"country": "CA", "region": "QC", "year": 2019, "month": 1, "model": "S", "sales": 10},
        {"country": "US", "region": "CA", "year": 2019, "month": 1, "model": "Y", "sales": 21},
        {"country": "CA", "region": "QC", "year": 2019, "month": 1, "model": "3", "sales": 152.25},
        {"country": "CA", "region": "QC", "year": 2019, "month": 1, "model": "X", "sales": 42},
        {"country": "CA", "region": "QC", "year": 2019, "month": 1, "model": "Y", "sales": 37}
  ])";
    expected_physical_schema_ = source_schema_;

    AssertWrittenAsExpected();
  }

  void AssertWrittenAsExpected() {
    std::unordered_set<std::string> expected_paths, actual_paths;
    for (const auto& file_contents : expected_files_) {
      expected_paths.insert(file_contents.first);
    }
    for (auto path : checked_pointer_cast<FileSystemDataset>(written_)->files()) {
      actual_paths.insert(std::move(path));
    }
    EXPECT_THAT(actual_paths, testing::UnorderedElementsAreArray(expected_paths));

    ASSERT_OK_AND_ASSIGN(auto written_fragments_it, written_->GetFragments());
    for (auto maybe_fragment : written_fragments_it) {
      ASSERT_OK_AND_ASSIGN(auto fragment, maybe_fragment);

      ASSERT_OK_AND_ASSIGN(auto actual_physical_schema, fragment->ReadPhysicalSchema());
      AssertSchemaEqual(*expected_physical_schema_, *actual_physical_schema,
                        check_metadata_);

      const auto& path = checked_pointer_cast<FileFragment>(fragment)->source().path();

      auto file_contents = expected_files_.find(path);
      if (file_contents == expected_files_.end()) {
        // file wasn't expected to be written at all; nothing to compare with
        continue;
      }

      ASSERT_OK_AND_ASSIGN(auto scanner, ScannerBuilder(actual_physical_schema, fragment,
                                                        std::make_shared<ScanOptions>())
                                             .Finish());
      ASSERT_OK_AND_ASSIGN(auto actual_table, scanner->ToTable());
      ASSERT_OK_AND_ASSIGN(actual_table, actual_table->CombineChunks());
      std::shared_ptr<Array> actual_struct;

      for (auto maybe_batch :
           MakeIteratorFromReader(std::make_shared<TableBatchReader>(*actual_table))) {
        ASSERT_OK_AND_ASSIGN(auto batch, maybe_batch);
        ASSERT_OK_AND_ASSIGN(actual_struct, batch->ToStructArray());
      }

      auto expected_struct = ArrayFromJSON(struct_(expected_physical_schema_->fields()),
                                           {file_contents->second});

      AssertArraysEqual(*expected_struct, *actual_struct, /*verbose=*/true);
    }
  }

  bool check_metadata_ = true;
  std::shared_ptr<Schema> source_schema_;
  std::shared_ptr<FileFormat> format_;
  PathAndContent expected_files_;
  std::shared_ptr<Schema> expected_physical_schema_;
  std::shared_ptr<Dataset> written_;
  FileSystemDatasetWriteOptions write_options_;
  std::shared_ptr<ScanOptions> scan_options_;
};

}  // namespace dataset
}  // namespace arrow
