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

#include <stdlib.h>
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "benchmark/benchmark.h"
#include "gandiva/projector.h"
#include "gandiva/tests/test_util.h"
#include "gandiva/tests/timed_evaluate.h"
#include "gandiva/tree_expr_builder.h"

namespace gandiva {

using arrow::boolean;
using arrow::int32;
using arrow::int64;
using arrow::utf8;

// TODO : the base numbers are from a mac. they need to be caliberated
// for the hardware used by travis.
float tolerance_ratio = 6.0;

static void TimedTestAdd3(benchmark::State& state) {
  // schema for input fields
  auto field0 = field("f0", int64());
  auto field1 = field("f1", int64());
  auto field2 = field("f2", int64());
  auto schema = arrow::schema({field0, field1, field2});
  auto pool_ = arrow::default_memory_pool();

  // output field
  auto field_sum = field("add", int64());

  // Build expression
  auto part_sum = TreeExprBuilder::MakeFunction(
      "add", {TreeExprBuilder::MakeField(field1), TreeExprBuilder::MakeField(field2)},
      int64());
  auto sum = TreeExprBuilder::MakeFunction(
      "add", {TreeExprBuilder::MakeField(field0), part_sum}, int64());

  auto sum_expr = TreeExprBuilder::MakeExpression(sum, field_sum);

  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, {sum_expr}, TestConfiguration(), &projector));

  Int64DataGenerator data_generator;
  ProjectEvaluator evaluator(projector);

  Status status = TimedEvaluate<arrow::Int64Type, int64_t>(
      schema, evaluator, data_generator, pool_, 1 * MILLION, 16 * THOUSAND, state);
  ASSERT_OK(status);
}

static void TimedTestBigNested(benchmark::State& state) {
  // schema for input fields
  auto fielda = field("a", int32());
  auto schema = arrow::schema({fielda});
  auto pool_ = arrow::default_memory_pool();

  // output fields
  auto field_result = field("res", int32());

  // build expression.
  // if (a < 10)
  //   10
  // else if (a < 20)
  //   20
  // ..
  // ..
  // else if (a < 190)
  //   190
  // else
  //   200
  auto node_a = TreeExprBuilder::MakeField(fielda);
  auto top_node = TreeExprBuilder::MakeLiteral(200);
  for (int thresh = 190; thresh > 0; thresh -= 10) {
    auto literal = TreeExprBuilder::MakeLiteral(thresh);
    auto condition =
        TreeExprBuilder::MakeFunction("less_than", {node_a, literal}, boolean());
    auto if_node = TreeExprBuilder::MakeIf(condition, literal, top_node, int32());
    top_node = if_node;
  }
  auto expr = TreeExprBuilder::MakeExpression(top_node, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, {expr}, TestConfiguration(), &projector));

  BoundedInt32DataGenerator data_generator(250);
  ProjectEvaluator evaluator(projector);

  Status status = TimedEvaluate<arrow::Int32Type, int32_t>(
      schema, evaluator, data_generator, pool_, 1 * MILLION, 16 * THOUSAND, state);
  ASSERT_TRUE(status.ok());
}

static void TimedTestExtractYear(benchmark::State& state) {
  // schema for input fields
  auto field0 = field("f0", arrow::date64());
  auto schema = arrow::schema({field0});
  auto pool_ = arrow::default_memory_pool();

  // output field
  auto field_res = field("res", int64());

  // Build expression
  auto expr = TreeExprBuilder::MakeExpression("extractYear", {field0}, field_res);

  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, {expr}, TestConfiguration(), &projector));

  Int64DataGenerator data_generator;
  ProjectEvaluator evaluator(projector);

  Status status = TimedEvaluate<arrow::Date64Type, int64_t>(
      schema, evaluator, data_generator, pool_, 1 * MILLION, 16 * THOUSAND, state);
  ASSERT_TRUE(status.ok());
}

static void TimedTestFilterAdd2(benchmark::State& state) {
  // schema for input fields
  auto field0 = field("f0", int64());
  auto field1 = field("f1", int64());
  auto field2 = field("f2", int64());
  auto schema = arrow::schema({field0, field1, field2});
  auto pool_ = arrow::default_memory_pool();

  // Build expression
  auto sum = TreeExprBuilder::MakeFunction(
      "add", {TreeExprBuilder::MakeField(field1), TreeExprBuilder::MakeField(field2)},
      int64());
  auto less_than = TreeExprBuilder::MakeFunction(
      "less_than", {sum, TreeExprBuilder::MakeField(field2)}, boolean());
  auto condition = TreeExprBuilder::MakeCondition(less_than);

  std::shared_ptr<Filter> filter;
  ASSERT_OK(Filter::Make(schema, condition, TestConfiguration(), &filter));

  Int64DataGenerator data_generator;
  FilterEvaluator evaluator(filter);

  Status status = TimedEvaluate<arrow::Int64Type, int64_t>(
      schema, evaluator, data_generator, pool_, MILLION, 16 * THOUSAND, state);
  ASSERT_TRUE(status.ok());
}

static void TimedTestFilterLike(benchmark::State& state) {
  // schema for input fields
  auto fielda = field("a", utf8());
  auto schema = arrow::schema({fielda});
  auto pool_ = arrow::default_memory_pool();

  // build expression.
  auto node_a = TreeExprBuilder::MakeField(fielda);
  auto pattern_node = TreeExprBuilder::MakeStringLiteral("%yellow%");
  auto like_yellow =
      TreeExprBuilder::MakeFunction("like", {node_a, pattern_node}, arrow::boolean());
  auto condition = TreeExprBuilder::MakeCondition(like_yellow);

  std::shared_ptr<Filter> filter;
  ASSERT_OK(Filter::Make(schema, condition, TestConfiguration(), &filter));

  FastUtf8DataGenerator data_generator(32);
  FilterEvaluator evaluator(filter);

  Status status = TimedEvaluate<arrow::StringType, std::string>(
      schema, evaluator, data_generator, pool_, 1 * MILLION, 16 * THOUSAND, state);
  ASSERT_TRUE(status.ok());
}

static void TimedTestAllocs(benchmark::State& state) {
  // schema for input fields
  auto field_a = field("a", arrow::utf8());
  auto schema = arrow::schema({field_a});
  auto pool_ = arrow::default_memory_pool();

  // output field
  auto field_res = field("res", int32());

  // Build expression
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto upper = TreeExprBuilder::MakeFunction("upper", {node_a}, utf8());
  auto length = TreeExprBuilder::MakeFunction("octet_length", {upper}, int32());
  auto expr = TreeExprBuilder::MakeExpression(length, field_res);

  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, {expr}, TestConfiguration(), &projector));

  FastUtf8DataGenerator data_generator(64);
  ProjectEvaluator evaluator(projector);

  Status status = TimedEvaluate<arrow::StringType, std::string>(
      schema, evaluator, data_generator, pool_, 1 * MILLION, 16 * THOUSAND, state);
  ASSERT_TRUE(status.ok());
}
// following two tests are for benchmark optimization of
// in expr. will be used in follow-up PRs to optimize in expr.

static void TimedTestMultiOr(benchmark::State& state) {
  // schema for input fields
  auto fielda = field("a", utf8());
  auto schema = arrow::schema({fielda});
  auto pool_ = arrow::default_memory_pool();

  // output fields
  auto field_result = field("res", boolean());

  // build expression.
  // booleanOr(a = string1, a = string2, ..)
  auto node_a = TreeExprBuilder::MakeField(fielda);

  NodeVector boolean_functions;
  FastUtf8DataGenerator data_generator1(250);
  for (int thresh = 1; thresh <= 32; thresh++) {
    auto literal = TreeExprBuilder::MakeStringLiteral(data_generator1.GenerateData());
    auto condition = TreeExprBuilder::MakeFunction("equal", {node_a, literal}, boolean());
    boolean_functions.push_back(condition);
  }

  auto boolean_or = TreeExprBuilder::MakeOr(boolean_functions);
  auto expr = TreeExprBuilder::MakeExpression(boolean_or, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, {expr}, TestConfiguration(), &projector));

  FastUtf8DataGenerator data_generator(250);
  ProjectEvaluator evaluator(projector);
  Status status = TimedEvaluate<arrow::StringType, std::string>(
      schema, evaluator, data_generator, pool_, 100 * THOUSAND, 16 * THOUSAND, state);
  ASSERT_OK(status);
}

static void TimedTestInExpr(benchmark::State& state) {
  // schema for input fields
  auto fielda = field("a", utf8());
  auto schema = arrow::schema({fielda});
  auto pool_ = arrow::default_memory_pool();

  // output fields
  auto field_result = field("res", boolean());

  // build expression.
  // a in (string1, string2, ..)
  auto node_a = TreeExprBuilder::MakeField(fielda);

  std::unordered_set<std::string> values;
  FastUtf8DataGenerator data_generator1(250);
  for (int i = 1; i <= 32; i++) {
    values.insert(data_generator1.GenerateData());
  }
  auto boolean_or = TreeExprBuilder::MakeInExpressionString(node_a, values);
  auto expr = TreeExprBuilder::MakeExpression(boolean_or, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, {expr}, TestConfiguration(), &projector));

  FastUtf8DataGenerator data_generator(250);
  ProjectEvaluator evaluator(projector);

  Status status = TimedEvaluate<arrow::StringType, std::string>(
      schema, evaluator, data_generator, pool_, 100 * THOUSAND, 16 * THOUSAND, state);

  ASSERT_OK(status);
}

BENCHMARK(TimedTestAdd3)->MinTime(1.0)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestBigNested)->MinTime(1.0)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestBigNested)->MinTime(1.0)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestExtractYear)->MinTime(1.0)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestFilterAdd2)->MinTime(1.0)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestFilterLike)->MinTime(1.0)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestAllocs)->MinTime(1.0)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestMultiOr)->MinTime(1.0)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestInExpr)->MinTime(1.0)->Unit(benchmark::kMicrosecond);

}  // namespace gandiva
