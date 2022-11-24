#
# Copyright 2021 zero323
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


test_that("can execute compaction", {
  path <- delta_test_tempfile()

  withr::with_file(path, {
    SparkR::createDataFrame(data.frame(key = c("a", "a"), value = c(1, 2))) %>%
      SparkR::repartition(1) %>%
      dlt_write(path, mode = "append")

    SparkR::createDataFrame(data.frame(key = c("a", "a"), value = c(3, 4))) %>%
      SparkR::repartition(1) %>%
      dlt_write(path, mode = "append")

    SparkR::createDataFrame(data.frame(key = c("b", "b"), value = c(1, 2))) %>%
      SparkR::repartition(1) %>%
      dlt_write(path, mode = "append")

    dlt_for_path(path) %>%
      dlt_optimize() %>%
      expect_s4_class("DeltaOptimizeBuilder") %>%
      dlt_execute_compaction() %>%
      expect_s4_class("SparkDataFrame") %>%
      SparkR::selectExpr("metrics.numFilesAdded = 1 AND metrics.numFilesRemoved = 3") %>%
      SparkR::collect() %>%
      all() %>%
      expect_true()

    dlt_for_path(path) %>%
      dlt_history() %>%
      SparkR::select(SparkR::column("operationParameters.predicate") == "[]") %>%
      SparkR::where("version = 3") %>%
      SparkR::collect() %>%
      all() %>%
      expect_true()
  })
})


test_that("can execute compaction with partition filter", {
  path <- delta_test_tempfile()

  withr::with_file(path, {
    SparkR::createDataFrame(data.frame(key = c("a", "a"), value = c(1, 2))) %>%
      SparkR::repartition(1) %>%
      dlt_write(path, mode = "append", partitionBy = "key")

    SparkR::createDataFrame(data.frame(key = c("a", "a"), value = c(3, 4))) %>%
      SparkR::repartition(1) %>%
      dlt_write(path, mode = "append", partitionBy = "key")

    SparkR::createDataFrame(data.frame(key = c("b", "b"), value = c(1, 2))) %>%
      SparkR::repartition(1) %>%
      dlt_write(path, mode = "append", partitionBy = "key")

    dlt_for_path(path) %>%
      dlt_optimize() %>%
      expect_s4_class("DeltaOptimizeBuilder") %>%
      dlt_where("key = 'a'") %>%
      expect_s4_class("DeltaOptimizeBuilder") %>%
      dlt_execute_compaction() %>%
      expect_s4_class("SparkDataFrame") %>%
      SparkR::selectExpr("metrics.numFilesAdded = 1 AND metrics.numFilesRemoved = 2") %>%
      SparkR::collect() %>%
      all() %>%
      expect_true()

    dlt_for_path(path) %>%
      dlt_history() %>%
      SparkR::select(SparkR::column("operationParameters.predicate") == "[\"(key = 'a')\"]") %>%
      SparkR::where("version = 3") %>%
      SparkR::collect() %>%
      all() %>%
      expect_true()
  })
})


test_that("can execute Z-order by", {
  path <- delta_test_tempfile()

  withr::with_file(path, {
    SparkR::sql("SELECT cast(id AS integer) AS value FROM range(0, 100)") %>%
      SparkR::selectExpr("floor(value % 7) AS col1", "floor(value % 27) AS col2", "floor(value % 10) AS p") %>%
      SparkR::repartition(4) %>%
      dlt_write(path, partitionBy = "p")

    dlt_for_path(path) %>%
      dlt_optimize() %>%
      expect_s4_class("DeltaOptimizeBuilder") %>%
      dlt_execute_z_order_by("col1", "col2") %>%
      expect_s4_class("SparkDataFrame")

    dlt_for_path(path) %>%
      dlt_history() %>%
      SparkR::select(
        (SparkR::column("operationParameters.predicate") == "[]") &
          (SparkR::column("operationParameters.zOrderBy") == '["col1","col2"]')
      ) %>%
      SparkR::where("version = 1") %>%
      SparkR::collect() %>%
      all() %>%
      expect_true()
  })
})


test_that("can execute Z-order by with partition filter", {
  path <- delta_test_tempfile()

  withr::with_file(path, {
    SparkR::sql("SELECT cast(id AS integer) AS value FROM range(0, 100)") %>%
      SparkR::selectExpr("floor(value % 7) AS col1", "floor(value % 27) AS col2", "floor(value % 10) AS p") %>%
      SparkR::repartition(4) %>%
      dlt_write(path, partitionBy = "p")

    dlt_for_path(path) %>%
      dlt_optimize() %>%
      dlt_where("p = 2") %>%
      expect_s4_class("DeltaOptimizeBuilder") %>%
      dlt_execute_z_order_by("col1", "col2") %>%
      expect_s4_class("SparkDataFrame")

    dlt_for_path(path) %>%
      dlt_history() %>%
      SparkR::select(
        (SparkR::column("operationParameters.predicate") == '["(p = 2)"]') &
          (SparkR::column("operationParameters.zOrderBy") == '["col1","col2"]')
      ) %>%
      SparkR::where("version = 1") %>%
      SparkR::collect() %>%
      all() %>%
      expect_true()
  })
})
