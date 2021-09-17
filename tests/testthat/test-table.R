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

test_that("can load with dlt_for_path", {
  path <- delta_test_tempfile()

  withr::with_file(path, {
    expected <- test_data()

    expected %>%
      dlt_write(path)

    dlt_for_path(path) %>%
      dlt_to_df() %>%
      expect_sdf_equivalent(expected)
  })
})


test_that("can load with dlt_for_name", {
  skip_if(
    toupper(Sys.getenv("DLT_TESTTHAT_SKIP_SLOW")) == "TRUE",
    "DLT_TESTTHAT_SKIP_SLOW is TRUE"
  )

  name <- delta_test_name()
  withr::defer(SparkR::sql(paste("DROP TABLE IF EXISTS", name)))

  expected <- test_data()

  expected %>%
    SparkR::saveAsTable(name, source = "delta")

  dlt_for_name(name) %>%
    dlt_to_df() %>%
    expect_sdf_equivalent(expected)
})


test_that("can alias", {
  path <- delta_test_tempfile()

  withr::with_file(path, {
    test_data() %>%
      dlt_write(path)

    dlt_for_path(path) %>%
      dlt_alias("this_table") %>%
      dlt_to_df() %>%
      SparkR::select("this_table.id") %>%
      expect_s4_class("SparkDataFrame")
  })
})


test_that("can delete all from table", {
  path <- delta_test_tempfile()

  withr::with_file(path, {
    test_data() %>%
      dlt_write(path)

    tbl <- dlt_for_path(path)

    tbl %>%
      dlt_to_df() %>%
      SparkR::count() %>%
      expect_equal(SparkR::count(test_data()))

    tbl %>%
      dlt_delete()

    tbl %>%
      dlt_to_df() %>%
      SparkR::count() %>%
      expect_equal(0)
  })
})


test_that("can delete with condition from table", {
  path <- delta_test_tempfile()

  withr::with_file(path, {
    test_data() %>%
      dlt_write(path)

    tbl <- dlt_for_path(path)

    cond1 <- "key = 'b'"

    tbl %>%
      dlt_delete(cond1)

    expected1 <- test_data() %>%
      SparkR::where(!SparkR::expr(cond1))

    tbl %>%
      dlt_to_df() %>%
      expect_sdf_equivalent(expected1)

    cond2 <- SparkR::column("ind") == -1 & SparkR::column("long") < 0

    tbl %>%
      dlt_delete(cond2)

    expected2 <- expected1 %>%
      SparkR::where(!cond2)

    tbl %>%
      dlt_to_df() %>%
      expect_sdf_equivalent(expected2)
  })
})


test_that("can update table", {
  path <- delta_test_tempfile()

  withr::with_file(path, {
    test_data() %>%
      dlt_write(path)

    tbl <- dlt_for_path(path)

    lat1 <- "0"
    long1 <- "0"

    tbl %>%
      dlt_update(c("long" = lat1, lat = long1))

    expected1 <- test_data() %>%
      SparkR::withColumn("long", SparkR::expr(lat1)) %>%
      SparkR::withColumn("lat", SparkR::expr(long1))

    tbl %>%
      dlt_to_df() %>%
      expect_sdf_equivalent(expected1)


    cond1 <- SparkR::column("key") == "a"
    lat2 <- SparkR::lit(42)

    tbl %>%
      dlt_update(list(lat = lat2), cond1)

    expected2 <- expected1 %>%
      SparkR::withColumn(
        "lat",
        SparkR::when(cond1, lat2) %>%
          SparkR::otherwise(SparkR::column("lat"))
      )

    tbl %>%
      dlt_to_df() %>%
      expect_sdf_equivalent(expected2)

    cond2 <- "key = 'b'"
    long2 <- "long - 1"

    tbl %>%
      dlt_update(list(long = long2), cond2)

    expected3 <- expected2 %>%
      SparkR::withColumn(
        "long",
        SparkR::when(SparkR::expr(cond2), SparkR::expr(long2)) %>%
          SparkR::otherwise(SparkR::column("long"))
      )

    tbl %>%
      dlt_to_df() %>%
      expect_sdf_equivalent(expected3)
  })
})


test_that("can check if delta is delta", {
  path <- delta_test_tempfile()

  withr::with_file(path, {
    test_data() %>%
      dlt_write(path)

    path %>%
      dlt_is_delta_table() %>%
      expect_true()
  })
})


test_that("can check if not-delta is not-delta", {
  path <- delta_test_tempfile()

  withr::with_file(path, {
    test_data() %>%
      SparkR::write.parquet(path)

    path %>%
      dlt_is_delta_table() %>%
      expect_false()
  })
})


test_that("can query history", {
  path <- delta_test_tempfile()

  withr::with_file(path, {
    test_data() %>%
      dlt_write(path)

    tbl <- dlt_for_path(path)

    tbl %>%
      dlt_history() %>%
      expect_s4_class("SparkDataFrame") %>%
      SparkR::count() %>%
      expect_equal(1)

    tbl %>%
      dlt_delete()

    tbl %>%
      dlt_history() %>%
      SparkR::count() %>%
      expect_equal(2)

    tbl %>%
      dlt_history(1) %>%
      SparkR::count() %>%
      expect_equal(1)
  })
})


test_that("can convert to delta", {
  skip_if(
    toupper(Sys.getenv("DLT_TESTTHAT_SKIP_SLOW")) == "TRUE",
    "DLT_TESTTHAT_SKIP_SLOW is TRUE"
  )

  path <- delta_test_tempfile()

  withr::with_file(path, {
    test_data() %>%
      SparkR::write.parquet(path)

    expect_error(
      dlt_for_path(path)
    )

    dlt_convert_to_delta(paste0("parquet.`", path, "`"))

    path %>%
      dlt_is_delta_table() %>%
      expect_true()

    path %>%
      dlt_for_path() %>%
      expect_s4_class("DeltaTable")
  })
})


test_that("can convert to delta with partitions", {
  skip_if(
    toupper(Sys.getenv("DLT_TESTTHAT_SKIP_SLOW")) == "TRUE",
    "DLT_TESTTHAT_SKIP_SLOW is TRUE"
  )

  path <- delta_test_tempfile()

  withr::with_file(path, {
    test_data() %>%
      SparkR::write.df(path, partitionBy = "key", source = "parquet")

    expect_error(
      dlt_for_path(path)
    )

    dlt_convert_to_delta(paste0("parquet.`", path, "`"), "key string")

    path %>%
      dlt_is_delta_table() %>%
      expect_true()

    path %>%
      dlt_for_path() %>%
      expect_s4_class("DeltaTable")

    path %>%
      dlt_for_path() %>%
      dlt_history() %>%
      SparkR::filter(
        SparkR::expr("operationParameters['partitionedBy']") == '["key"]'
      ) %>%
      SparkR::count() %>%
      expect_gt(0)
  })
})


test_that("can vaccuum", {
  path <- delta_test_tempfile()

  withr::with_file(path, {
    test_data() %>%
      dlt_write(path)

    tbl <- dlt_for_path(path)

    files <- list.files(path, pattern = "*.parquet")

    tbl %>%
      dlt_delete()

    tbl %>%
      dlt_vacuum()

    # Shouldn't remove files with default retention
    list.files(path, pattern = "*.parquet") %>%
      expect_equal(files)

    set_spark_config(
      "spark.databricks.delta.retentionDurationCheck.enabled",
      "false"
    )

    withr::defer(
      set_spark_config(
        "spark.databricks.delta.retentionDurationCheck.enabled",
        "true"
      )
    )

    tbl %>%
      dlt_vacuum(0)

    list.files(path, pattern = "*.parquet") %>%
      length() %>%
      expect_equal(0)
  })
})


test_that("can update protocol", {
  path <- delta_test_tempfile()


  set_spark_config(
    "spark.databricks.delta.minReaderVersion", "1"
  )
  set_spark_config(
    "spark.databricks.delta.minWriterVersion", "2"
  )


  withr::defer({
    unset_spark_config(
      "spark.databricks.delta.minReaderVersion"
    )
    unset_spark_config(
      "spark.databricks.delta.minWriterVersion"
    )
  })


  withr::with_file(path, {
    test_data() %>%
      dlt_write(path)

    tbl <- dlt_for_path(path)

    # Should pass
    expect_null(
      dlt_upgrade_table_protocol(tbl, 1, 3)
    )

    # Should fail on attempted downgrade
    expect_error(
      dlt_upgrade_table_protocol(tbl, 1, 2)
    )
  })
})
