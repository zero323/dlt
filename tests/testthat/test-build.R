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


test_that("can create basic table in location", {
  path <- delta_test_tempfile()

  withr::with_file(path, {
    dlt_create() %>%
      dlt_location(path) %>%
      dlt_add_column("id", "integer") %>%
      dlt_execute() %>%
      expect_s4_class("DeltaTable")

    dlt_for_path(path) %>%
      dlt_to_df() %>%
      expect_schema_equal(
        "id integer"
      )
  })
})


test_that("can create table with name", {
  skip_if(
    toupper(Sys.getenv("DLT_TESTTHAT_SKIP_SLOW")) == "TRUE",
    "DLT_TESTTHAT_SKIP_SLOW is TRUE"
  )

  name <- delta_test_name()
  withr::defer(SparkR::sql(paste("DROP TABLE IF EXISTS", name)))


  dlt_create() %>%
    dlt_table_name(name) %>%
    dlt_add_column("id", "integer") %>%
    dlt_execute() %>%
    expect_s4_class("DeltaTable")


  dlt_for_name(name) %>%
    dlt_to_df() %>%
    expect_schema_equal(SparkR::structType("id integer"))
})


test_that("can create with columns with additional properties", {
  path <- delta_test_tempfile()

  withr::with_file(path, {
    dlt_create() %>%
      dlt_location(path) %>%
      dlt_add_column("x", "integer") %>%
      dlt_add_column(
        "minus_x", "integer",
        nullable = TRUE, generated_always_as = "-x"
      ) %>%
      dlt_add_column(
        "comment", "string",
        nullable = FALSE, comment = "This is a comment"
      ) %>%
      dlt_execute() %>%
      dlt_to_df() %>%
      SparkR::dtypes() %>%
      expect_equal(list(
        c("x", "int"),
        c("minus_x", "int"),
        c("comment", "string")
      ))
  })
})


test_that("cannot create twice for the same location", {
  path <- delta_test_tempfile()

  withr::with_file(path, {
    dlt_create() %>%
      dlt_location(path) %>%
      dlt_add_column("id", "integer") %>%
      dlt_execute()

    dlt_create() %>%
      dlt_location(path) %>%
      dlt_add_column("id", "integer") %>%
      dlt_execute() %>%
      expect_error()
  })
})


test_that("cannot create twice for the same name", {
  skip_if(
    toupper(Sys.getenv("DLT_TESTTHAT_SKIP_SLOW")) == "TRUE",
    "DLT_TESTTHAT_SKIP_SLOW is TRUE"
  )

  name <- delta_test_name()
  withr::defer(SparkR::sql(paste("DROP TABLE IF EXISTS", name)))

  dlt_create() %>%
    dlt_table_name(name) %>%
    dlt_add_column("id", "integer") %>%
    dlt_execute()

  dlt_create() %>%
    dlt_table_name(name) %>%
    dlt_add_column("id", "integer") %>%
    dlt_execute() %>%
    expect_error()
})


test_that("can create if not exist", {
  path <- delta_test_tempfile()

  expected_schema <- "key string"

  withr::with_file(path, {
    dlt_create_if_not_exists() %>%
      dlt_location(path) %>%
      dlt_add_column("key", "string") %>%
      dlt_execute() %>%
      expect_s4_class("DeltaTable") %>%
      dlt_to_df() %>%
      expect_schema_equal(expected_schema)

    # Shouldn't fail
    dlt_create_if_not_exists() %>%
      dlt_location(path) %>%
      dlt_add_column("value", "float") %>%
      dlt_execute() %>%
      expect_s4_class("DeltaTable")

    # Shouldn't replace
    dlt_for_path(path) %>%
      dlt_to_df() %>%
      expect_schema_equal(expected_schema)
  })
})


test_that("can replace", {
  path <- delta_test_tempfile()

  withr::with_file(path, {
    # Should fail if nothing to replace
    dlt_replace() %>%
      dlt_location(path) %>%
      dlt_add_column("key", "string") %>%
      dlt_execute() %>%
      expect_error()

    dlt_create() %>%
      dlt_location(path) %>%
      dlt_add_column("key", "string") %>%
      dlt_execute()

    # Shouldn't fail
    dlt_replace() %>%
      dlt_location(path) %>%
      dlt_add_column("value", "float") %>%
      dlt_execute() %>%
      expect_s4_class("DeltaTable")

    # Should replace
    dlt_for_path(path) %>%
      dlt_to_df() %>%
      expect_schema_equal("value float")
  })
})


test_that("can create or replace", {
  skip_if(
    toupper(Sys.getenv("DLT_TESTTHAT_SKIP_SLOW")) == "TRUE",
    "DLT_TESTTHAT_SKIP_SLOW is TRUE"
  )

  name <- delta_test_name()
  withr::defer(SparkR::sql(paste("DROP TABLE IF EXISTS", name)))

  dlt_create_or_replace() %>%
    dlt_table_name(name) %>%
    dlt_add_column("key", "string") %>%
    dlt_execute() %>%
    expect_s4_class("DeltaTable")

  dlt_for_name(name) %>%
    dlt_to_df() %>%
    expect_schema_equal("key string")

  dlt_create_or_replace() %>%
    dlt_table_name(name) %>%
    dlt_add_column("value", "float") %>%
    dlt_execute() %>%
    expect_s4_class("DeltaTable")

  dlt_for_name(name) %>%
    dlt_to_df() %>%
    expect_schema_equal("value float")
})


test_that("can create in location with various modifiers", {
  path <- delta_test_tempfile()

  withr::with_file(path, {
    dlt_create() %>%
      dlt_location(path) %>%
      dlt_add_column("id", "integer") %>%
      dlt_add_columns(SparkR::structType("key string, value float")) %>%
      dlt_add_columns("ind int, category string") %>%
      dlt_partitioned_by("id", "key") %>%
      dlt_property("some", "value") %>%
      dlt_comment("a test table") %>%
      dlt_execute() %>%
      expect_s4_class("DeltaTable")

    dlt_for_path(path) %>%
      dlt_to_df() %>%
      expect_schema_equal(
        "id integer, key string, value float, ind int, category string"
      )
  })
})