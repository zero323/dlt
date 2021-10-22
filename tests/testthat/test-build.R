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
