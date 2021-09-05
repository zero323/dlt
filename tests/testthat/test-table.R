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
