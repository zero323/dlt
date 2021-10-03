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


test_that("can execute merge with basic update", {
  path <- delta_test_tempfile()

  source <- test_data("source", TRUE)

  expected_long <- SparkR::coalesce(
    SparkR::column("source.long"), SparkR::column("target.long")
  ) %>%
    SparkR::alias("long")

  preserved_columns <- c(
    "id", "key", "val", "ind", "category", "lat"
  )

  expected <- test_data("target", TRUE) %>%
    SparkR::join(source, SparkR::expr("source.id = target.id"), "left") %>%
    SparkR::select(c(
      to_fully_qualified_columns(preserved_columns, "target"),
      expected_long
    ))

  # With character condition and Column set
  withr::with_file(path, {
    target <- test_path_target(path)

    target %>%
      dlt_merge(source, "source.id = target.id") %>%
      dlt_when_matched_update(list(long = SparkR::column("source.long"))) %>%
      dlt_execute()

    target %>%
      dlt_to_df() %>%
      expect_sdf_equivalent(expected)
  })

  # With column condition and character set
  withr::with_file(path, {
    target <- test_path_target(path)

    target %>%
      dlt_merge(
        source,
        SparkR::column("source.id") == SparkR::column("target.id")
      ) %>%
      dlt_when_matched_update(c(long = "source.long")) %>%
      dlt_execute()

    target %>%
      dlt_to_df() %>%
      expect_sdf_equivalent(expected)
  })
})