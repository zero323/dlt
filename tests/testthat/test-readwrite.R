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

test_that("can write and read back", {
  path <- delta_test_tempfile()

  withr::with_file(path, {
    expected <- SparkR::sql("SELECT * FROM range(10)")

    expected %>%
      dlt_write(path)

    actual <- dlt_read(path)

    expect_sdf_equivalent(
      actual, expected
    )
  })
})


test_that("can write and read back with compatibility aliases", {
  path <- delta_test_tempfile()

  withr::with_file(path, {
    expected <- SparkR::sql("SELECT * FROM range(10)")

    expected %>%
      write.delta(path)

    actual <- read.delta(path)

    expect_sdf_equivalent(
      actual, expected
    )
  })
})


test_that("can read with time travel", {
  path <- delta_test_tempfile()

  withr::with_file(path, {
    SparkR::sql("SELECT * FROM range(10)") %>%
      dlt_write(path)

    at_time <- strftime(Sys.time() + 1)
    Sys.sleep(6)

    dlt_for_path(path) %>%
      dlt_delete("id >= 5")

    dlt_read(path) %>%
      SparkR::count() %>%
      expect_equal(5)

    dlt_read(path, versionAsOf = 0) %>%
      SparkR::count() %>%
      expect_equal(10)

    dlt_read(path, timestampAsOf = at_time) %>%
      SparkR::count() %>%
      expect_equal(10)

    dlt_read(path) %>%
      SparkR::count() %>%
      expect_equal(5)
  })
})
