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

#' Check if SparkDataFrames are equivalent using symmetric diff
#'
#' @noRd
expect_sdf_equivalent <- function(actual, expected) {
  act <- quasi_label(rlang::enquo(actual), arg = "actual")
  exp <- quasi_label(rlang::enquo(expected), arg = "expected")


  actual_minus_expected <- SparkR::exceptAll(actual, expected)

  if (SparkR::count(actual_minus_expected) != 0) {
    fail("Actual contains rows not present in expected")
  }


  expected_minus_actual <- SparkR::exceptAll(expected, actual)

  if (SparkR::count(expected_minus_actual) != 0) {
    fail("Expected contains rows not present in actual")
  }

  succeed()
  invisible(act$val)
}


#' Check if collected SparkDataFrames are equivalent using specified ordering
#'
#' @noRd
expect_collected_sdf_equivalent <- function(actual, expected, ordering = "id") {
  act <- quasi_label(rlang::enquo(actual), arg = "actual")
  exp <- quasi_label(rlang::enquo(expected), arg = "expected")


  expect_equal(
    do.call(SparkR::arrange, c(actual, as.list(ordering))) %>%
      SparkR::collect(),
    do.call(SparkR::arrange, c(expected, as.list(ordering))) %>%
      SparkR::collect()
  )

  invisible(act$val)
}