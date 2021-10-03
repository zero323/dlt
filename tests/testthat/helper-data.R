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

#' Load data from test path, with optional alias
#'
#' @noRd
test_data <- function(name = c("target", "source"), alias = FALSE) {
  name <- match.arg(name)

  finalize <- if (alias) {
    function(x) SparkR::alias(x, name)
  } else {
    identity
  }

  name %>%
    paste0(".parquet") %>%
    testthat::test_path("data", .) %>%
    SparkR::read.parquet() %>%
    finalize()
}


#' Write test target to delta and return corresponding delta table
#'
#' @noRd
test_path_target <- function(path) {
  test_data() %>%
    dlt_write(path)

  dlt_for_path(path) %>%
    dlt_alias("target")
}
