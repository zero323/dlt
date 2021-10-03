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

#' Generate test file name
#'
#' @noRd
delta_test_tempfile <- function() {
  tempfile(pattern = "delta_test_")
}


#' Generate test table name
#'
#' @noRd
delta_test_name <- function(prefix = "delta_test") {
  paste(prefix, stringi::stri_rand_strings(1, 10), sep = "_")
}


#' Set Spark configuration
#'
#' @noRd
set_spark_config <- function(key, value) {
  active_session() %>%
    SparkR::sparkR.callJMethod("conf") %>%
    SparkR::sparkR.callJMethod("set", key, value)
}


#' Unset Spark configuration
#'
#' @noRd
unset_spark_config <- function(key) {
  active_session() %>%
    SparkR::sparkR.callJMethod("conf") %>%
    SparkR::sparkR.callJMethod("unset", key)
}


#' Convert list of character names to list of Columns with given table alias
#'
#' @noRd
to_fully_qualified_columns <- function(x, alias) {
  paste(alias, x, sep = ".") %>%
    lapply(SparkR::column)
}
