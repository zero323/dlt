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

setOldClass("jobj")


#' S4 class that represents DeltaTable
#'
#' @family DeltaTable functions
#' @rdname DeltaTable
#' @docType class
#'
#' @slot jdt A Java object reference to the backing DeltaTable
#'
#' @note DeltaTable since 1.0.0
setClass("DeltaTable", slots = c(jdt = "jobj"))


#' Get SparkDataFrame corresponding to DeltaTable
#'
#' @param dt DeltaTable
#' @returns SparkDataFrame
#'
#' @name dlt_to_df
#' @rdname dlt_to_df
#' @aliases dlt_to_df,DeltaTable-method
#'
#' @export
#' @note dlt_to_df, since 1.0.0
setMethod(
  "dlt_to_df",
  signature(dt = "DeltaTable"),
  function(dt) {
    new(
      "SparkDataFrame",
      sparkR.callJMethod(dt@jdt, "toDF"),
      FALSE
    )
  }
)


#' Load DeltaTable from metastore
#'
#' Create DeltaTable for a given name
#'
#' @param name character
#' @returns DeltaTable
#'
#' @export
#' @note dlt_for_name, since 1.0.0
dlt_for_name <- function(name) {
  new("DeltaTable",
    jdt = invoke_delta_table_static(
      "forName",
      name
    )
  )
}


#' Load DeltaTable from a path
#'
#' Create DeltaTable using given path
#'
#' @param path character
#' @returns DeltaTable
#'
#' @export
#' @note dlt_for_path, since 1.0.0
dlt_for_path <- function(path) {
  new("DeltaTable",
    jdt = invoke_delta_table_static(
      "forPath",
      path
    )
  )
}
