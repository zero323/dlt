#
# Copyright 2022 zero323
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

#' @include generics.R package.R utils.R table.R
NULL

#' S4 class that represents pending optimize operation
#'
#' @family  DeltaOptimizeBuilder functions
#' @docType class
#'
#' @slot job A Java object reference to the backing DeltaOptimizeBuilder
#'
#' @examples \dontrun{
#'
#' }
#'
#' @export
#' @note DeltaOptimizeBuilder, since 2.0.0
setClass("DeltaOptimizeBuilder", slots = c(job = "jobj"))


#' A helper method that invokes new for DeltaMergeBuilder
#'
#' @noRd
newDeltaOptimizeBuilder <- function(x) { # nolint
  new("DeltaOptimizeBuilder", job = x)
}


#' Optimize the data layout of the table.
#'
#' @param dob DeltaOptimizeBuilder
#' @returns DeltaOptimizeBuilder
#'
#' @name dlt_where
#' @rdname dlt_where
#' @aliases dlt_where,DeltaOptimizeBuilder,character-method
#'
#' @export
#' @seealso [DeltaOptimizeBuilder-class]
#' @note dlt_where, since 2.0.0
setMethod(
  "dlt_where",
  signature(dob = "DeltaOptimizeBuilder"),
  function(dob, partition_filter) {
    sparkR.callJMethod(
      dob@job,
      "where",
      partition_filter
    )
    dob
  }
)


#' Compact the small files in selected partitions.
#'
#' @param dob DeltaOptimizeBuilder
#' @returns SparkDataFrame containing the OPTIMIZE execution metrics
#'
#' @name dlt_execute_compaction
#' @rdname dlt_execute_compaction
#' @aliases dlt_execute_compaction,DeltaOptimizeBuilder-method
#'
#' @export
#' @seealso [DeltaOptimizeBuilder-class]
#' @note dlt_execute_compaction, since 2.0.0
setMethod(
  "dlt_execute_compaction",
  signature(dob = "DeltaOptimizeBuilder"),
  function(dob) {
    new(
      "SparkDataFrame",
      sparkR.callJMethod(dob@job, "executeCompaction"),
      FALSE
    )
  }
)


#' Z-Order the data in selected partitions using the given columns.
#'
#' @param dob DeltaOptimizeBuilder
#' @param ...  character columns
#' @returns SparkDataFrame containing the OPTIMIZE execution metrics
#'
#' @name dlt_execute_z_order_by
#' @rdname dlt_execute_z_order_by
#'
#' @export
#' @seealso [DeltaOptimizeBuilder-class]
#' @note dlt_execute_z_order_by, since 2.0.0
setMethod(
  "dlt_execute_z_order_by",
  signature(dob = "DeltaOptimizeBuilder"),
  function(dob, ...) {
    cols <- prepare_and_validate_cols(...)

    new(
      "SparkDataFrame",
      sparkR.callJMethod(dob@job, "executeZOrderBy", cols),
      FALSE
    )
  }
)
