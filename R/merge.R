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

#' @include generics.R package.R utils.R table.R
NULL


#' S4 class that represents pending merge operation
#'
#' @family  DeltaMergeBuilder functions
#' @rdname DeltaMergeBuilder
#' @docType class
#'
#' @slot jmb A Java object reference to the backing DeltaMergeBuilder
#' @slot .target DeltaTable. A reference to the target table
#'
#' @note DeltaMergeBuilder, since 1.0.0
setClass("DeltaMergeBuilder", slots = c(jmb = "jobj", .target = "DeltaTable"))


#' A helper method that invokes new for DeltaMergeBuilder
#'
#' @noRd
newDeltaMergeBuilder <- function(x, .target) { # nolint
  new("DeltaMergeBuilder", jmb = x, .target = .target)
}


#' Execute merge operation on this builder
#'
#' @param bldr DeltaMergeBuilder
#' @returns NULL, invisibly
#'
#' @name dlt_execute
#' @rdname dlt_execute
#' @aliases dlt_execute,DeltaMergeBuilder-method
#'
#' @export
#' @note dlt_execute, since 1.0.0
setMethod(
  "dlt_execute",
  signature(bldr = "DeltaMergeBuilder"),
  function(bldr) {
    sparkR.callJMethod(bldr@jmb, "execute")
    invisible(bldr@.target)
  }
)


#' Update rows for table using set rules
#'
#' @param dmb DeltaMergeBuilder
#' @param set character or list
#' @param condition optional, character or Column
#' @returns DeltaMergeBuilder
#'
#' @name dlt_when_matched_update
#' @rdname dlt_when_matched_update
#' @aliases dlt_when_matched_update,DeltaMergeBuilder,characterOrList,missing-method
#'
#' @export
#' @note dlt_when_matched_update, since 1.0.0
setMethod(
  "dlt_when_matched_update",
  signature(dmb = "DeltaMergeBuilder", set = "characterOrList", condition = "missing"),
  function(dmb, set) {
    sparkR.callJMethod(dmb@jmb, "whenMatched") %>%
      sparkR.callJMethod("update", to_expression_env(set)) %>%
      newDeltaMergeBuilder(dmb@.target)
  }
)


#' @rdname dlt_when_matched_update
#' @aliases dlt_when_matched_update,DeltaMergeBuilder,characterOrList,Column-method
#'
#' @export
setMethod(
  "dlt_when_matched_update",
  signature(dmb = "DeltaMergeBuilder", set = "characterOrList", condition = "Column"),
  function(dmb, set, condition) {
    sparkR.callJMethod(dmb@jmb, "whenMatched", condition@jc) %>%
      sparkR.callJMethod("update", to_expression_env(set)) %>%
      newDeltaMergeBuilder(dmb@.target)
  }
)


#' @rdname dlt_when_matched_update
#' @aliases dlt_when_matched_update,DeltaMergeBuilder,characterOrList,character-method
#'
#' @export
setMethod(
  "dlt_when_matched_update",
  signature(dmb = "DeltaMergeBuilder", set = "characterOrList", condition = "character"),
  function(dmb, set, condition) {
    validate_is_scalar_like_character(condition)

    dlt_when_matched_update(dmb, set, SparkR::expr(condition))
  }
)


#' Update all columns of the matched table
#'
#' @param dmb DeltaMergeBuilder
#' @param condition optional, character or Column
#' @returns DeltaMergeBuilder
#'
#' @name dlt_when_matched_update_all
#' @rdname dlt_when_matched_update_all
#' @aliases dlt_when_matched_update_all,DeltaMergeBuilder,missing-method
#'
#' @export
#' @note dlt_when_matched_update_all, since 1.0.0
setMethod(
  "dlt_when_matched_update_all",
  signature(dmb = "DeltaMergeBuilder", condition = "missing"),
  function(dmb) {
    sparkR.callJMethod(dmb@jmb, "whenMatched") %>%
      sparkR.callJMethod("updateAll") %>%
      newDeltaMergeBuilder(dmb@.target)
  }
)


#' @rdname dlt_when_matched_update_all
#' @aliases dlt_when_matched_update_all,DeltaMergeBuilder,Column-method
#'
#' @export
setMethod(
  "dlt_when_matched_update_all",
  signature(dmb = "DeltaMergeBuilder", condition = "Column"),
  function(dmb, condition) {
    sparkR.callJMethod(dmb@jmb, "whenMatched", condition@jc) %>%
      sparkR.callJMethod("updateAll") %>%
      newDeltaMergeBuilder(dmb@.target)
  }
)


#' @rdname dlt_when_matched_update_all
#' @aliases dlt_when_matched_update_all,DeltaMergeBuilder,character-method
#'
#' @export
setMethod(
  "dlt_when_matched_update_all",
  signature(dmb = "DeltaMergeBuilder", condition = "character"),
  function(dmb, condition) {
    validate_is_scalar_like_character(condition)

    dlt_when_matched_update_all(dmb, SparkR::expr(condition))
  }
)


#' Delete row from the table
#'
#' @param dmb DeltaMergeBuilder
#' @param condition optional, character or Column
#' @returns DeltaMergeBuilder
#'
#' @name dlt_when_matched_delete
#' @rdname dlt_when_matched_delete
#' @aliases dlt_when_matched_delete,DeltaMergeBuilder,missing-method
#'
#' @export
#' @note dlt_when_matched_delete, since 1.0.0
setMethod(
  "dlt_when_matched_delete",
  signature(dmb = "DeltaMergeBuilder", condition = "missing"),
  function(dmb) {
    sparkR.callJMethod(
      dmb@jmb,
      "whenMatched"
    ) %>%
      sparkR.callJMethod("delete") %>%
      newDeltaMergeBuilder(dmb@.target)
  }
)


#' @rdname dlt_when_matched_delete
#' @aliases dlt_when_matched_delete,DeltaMergeBuilder,Column-method
#'
#' @export
setMethod(
  "dlt_when_matched_delete",
  signature(dmb = "DeltaMergeBuilder", condition = "Column"),
  function(dmb, condition) {
    sparkR.callJMethod(
      dmb@jmb,
      "whenMatched",
      condition@jc
    ) %>%
      sparkR.callJMethod("delete") %>%
      newDeltaMergeBuilder(dmb@.target)
  }
)


#' @rdname dlt_when_matched_delete
#' @aliases dlt_when_matched_delete,DeltaMergeBuilder,character-method
#'
#' @export
setMethod(
  "dlt_when_matched_delete",
  signature(dmb = "DeltaMergeBuilder", condition = "character"),
  function(dmb, condition) {
    validate_is_scalar_like_character(condition)

    dlt_when_matched_delete(dmb, SparkR::expr(condition))
  }
)


#' Insert a new row based the set of rules
#'
#' @param dmb DeltaMergeBuilder
#' @param set character or list
#' @param condition optional, character or Column
#' @returns DeltaMergeBuilder
#'
#' @name dlt_when_not_matched_insert
#' @rdname dlt_when_not_matched_insert
#' @aliases dlt_when_not_matched_insert,DeltaMergeBuilder,characterOrList,missing-method
#'
#' @export
#' @note dlt_when_not_matched_insert, since 1.0.0
setMethod(
  "dlt_when_not_matched_insert",
  signature(dmb = "DeltaMergeBuilder", set = "characterOrList", condition = "missing"),
  function(dmb, set) {
    sparkR.callJMethod(
      dmb@jmb,
      "whenNotMatched"
    ) %>%
      sparkR.callJMethod("insert", to_expression_env(set)) %>%
      newDeltaMergeBuilder(dmb@.target)
  }
)


#' @rdname dlt_when_not_matched_insert
#' @aliases dlt_when_not_matched_insert,DeltaMergeBuilder,characterOrList,Column-method
#'
#' @export
setMethod(
  "dlt_when_not_matched_insert",
  signature(dmb = "DeltaMergeBuilder", set = "characterOrList", condition = "Column"),
  function(dmb, set, condition) {
    sparkR.callJMethod(
      dmb@jmb,
      "whenNotMatched",
      condition@jc
    ) %>%
      sparkR.callJMethod("insert", to_expression_env(set)) %>%
      newDeltaMergeBuilder(dmb@.target)
  }
)


#' @rdname dlt_when_not_matched_insert
#' @aliases dlt_when_not_matched_insert,DeltaMergeBuilder,characterOrList,character-method
#'
#' @export
setMethod(
  "dlt_when_not_matched_insert",
  signature(dmb = "DeltaMergeBuilder", set = "characterOrList", condition = "character"),
  function(dmb, set, condition) {
    validate_is_scalar_like_character(condition)

    dlt_when_not_matched_insert(dmb, set, SparkR::expr(condition))
  }
)


#' Insert a new row if not source not matched
#'
#' @param dmb DeltaMergeBuilder
#' @param condition optional, character or Column
#' @returns DeltaMergeBuilder
#'
#' @name dlt_when_not_matched_insert_all
#' @rdname dlt_when_not_matched_insert_all
#' @aliases dlt_when_not_matched_insert_all,DeltaMergeBuilder,missing-method
#'
#' @export
#' @note dlt_when_not_matched_insert_all, since 1.0.0
setMethod(
  "dlt_when_not_matched_insert_all",
  signature(dmb = "DeltaMergeBuilder", condition = "missing"),
  function(dmb) {
    sparkR.callJMethod(
      dmb@jmb,
      "whenNotMatched"
    ) %>%
      sparkR.callJMethod("insertAll") %>%
      newDeltaMergeBuilder(dmb@.target)
  }
)


#' @rdname dlt_when_not_matched_insert_all
#' @aliases dlt_when_not_matched_insert_all,DeltaMergeBuilder,Column-method
#'
#' @export
setMethod(
  "dlt_when_not_matched_insert_all",
  signature(dmb = "DeltaMergeBuilder", condition = "Column"),
  function(dmb, condition) {
    sparkR.callJMethod(
      dmb@jmb,
      "whenNotMatched",
      condition@jc
    ) %>%
      sparkR.callJMethod("insertAll") %>%
      newDeltaMergeBuilder(dmb@.target)
  }
)


#' @rdname dlt_when_not_matched_insert_all
#' @aliases dlt_when_not_matched_insert_all,DeltaMergeBuilder,character-method
#'
#' @export
setMethod(
  "dlt_when_not_matched_insert_all",
  signature(dmb = "DeltaMergeBuilder", condition = "character"),
  function(dmb, condition) {
    validate_is_scalar_like_character(condition)

    dlt_when_not_matched_insert_all(dmb, SparkR::expr(condition))
  }
)
