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

#' @include generics.R utils.R
NULL

setOldClass("jobj")

setClassUnion("characterOrList", members = c("character", "list"))


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


#' Set alias for this DeltaTable
#'
#' @param dt DeltaTable
#' @param name character
#' @returns DeltaTable
#'
#' @name dlt_alias
#' @rdname dlt_alias
#' @aliases dlt_alias,DeltaTable,character-method
#'
#' @export
#' @note dlt_alias, since 1.0.0
setMethod(
  "dlt_alias",
  signature(dt = "DeltaTable", name = "character"),
  function(dt, name) {
    new(
      "DeltaTable",
      jdt = sparkR.callJMethod(dt@jdt, "alias", name)
    )
  }
)


#' Delete data from this DataTable
#'
#' Removes data that matches optional condition
#'
#' @param dt DeltaTable
#' @param condition optional, character of Column
#'   If character, interpreted as SQL expression
#'
#' @name dlt_delete
#' @rdname dlt_delete
#' @aliases dlt_delete,DeltaTable,missing-method
#'
#' @export
#' @note dlt_delete, since 1.0.0
setMethod(
  "dlt_delete",
  signature(dt = "DeltaTable", condition = "missing"),
  function(dt) {
    sparkR.callJMethod(dt@jdt, "delete") %>%
      invisible()
  }
)


#' @rdname dlt_delete
#' @aliases dlt_delete,DeltaTable,Column-method
#'
#' @export
setMethod(
  "dlt_delete",
  signature(dt = "DeltaTable", condition = "Column"),
  function(dt, condition) {
    sparkR.callJMethod(
      dt@jdt,
      "delete",
      condition@jc
    ) %>% invisible()
  }
)


#' @rdname dlt_delete
#' @aliases dlt_delete,DeltaTable,character-method
#'
#' @export
setMethod(
  "dlt_delete",
  signature(dt = "DeltaTable", condition = "character"),
  function(dt, condition) {
    dlt_delete(dt, SparkR::expr(condition))
  }
)


#' Update data in this DataTable
#'
#' @param dt DeltaTable
#' @param set named character vector or list of Columns
#' @param condition optional, character or Column
#'
#' @name dlt_update
#' @rdname dlt_update
#' @aliases dlt_update,DeltaTable,characterOrList,missing-method
#'
#' @export
#' @note dlt_update, since 1.0.0
setMethod(
  "dlt_update",
  signature(
    dt = "DeltaTable",
    set = "characterOrList",
    condition = "missing"
  ),
  function(dt, set) {
    sparkR.callJMethod(
      dt@jdt,
      "update",
      to_expression_env(set)
    ) %>% invisible()
  }
)


#' @rdname dlt_update
#' @aliases dlt_update,DeltaTable,characterOrList,Column-method
#'
#' @export
setMethod(
  "dlt_update",
  signature(
    dt = "DeltaTable",
    set = "characterOrList",
    condition = "Column"
  ),
  function(dt, set, condition) {
    sparkR.callJMethod(
      dt@jdt,
      "update",
      condition@jc,
      to_expression_env(set)
    ) %>% invisible()
  }
)


#' @rdname dlt_update
#' @aliases dlt_update,DeltaTable,characterOrList,character-method
#'
#' @export
setMethod(
  "dlt_update",
  signature(
    dt = "DeltaTable",
    set = "characterOrList",
    condition = "character"
  ),
  function(dt, set, condition) {
    dlt_update(dt, set, SparkR::expr(condition))
  }
)


#' Check if path corresponds to a DeltaTable
#'
#' @param identifier character path
#' @returns logical
#'
#' @name dlt_is_delta_table
#' @rdname dlt_is_delta_table
#' @aliases dlt_is_delta_table,character-method
#'
#' @export
#' @note dlt_is_delta_table, since 1.0.0
setMethod(
  "dlt_is_delta_table",
  signature(identifier = "character"),
  function(identifier) {
    invoke_delta_table_static(
      "isDeltaTable",
      active_session(),
      identifier
    )
  }
)


#' Get commit history for this DeltaTable
#'
#' @param dt DeltaTable
#' @param limit optional, numeric
#' @returns SparkDataFrame
#'
#' @name dlt_history
#' @rdname dlt_history
#' @aliases dlt_history,DeltaTable,missing-method
#'
#' @export
#' @note dlt_history, since 1.0.0
setMethod(
  "dlt_history",
  signature(dt = "DeltaTable", limit = "missing"),
  function(dt) {
    new(
      "SparkDataFrame",
      sparkR.callJMethod(dt@jdt, "history"),
      FALSE
    )
  }
)


#' @rdname dlt_history
#' @aliases dlt_history,DeltaTable,numeric-method
#'
#' @export
setMethod(
  "dlt_history",
  signature(dt = "DeltaTable", limit = "numeric"),
  function(dt, limit) {
    new(
      "SparkDataFrame",
      sparkR.callJMethod(dt@jdt, "history", as.integer(limit)),
      FALSE
    )
  }
)


#' Create a DeltaTable from the given parquet table.
#'
#' @param identifier character
#' @param partition_schema optional, character
#' @returns DeltaTable
#'
#' @name dlt_convert_to_delta
#' @rdname dlt_convert_to_delta
#' @aliases dlt_convert_to_delta,character,missing-method
#' @examples
#' \dontrun{
#' library(SparkR)
#' createDataFrame(iris) %>%
#'   write.parquet("/tmp/iris-non-delta")
#'
#' dlt_convert_to_delta("parquet.`/tmp/iris-non-delta`") %>%
#'   dlt_to_df() %>%
#'   showDF()
#' }
#'
#' @export
#' @note dlt_convert_to_delta, since 1.0.0
setMethod(
  "dlt_convert_to_delta",
  signature(identifier = "character", partition_schema = "missing"),
  function(identifier) {
    new(
      "DeltaTable",
      jdt = invoke_delta_table_static(
        "convertToDelta",
        active_session(),
        identifier
      )
    )
  }
)


#' @rdname dlt_convert_to_delta
#' @aliases dlt_convert_to_delta,character,character-method
#'
#' @export
setMethod(
  "dlt_convert_to_delta",
  signature(identifier = "character", partition_schema = "character"),
  function(identifier, partition_schema) {
    new(
      "DeltaTable",
      jdt = invoke_delta_table_static(
        "convertToDelta",
        active_session(),
        identifier,
        partition_schema
      )
    )
  }
)


#' Vacuum this DeltaTable
#'
#' @param dt DataTable
#' @param retention_hours numeric
#' @returns NULL, invisibly
#'
#' @name dlt_vacuum
#' @rdname dlt_vacuum
#' @aliases dlt_vacuum,DeltaTable,missing-method
#'
#' @export
#' @note dlt_vacuum, since 1.0.0
setMethod(
  "dlt_vacuum",
  signature(dt = "DeltaTable", retention_hours = "missing"),
  function(dt) {
    sparkR.callJMethod(dt@jdt, "vacuum") %>%
      invisible()
  }
)


#' @rdname dlt_vacuum
#' @aliases dlt_vacuum,DeltaTable,numeric-method
#'
#' @export
setMethod(
  "dlt_vacuum",
  signature(dt = "DeltaTable", retention_hours = "numeric"),
  function(dt, retention_hours) {
    sparkR.callJMethod(dt@jdt, "vacuum", as.double(retention_hours)) %>% invisible()
  }
)


#' Upgrade protocol version of this DeltaTable
#'
#' @param dt DeltaTable
#' @param reader_version numeric
#' @param writer_version numeric
#' @returns NULL, invisibly
#'
#' @name dlt_upgrade_table_protocol
#' @rdname dlt_upgrade_table_protocol
#' @aliases dlt_upgrade_table_protocol,DeltaTable,numeric,numeric-method
#'
#' @export
#' @note dlt_upgrade_table_protocol, since 1.0.0
setMethod(
  "dlt_upgrade_table_protocol",
  signature(dt = "DeltaTable", reader_version = "numeric", writer_version = "numeric"),
  function(dt, reader_version, writer_version) {
    stopifnot(round(reader_version) == reader_version)
    stopifnot(round(writer_version) == writer_version)

    sparkR.callJMethod(
      dt@jdt,
      "upgradeTableProtocol",
      as.integer(reader_version),
      as.integer(writer_version)
    ) %>% invisible()
  }
)


#' Generate manifest files for this DeltaTable
#'
#' @param dt DeltaTable
#' @param mode character mode for the type of manifest file to be generated
#'
#' @name dlt_generate_manifest
#' @rdname dlt_generate_manifest
#' @aliases dlt_generate_manifest,DeltaTable,character-method
#'
#' @export
#' @note dlt_generate_manifest, since 1.0.0
setMethod(
  "dlt_generate_manifest",
  signature(dt = "DeltaTable", mode = "character"),
  function(dt, mode = c("symlink_format_manifest")) {
    sparkR.callJMethod(
      dt@jdt,
      "generate",
      match.arg(mode)
    ) %>% invisible()
  }
)


#' Merge data from sourc with this DataTable, based on condition
#'
#' @param dt DeltaTable
#' @param source SparkDataFrame
#' @param condition character or Column
#'
#' @name dlt_merge
#' @rdname dlt_merge
#' @aliases dlt_merge,DeltaTable,SparkDataFrame,Column-method
#'
#' @export
#' @note dlt_merge, since 1.0.0
setMethod(
  "dlt_merge",
  signature(
    dt = "DeltaTable",
    source = "SparkDataFrame",
    condition = "Column"
  ),
  function(dt, source, condition) {
    sparkR.callJMethod(
      dt@jdt,
      "merge",
      source@sdf,
      condition@jc
    ) %>% newDeltaMergeBuilder()
  }
)


#' @rdname dlt_merge
#' @aliases dlt_merge,DeltaTable,SparkDataFrame,character-method
#'
#' @export
setMethod(
  "dlt_merge",
  signature(
    dt = "DeltaTable",
    source = "SparkDataFrame",
    condition = "character"
  ),
  function(dt, source, condition) {
    dlt_merge(dt, source, SparkR::expr(condition))
  }
)