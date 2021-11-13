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


test_that("can execute merge with update on condition", {
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
    SparkR::join(
      source,
      SparkR::expr("source.id = target.id") & SparkR::column("target.ind") == -1,
      "left"
    ) %>%
    SparkR::select(c(
      to_fully_qualified_columns(preserved_columns, "target"),
      expected_long
    ))

  # With Column condition and Column set
  withr::with_file(path, {
    target <- test_path_target(path)

    target %>%
      dlt_merge(source, "source.id = target.id") %>%
      dlt_when_matched_update(
        list(long = SparkR::column("source.long")),
        SparkR::column("target.ind") == -1
      ) %>%
      dlt_execute()

    target %>%
      dlt_to_df() %>%
      expect_sdf_equivalent(expected)
  })

  # With character condition and character set
  withr::with_file(path, {
    target <- test_path_target(path)

    target %>%
      dlt_merge(
        source,
        SparkR::column("source.id") == SparkR::column("target.id")
      ) %>%
      dlt_when_matched_update(
        c(long = "source.long"),
        "target.ind == -1"
      ) %>%
      dlt_execute()

    target %>%
      dlt_to_df() %>%
      expect_sdf_equivalent(expected)
  })

  # With Column condition and character set
  withr::with_file(path, {
    test_data() %>%
      dlt_write(path)

    target <- dlt_for_path(path) %>%
      dlt_alias("target")

    target %>%
      dlt_merge(source, "source.id = target.id") %>%
      dlt_when_matched_update(
        list(long = "source.long"),
        SparkR::column("target.ind") == -1
      ) %>%
      dlt_execute()

    target %>%
      dlt_to_df() %>%
      expect_sdf_equivalent(expected)
  })

  # With character condition and Column set
  withr::with_file(path, {
    target <- test_path_target(path)

    target %>%
      dlt_merge(source, "source.id = target.id") %>%
      dlt_when_matched_update(
        list(long = SparkR::column("source.long")),
        "target.ind = -1"
      ) %>%
      dlt_execute()

    target %>%
      dlt_to_df() %>%
      expect_sdf_equivalent(expected)
  })
})


test_that("can execute merge with basic update all", {
  path <- delta_test_tempfile()

  source <- test_data("source", TRUE)
  target <- test_data("target", TRUE)

  cond <- SparkR::expr("source.id = target.id")

  matched <- target %>%
    SparkR::join(
      source,
      cond,
      "leftanti"
    )

  not_matched <- source %>%
    SparkR::join(
      target,
      cond,
      "leftsemi"
    )

  expected <- matched %>%
    SparkR::unionAll(not_matched)

  withr::with_file(path, {
    target <- test_path_target(path)

    target %>%
      dlt_merge(
        source,
        SparkR::column("source.id") == SparkR::column("target.id")
      ) %>%
      dlt_when_matched_update_all() %>%
      dlt_execute()

    target %>%
      dlt_to_df() %>%
      expect_sdf_equivalent(expected)
  })
})


test_that("can execute merge with update all on condition", {
  path <- delta_test_tempfile()

  source <- test_data("source", TRUE)
  target <- test_data("target", TRUE)

  cond <- SparkR::expr("source.id = target.id AND source.ind = target.ind")

  matched <- target %>%
    SparkR::join(
      source,
      cond,
      "leftanti"
    )

  not_matched <- source %>%
    SparkR::join(
      target,
      cond,
      "leftsemi"
    )

  expected <- matched %>%
    SparkR::unionAll(not_matched)

  # With character condition
  withr::with_file(path, {
    target <- test_path_target(path)

    target %>%
      dlt_merge(
        source,
        SparkR::column("source.id") == SparkR::column("target.id")
      ) %>%
      dlt_when_matched_update_all("source.ind = target.ind") %>%
      dlt_execute()

    target %>%
      dlt_to_df() %>%
      expect_sdf_equivalent(expected)
  })

  # With Column condition
  withr::with_file(path, {
    target <- test_path_target(path)

    target %>%
      dlt_merge(
        source,
        SparkR::column("source.id") == SparkR::column("target.id")
      ) %>%
      dlt_when_matched_update_all(
        SparkR::expr("source.ind = target.ind")
      ) %>%
      dlt_execute()

    target %>%
      dlt_to_df() %>%
      expect_sdf_equivalent(expected)
  })
})


test_that("can execute merge with basic delete", {
  path <- delta_test_tempfile()

  source <- test_data("source", TRUE)

  expected <- test_data("target", TRUE) %>%
    SparkR::join(
      source,
      SparkR::expr("source.id = target.id"),
      "leftanti"
    )


  withr::with_file(path, {
    target <- test_path_target(path)

    target %>%
      dlt_merge(
        source,
        "source.id = target.id"
      ) %>%
      dlt_when_matched_delete() %>%
      dlt_execute()

    target %>%
      dlt_to_df() %>%
      expect_sdf_equivalent(expected)
  })
})


test_that("can execute merge with delete on condition", {
  path <- delta_test_tempfile()

  source <- test_data("source", TRUE)

  expected <- test_data("target", TRUE) %>%
    SparkR::join(
      source,
      SparkR::expr("source.id = target.id AND source.ind != target.ind"),
      "leftanti"
    )

  # With character condition
  withr::with_file(path, {
    target <- test_path_target(path)

    target %>%
      dlt_merge(
        source,
        "source.id = target.id"
      ) %>%
      dlt_when_matched_delete("source.ind != target.ind") %>%
      dlt_execute()

    target %>%
      dlt_to_df() %>%
      expect_sdf_equivalent(expected)
  })


  # With column condition
  withr::with_file(path, {
    target <- test_path_target(path)

    target %>%
      dlt_merge(
        source,
        SparkR::column("source.id") == SparkR::column("target.id")
      ) %>%
      dlt_when_matched_delete(
        SparkR::column("source.ind") != SparkR::column("target.ind")
      ) %>%
      dlt_execute()

    target %>%
      dlt_to_df() %>%
      expect_sdf_equivalent(expected)
  })
})


test_that("can execute merge with basic insert on not matched", {
  path <- delta_test_tempfile()

  source <- test_data("source", TRUE)
  target <- test_data("target", TRUE)

  exprs <- c(
    id = "source.id AS id",
    key = "'x' AS key", val = "-1 AS val",
    ind = "0 AS ind", category = "'FOO' AS category",
    lat = "source.lat AS lat", long = "source.long AS long"
  )

  expected <- source %>%
    SparkR::join(
      target, SparkR::expr("source.id = target.id"), "leftanti"
    ) %>%
    SparkR::select(lapply(exprs, SparkR::expr)) %>%
    SparkR::unionAll(target, .)


  # With character set
  withr::with_file(path, {
    target <- test_path_target(path)

    target %>%
      dlt_merge(source, SparkR::expr("source.id = target.id")) %>%
      dlt_when_not_matched_insert(exprs) %>%
      dlt_execute()

    target %>%
      dlt_to_df() %>%
      expect_sdf_equivalent(expected)
  })

  # With Column set
  withr::with_file(path, {
    target <- test_path_target(path)

    target %>%
      dlt_merge(source, SparkR::expr("source.id = target.id")) %>%
      dlt_when_not_matched_insert(lapply(exprs, SparkR::expr)) %>%
      dlt_execute()

    target %>%
      dlt_to_df() %>%
      expect_sdf_equivalent(expected)
  })
})


test_that("can execute merge with insert on not matched with conditon", {
  path <- delta_test_tempfile()

  source <- test_data("source", TRUE)
  target <- test_data("target", TRUE)

  exprs <- c(
    id = "source.id AS id",
    key = "'x' AS key", val = "-1 AS val",
    ind = "0 AS ind", category = "'FOO' AS category",
    lat = "source.lat AS lat", long = "source.long AS long"
  )

  expected <- source %>%
    SparkR::where(SparkR::expr("key = 'd'")) %>%
    SparkR::join(
      target, SparkR::expr("source.id = target.id"), "leftanti"
    ) %>%
    SparkR::select(lapply(exprs, SparkR::expr)) %>%
    SparkR::unionAll(target, .)


  # With character set and Column condition
  withr::with_file(path, {
    target <- test_path_target(path)

    target %>%
      dlt_merge(source, SparkR::expr("source.id = target.id")) %>%
      dlt_when_not_matched_insert(exprs, SparkR::expr("key = 'd'")) %>%
      dlt_execute()

    target %>%
      dlt_to_df() %>%
      expect_sdf_equivalent(expected)
  })

  # With list set and character condition
  withr::with_file(path, {
    target <- test_path_target(path)

    target %>%
      dlt_merge(source, SparkR::expr("source.id = target.id")) %>%
      dlt_when_not_matched_insert(lapply(exprs, SparkR::expr), "key = 'd'") %>%
      dlt_execute()

    target %>%
      dlt_to_df() %>%
      expect_sdf_equivalent(expected)
  })
})


test_that("can execute merge with insert all on not matched", {
  path <- delta_test_tempfile()

  source <- test_data("source", TRUE)
  target <- test_data("target", TRUE)

  not_matched <- source %>%
    SparkR::join(
      target,
      SparkR::expr("source.id = target.id"),
      "leftanti"
    )

  expected <- target %>%
    SparkR::unionAll(not_matched)

  withr::with_file(path, {
    target <- test_path_target(path)

    target %>%
      dlt_merge(source, SparkR::expr("source.id = target.id")) %>%
      dlt_when_not_matched_insert_all() %>%
      dlt_execute()

    target %>%
      dlt_to_df() %>%
      expect_sdf_equivalent(expected)
  })
})


test_that("can execute merge with insert all on not matched with condition", {
  path <- delta_test_tempfile()

  source <- test_data("source", TRUE)
  target <- test_data("target", TRUE)

  not_matched <- source %>%
    SparkR::join(
      target,
      SparkR::expr("source.id = target.id"),
      "leftanti"
    ) %>%
    SparkR::where(SparkR::column("source.key") == "d")

  expected <- target %>%
    SparkR::unionAll(not_matched)


  # With Column merge and Column insert condtion
  withr::with_file(path, {
    target <- test_path_target(path)

    target %>%
      dlt_merge(source, SparkR::expr("source.id = target.id")) %>%
      dlt_when_not_matched_insert_all(SparkR::column("source.key") == "d") %>%
      dlt_execute()

    target %>%
      dlt_to_df() %>%
      expect_sdf_equivalent(expected)
  })

  # With character merge and character insert condition
  withr::with_file(path, {
    target <- test_path_target(path)

    target %>%
      dlt_merge(source, "source.id = target.id") %>%
      dlt_when_not_matched_insert_all("source.key = 'd'") %>%
      dlt_execute()

    target %>%
      dlt_to_df() %>%
      expect_sdf_equivalent(expected)
  })
})


test_that("merge preserves target", {
  path <- delta_test_tempfile()

  source <- test_data("source", TRUE)
  target <- test_data("target", TRUE)

  withr::with_file(path, {
    target <- test_path_target(path)

    target %>%
      dlt_merge(source, "source.id = target.id") %>%
      dlt_when_matched_update_all() %>%
      dlt_execute() %>%
      expect_invisible() %>%
      expect_s4_class("DeltaTable") %>%
      expect_jdt_equal(target)
  })
})
