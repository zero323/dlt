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
  input_path <- delta_test_tempfile()
  output_path <- delta_test_tempfile()

  withr::with_file(c(input_path, output_path), {
    target <- test_data("target")
    target %>%
      dlt_write(input_path)

    query <- dlt_read_stream(path = input_path) %>%
      dlt_write_stream(
        path = output_path, queryName = "test", trigger.once = TRUE,
        checkpointLocation = file.path(output_path, "_checkpoints", "test")
      )

    SparkR::awaitTermination(query, 10 * 1000)
    sparkR.callJMethod(query@ssq, "processAllAvailable")

    dlt_read(output_path) %>%
      expect_sdf_equivalent(target)
  })
})
