/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.lance.integration.test;

import java.io.IOException;
import java.util.List;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.lance.Dataset;
import org.lance.WriteParams;

/** Creates existing native datasets for registration tests. */
final class LanceDatasetTestUtils {
  private LanceDatasetTestUtils() {}

  static void createDataset(String location) throws IOException {
    try (Dataset ignored =
        Dataset.write()
            .schema(new Schema(List.of(Field.nullable("id", new ArrowType.Int(32, true)))))
            .uri(location)
            .mode(WriteParams.WriteMode.CREATE)
            .execute()) {
      // Close the native dataset before exercising registration through the REST service.
    }
  }
}
