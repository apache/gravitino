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
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.maintenance.injob;

import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link MetricsJson}. */
class TestMetricsJson {

  @Test
  void testParseNumericValues() {
    Map<String, Object> metrics =
        MetricsJson.parseNumericValues(
            "{\"custom-data-file-mse\":1.5,\"custom-delete-file-number\":3,\"name\":\"x\"}");
    Assertions.assertEquals(1.5D, metrics.get("custom-data-file-mse"));
    Assertions.assertEquals(3L, metrics.get("custom-delete-file-number"));
    Assertions.assertFalse(metrics.containsKey("name"));
  }

  @Test
  void testBlankOrInvalidReturnsEmpty() {
    Assertions.assertTrue(MetricsJson.parseNumericValues(null).isEmpty());
    Assertions.assertTrue(MetricsJson.parseNumericValues("").isEmpty());
    Assertions.assertTrue(MetricsJson.parseNumericValues("not-json").isEmpty());
  }
}
