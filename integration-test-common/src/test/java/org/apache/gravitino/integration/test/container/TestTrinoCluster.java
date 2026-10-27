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
package org.apache.gravitino.integration.test.container;

import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests how {@link TrinoCluster} reads Trino statement responses; nothing here starts Trino. */
public class TestTrinoCluster {

  @Test
  void testQueuedPageHasNextUriAndNoValue() {
    String queued =
        "{\"id\":\"q1\",\"infoUri\":\"http://localhost:8080/ui/query.html?q1\","
            + "\"nextUri\":\"http://localhost:8080/v1/statement/queued/q1/x/1\","
            + "\"stats\":{\"state\":\"QUEUED\"}}";

    Assertions.assertEquals(
        Optional.of("http://localhost:8080/v1/statement/queued/q1/x/1"),
        TrinoCluster.nextUri(queued));
    Assertions.assertEquals(Optional.empty(), TrinoCluster.firstValue(queued));
  }

  @Test
  void testDataPageHasTheCount() {
    String data =
        "{\"id\":\"q1\",\"nextUri\":\"http://localhost:8080/v1/statement/executing/q1/y/2\","
            + "\"columns\":[{\"name\":\"_col0\",\"type\":\"bigint\"}],\"data\": [ [2] ],"
            + "\"stats\":{\"state\":\"RUNNING\"}}";

    Assertions.assertEquals(Optional.of(2), TrinoCluster.firstValue(data));
  }

  @Test
  void testLastPageHasNoNextUri() {
    String finished = "{\"id\":\"q1\",\"stats\":{\"state\":\"FINISHED\"}}";

    Assertions.assertEquals(Optional.empty(), TrinoCluster.nextUri(finished));
    Assertions.assertEquals(Optional.empty(), TrinoCluster.firstValue(finished));
  }
}
