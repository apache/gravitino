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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests the LocalStack readiness check without Docker. */
public class TestGravitinoLocalStackContainer {

  @Test
  void testS3MustBeAvailableOrRunning() {
    Assertions.assertTrue(
        GravitinoLocalStackContainer.reportsS3Ready(
            "{\"services\": {\"iam\": \"available\", \"s3\": \"available\"}, \"edition\": \"community\"}"));
    Assertions.assertTrue(
        GravitinoLocalStackContainer.reportsS3Ready("{\"services\":{\"s3\":\"running\"}}"));
    Assertions.assertFalse(
        GravitinoLocalStackContainer.reportsS3Ready("{\"services\":{\"s3\":\"starting\"}}"));
    Assertions.assertFalse(
        GravitinoLocalStackContainer.reportsS3Ready("{\"services\":{\"sqs\":\"running\"}}"));
  }
}
