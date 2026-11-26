/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.s3.fs;

import java.util.Map;
import org.apache.gravitino.catalog.hadoop.fs.Constants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests the S3A settings that {@link S3FileSystemProvider} adds to the user configuration. */
public class TestS3FileSystemProviderConfig {

  private final S3FileSystemProvider provider = new S3FileSystemProvider();

  @Test
  void testDirectoryMarkersAreDeletedByDefault() {
    Map<String, String> configs = provider.additionalS3Config(Map.of());

    Assertions.assertEquals("delete", configs.get(Constants.S3_DIRECTORY_MARKER_RETENTION));
  }

  @Test
  void testUserDirectoryMarkerPolicyIsKept() {
    Map<String, String> configs =
        provider.additionalS3Config(Map.of(Constants.S3_DIRECTORY_MARKER_RETENTION, "keep"));

    Assertions.assertEquals("keep", configs.get(Constants.S3_DIRECTORY_MARKER_RETENTION));
  }

  @Test
  void testDefaultRetries() {
    Map<String, String> configs =
        provider.additionalS3Config(Map.of(Constants.S3_RETRY_LIMIT, "5"));

    Assertions.assertEquals("5", configs.get(Constants.S3_RETRY_LIMIT));
    Assertions.assertEquals(
        Constants.DEFAULT_RETRY_LIMIT, configs.get(Constants.S3_MAX_ERROR_RETRIES));
    Assertions.assertEquals(
        Constants.DEFAULT_RETRY_LIMIT, configs.get(Constants.S3_RETRY_THROTTLE_LIMIT));
    Assertions.assertEquals(
        Constants.DEFAULT_CONNECTION_TIMEOUT, configs.get(Constants.S3_ESTABLISH_TIMEOUT));
  }
}
