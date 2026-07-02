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
package org.apache.gravitino.iceberg.service.sign;

import com.google.common.collect.ImmutableSet;
import java.net.URI;
import java.util.Collections;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.requests.ImmutableRemoteSignRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestRemoteSignSupport {

  @Test
  void testNormalizeS3Uri() {
    Assertions.assertEquals(
        "s3://bucket/db/tbl/data/file.parquet",
        RemoteSignSupport.normalize(
            RemoteSignSupport.Provider.S3, URI.create("s3://bucket/db/tbl/data/file.parquet")));
  }

  @Test
  void testValidateWithinPrefixesRejectsOutsidePrefix() {
    ForbiddenException exception =
        Assertions.assertThrows(
            ForbiddenException.class,
            () ->
                RemoteSignSupport.validateWithinPrefixes(
                    RemoteSignSupport.Provider.S3,
                    URI.create("s3://other-bucket/db/tbl/data/file.parquet"),
                    ImmutableSet.of("s3://bucket/db/tbl")));
    Assertions.assertTrue(exception.getMessage().contains("outside allowed table locations"));
  }

  @Test
  void testUnsupportedProvider() {
    RemoteSignSupport support =
        new RemoteSignSupport(
            new org.apache.gravitino.iceberg.common.IcebergConfig(Collections.emptyMap()));
    UnsupportedOperationException exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () ->
                support.sign(
                    ImmutableRemoteSignRequest.builder()
                        .region("us-east-1")
                        .method("PUT")
                        .uri(URI.create("gs://bucket/object"))
                        .provider("gcs")
                        .headers(Collections.emptyMap())
                        .build(),
                    new S3SecretKeyCredential("access-key", "secret-key")));
    Assertions.assertTrue(exception.getMessage().contains("gcs"));
  }
}
