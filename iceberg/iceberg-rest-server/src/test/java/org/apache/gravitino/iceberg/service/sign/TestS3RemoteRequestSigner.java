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

import java.net.URI;
import java.util.Collections;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.iceberg.rest.requests.ImmutableRemoteSignRequest;
import org.apache.iceberg.rest.responses.RemoteSignResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestS3RemoteRequestSigner {

  @Test
  void testSignPutRequest() {
    S3RemoteRequestSigner signer =
        new S3RemoteRequestSigner(null, false, RemoteSignSupport.DEFAULT_SIGNATURE_DURATION);
    RemoteSignResponse response =
        signer.sign(
            ImmutableRemoteSignRequest.builder()
                .region("us-east-1")
                .method("PUT")
                .uri(URI.create("s3://bucket/db/tbl/data/file.parquet"))
                .headers(Collections.emptyMap())
                .build(),
            new S3SecretKeyCredential("access-key", "secret-key"));

    Assertions.assertNotNull(response.uri());
    Assertions.assertTrue(response.uri().toString().contains("bucket"));
    Assertions.assertFalse(response.headers().isEmpty());
  }

  @Test
  void testUnsupportedProvider() {
    S3RemoteRequestSigner signer =
        new S3RemoteRequestSigner(null, false, RemoteSignSupport.DEFAULT_SIGNATURE_DURATION);
    UnsupportedOperationException exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () ->
                signer.sign(
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
