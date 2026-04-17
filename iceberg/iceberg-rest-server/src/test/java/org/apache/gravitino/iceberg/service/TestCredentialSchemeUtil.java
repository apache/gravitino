/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.service;

import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCredentialSchemeUtil {

  @Test
  void testS3Family() {
    Assertions.assertEquals(
        Optional.of("s3"), CredentialSchemeUtil.credentialTypePrefixFor("s3://bucket/path"));
    Assertions.assertEquals(
        Optional.of("s3"), CredentialSchemeUtil.credentialTypePrefixFor("s3a://bucket/path"));
    Assertions.assertEquals(
        Optional.of("s3"), CredentialSchemeUtil.credentialTypePrefixFor("s3n://bucket/path"));
  }

  @Test
  void testGcsFamily() {
    Assertions.assertEquals(
        Optional.of("gcs"), CredentialSchemeUtil.credentialTypePrefixFor("gs://bucket/path"));
    Assertions.assertEquals(
        Optional.of("gcs"), CredentialSchemeUtil.credentialTypePrefixFor("gcs://bucket/path"));
  }

  @Test
  void testAzureAdlsFamily() {
    Assertions.assertEquals(
        Optional.of("adls"),
        CredentialSchemeUtil.credentialTypePrefixFor(
            "abfs://container@account.dfs.core.windows.net/path"));
    Assertions.assertEquals(
        Optional.of("adls"),
        CredentialSchemeUtil.credentialTypePrefixFor(
            "abfss://container@account.dfs.core.windows.net/path"));
  }

  @Test
  void testAzureBlobFamily() {
    Assertions.assertEquals(
        Optional.of("azure"),
        CredentialSchemeUtil.credentialTypePrefixFor(
            "wasb://container@account.blob.core.windows.net/path"));
    Assertions.assertEquals(
        Optional.of("azure"),
        CredentialSchemeUtil.credentialTypePrefixFor(
            "wasbs://container@account.blob.core.windows.net/path"));
  }

  @Test
  void testOssFamily() {
    Assertions.assertEquals(
        Optional.of("oss"), CredentialSchemeUtil.credentialTypePrefixFor("oss://bucket/path"));
  }

  @Test
  void testSchemeCaseInsensitive() {
    Assertions.assertEquals(
        Optional.of("s3"), CredentialSchemeUtil.credentialTypePrefixFor("S3://bucket/path"));
    Assertions.assertEquals(
        Optional.of("adls"),
        CredentialSchemeUtil.credentialTypePrefixFor("ABFS://container@account/path"));
    Assertions.assertEquals(
        Optional.of("gcs"), CredentialSchemeUtil.credentialTypePrefixFor("GS://bucket/path"));
  }

  @Test
  void testLocalAndHdfsReturnsEmpty() {
    Assertions.assertEquals(
        Optional.empty(), CredentialSchemeUtil.credentialTypePrefixFor("file:///tmp/warehouse"));
    Assertions.assertEquals(
        Optional.empty(),
        CredentialSchemeUtil.credentialTypePrefixFor("hdfs://localhost:9000/warehouse"));
    Assertions.assertEquals(
        Optional.empty(), CredentialSchemeUtil.credentialTypePrefixFor("/tmp/warehouse"));
  }

  @Test
  void testBlankOrNullReturnsEmpty() {
    Assertions.assertEquals(Optional.empty(), CredentialSchemeUtil.credentialTypePrefixFor(null));
    Assertions.assertEquals(Optional.empty(), CredentialSchemeUtil.credentialTypePrefixFor(""));
    Assertions.assertEquals(Optional.empty(), CredentialSchemeUtil.credentialTypePrefixFor("   "));
  }

  @Test
  void testUnknownSchemeReturnsEmpty() {
    Assertions.assertEquals(
        Optional.empty(), CredentialSchemeUtil.credentialTypePrefixFor("unknown://foo/bar"));
    Assertions.assertEquals(
        Optional.empty(), CredentialSchemeUtil.credentialTypePrefixFor("ftp://host/path"));
  }

  @Test
  void testMalformedUriReturnsEmpty() {
    // URI.create throws IllegalArgumentException on malformed input; we swallow and return empty.
    Assertions.assertEquals(
        Optional.empty(),
        CredentialSchemeUtil.credentialTypePrefixFor("s3://bucket with spaces/path"));
  }
}
