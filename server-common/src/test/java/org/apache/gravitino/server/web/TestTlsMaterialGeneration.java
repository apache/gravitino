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

package org.apache.gravitino.server.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestTlsMaterialGeneration {

  @TempDir Path tempDir;

  @Test
  void testGenerateServerKeystore() throws Exception {
    Path path = tempDir.resolve("server.p12");

    TestTlsServerUtils.generateServerKeystore(path);

    KeyStore store = KeyStore.getInstance("PKCS12");
    try (InputStream input = Files.newInputStream(path)) {
      store.load(input, TestTlsServerUtils.TEST_STORE_PASSWORD.toCharArray());
    }

    assertTrue(store.entryInstanceOf("test-server", KeyStore.PrivateKeyEntry.class));

    X509Certificate certificate = (X509Certificate) store.getCertificate("test-server");
    assertNotNull(certificate);
    certificate.checkValidity();

    Path truststorePath = tempDir.resolve("client-truststore.p12");

    TestTlsServerUtils.generateTruststore(
        path, "test-server", tempDir.resolve("server.cer"), truststorePath);

    KeyStore truststore = KeyStore.getInstance("PKCS12");
    try (InputStream input = Files.newInputStream(truststorePath)) {
      truststore.load(input, TestTlsServerUtils.TEST_STORE_PASSWORD.toCharArray());
    }

    assertTrue(truststore.entryInstanceOf("test-server", KeyStore.TrustedCertificateEntry.class));
    assertEquals(certificate, truststore.getCertificate("test-server"));
  }
}
