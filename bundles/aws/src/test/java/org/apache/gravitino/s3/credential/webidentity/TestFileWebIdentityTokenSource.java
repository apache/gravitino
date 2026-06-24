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
package org.apache.gravitino.s3.credential.webidentity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestFileWebIdentityTokenSource {

  @Test
  void nameIsFile() {
    assertEquals("file", new FileWebIdentityTokenSource().name());
  }

  @Test
  void readsTokenFromConfiguredPath(@TempDir Path dir) throws IOException {
    Path tokenFile = dir.resolve("token");
    Files.write(tokenFile, "abc123".getBytes(StandardCharsets.UTF_8));

    FileWebIdentityTokenSource source = new FileWebIdentityTokenSource();
    source.initialize(
        Collections.singletonMap(WebIdentityTokenSourceConfig.FILE_PATH, tokenFile.toString()));

    assertEquals("abc123", source.getToken());
  }

  @Test
  void trimsTrailingWhitespace(@TempDir Path dir) throws IOException {
    Path tokenFile = dir.resolve("token");
    Files.write(tokenFile, "abc123\n".getBytes(StandardCharsets.UTF_8));

    FileWebIdentityTokenSource source = new FileWebIdentityTokenSource();
    source.initialize(
        Collections.singletonMap(WebIdentityTokenSourceConfig.FILE_PATH, tokenFile.toString()));

    assertEquals("abc123", source.getToken());
  }

  @Test
  void rereadsOnEachCallSoRotatedTokensArePickedUp(@TempDir Path dir) throws IOException {
    Path tokenFile = dir.resolve("token");
    Files.write(tokenFile, "first".getBytes(StandardCharsets.UTF_8));

    FileWebIdentityTokenSource source = new FileWebIdentityTokenSource();
    source.initialize(
        Collections.singletonMap(WebIdentityTokenSourceConfig.FILE_PATH, tokenFile.toString()));
    assertEquals("first", source.getToken());

    Files.write(tokenFile, "second".getBytes(StandardCharsets.UTF_8));
    assertEquals("second", source.getToken());
  }

  @Test
  void initializeFailsWhenNeitherPropertyNorEnvIsSet() {
    assumeTrue(
        StringUtils.isBlank(
            System.getenv(FileWebIdentityTokenSource.AWS_WEB_IDENTITY_TOKEN_FILE_ENV)));

    FileWebIdentityTokenSource source = new FileWebIdentityTokenSource();

    IllegalStateException error =
        assertThrows(IllegalStateException.class, () -> source.initialize(Collections.emptyMap()));
    assertTrue(error.getMessage().contains(WebIdentityTokenSourceConfig.FILE_PATH));
  }

  @Test
  void getTokenFailsWhenFileMissing(@TempDir Path dir) {
    Path tokenFile = dir.resolve("missing-token");

    FileWebIdentityTokenSource source = new FileWebIdentityTokenSource();
    source.initialize(
        Collections.singletonMap(WebIdentityTokenSourceConfig.FILE_PATH, tokenFile.toString()));

    IllegalStateException error = assertThrows(IllegalStateException.class, source::getToken);
    assertTrue(error.getMessage().contains("does not exist"));
  }

  @Test
  void getTokenFailsWhenFileIsEmpty(@TempDir Path dir) throws IOException {
    Path tokenFile = dir.resolve("empty-token");
    Files.write(tokenFile, new byte[0]);

    FileWebIdentityTokenSource source = new FileWebIdentityTokenSource();
    source.initialize(
        Collections.singletonMap(WebIdentityTokenSourceConfig.FILE_PATH, tokenFile.toString()));

    IllegalStateException error = assertThrows(IllegalStateException.class, source::getToken);
    assertTrue(error.getMessage().contains("empty"));
  }

  @Test
  void getTokenWrapsReadFailureAsIllegalStateException(@TempDir Path dir) {
    FileWebIdentityTokenSource source = new FileWebIdentityTokenSource();
    source.initialize(
        Collections.singletonMap(WebIdentityTokenSourceConfig.FILE_PATH, dir.toString()));

    IllegalStateException error = assertThrows(IllegalStateException.class, source::getToken);
    assertTrue(error.getMessage().contains("Failed to read WebIdentity token file"));
    assertTrue(error.getCause() instanceof IOException);
  }
}
