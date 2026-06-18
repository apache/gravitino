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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestWebIdentityTokenSources {

  @Test
  void defaultsToFileSource(@TempDir Path dir) throws IOException {
    Path tokenFile = dir.resolve("token");
    Files.write(tokenFile, "tok".getBytes(StandardCharsets.UTF_8));

    Map<String, String> props = new HashMap<>();
    props.put(WebIdentityTokenSourceConfig.FILE_PATH, tokenFile.toString());

    WebIdentityTokenSource source = WebIdentityTokenSources.create(props);
    assertInstanceOf(FileWebIdentityTokenSource.class, source);
    assertEquals("tok", source.getToken());
  }

  @Test
  void selectsSourceByName(@TempDir Path dir) throws IOException {
    Path tokenFile = dir.resolve("token");
    Files.write(tokenFile, "tok".getBytes(StandardCharsets.UTF_8));

    Map<String, String> props = new HashMap<>();
    props.put(WebIdentityTokenSourceConfig.SOURCE, "file");
    props.put(WebIdentityTokenSourceConfig.FILE_PATH, tokenFile.toString());

    WebIdentityTokenSource source = WebIdentityTokenSources.create(props);
    assertInstanceOf(FileWebIdentityTokenSource.class, source);
  }

  @Test
  void selectsSourceByNameIgnoringCase(@TempDir Path dir) throws IOException {
    Path tokenFile = dir.resolve("token");
    Files.write(tokenFile, "tok".getBytes(StandardCharsets.UTF_8));

    Map<String, String> props = new HashMap<>();
    props.put(WebIdentityTokenSourceConfig.SOURCE, "FILE");
    props.put(WebIdentityTokenSourceConfig.FILE_PATH, tokenFile.toString());

    WebIdentityTokenSource source = WebIdentityTokenSources.create(props);
    assertInstanceOf(FileWebIdentityTokenSource.class, source);
  }

  @Test
  void throwsForUnknownSource() {
    Map<String, String> props = new HashMap<>();
    props.put(WebIdentityTokenSourceConfig.SOURCE, "no-such-source");

    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> WebIdentityTokenSources.create(props));
    assertTrue(error.getMessage().contains("no-such-source"));
  }

  @Test
  void blankSourcePropertyFallsBackToDefault(@TempDir Path dir) throws IOException {
    Path tokenFile = dir.resolve("token");
    Files.write(tokenFile, "tok".getBytes(StandardCharsets.UTF_8));

    Map<String, String> props = new HashMap<>();
    props.put(WebIdentityTokenSourceConfig.SOURCE, "");
    props.put(WebIdentityTokenSourceConfig.FILE_PATH, tokenFile.toString());

    WebIdentityTokenSource source = WebIdentityTokenSources.create(props);
    assertInstanceOf(FileWebIdentityTokenSource.class, source);
  }

  @Test
  void throwsWhenMultipleSourcesShareTheSameName() {
    Map<String, String> props = new HashMap<>();
    props.put(WebIdentityTokenSourceConfig.SOURCE, "dup");

    WebIdentityTokenSource first = new NamedNoopSource("dup");
    WebIdentityTokenSource second = new NamedNoopSource("dup");

    IllegalStateException error =
        assertThrows(
            IllegalStateException.class,
            () -> WebIdentityTokenSources.create(props, Arrays.asList(first, second)));
    assertTrue(error.getMessage().contains("Multiple WebIdentity token sources"));
    assertTrue(error.getMessage().contains("dup"));
    assertTrue(error.getMessage().contains(NamedNoopSource.class.getName()));
  }

  @Test
  void throwsWhenMultipleSourcesShareTheSameNameIgnoringCase() {
    Map<String, String> props = new HashMap<>();
    props.put(WebIdentityTokenSourceConfig.SOURCE, "dup");

    WebIdentityTokenSource first = new NamedNoopSource("dup");
    WebIdentityTokenSource second = new NamedNoopSource("DUP");

    IllegalStateException error =
        assertThrows(
            IllegalStateException.class,
            () -> WebIdentityTokenSources.create(props, Arrays.asList(first, second)));
    assertTrue(error.getMessage().contains("Multiple WebIdentity token sources"));
    assertTrue(error.getMessage().contains("dup"));
  }

  @Test
  void throwsWhenUnselectedSourcesShareTheSameName() {
    Map<String, String> props = new HashMap<>();
    props.put(WebIdentityTokenSourceConfig.SOURCE, "selected");

    WebIdentityTokenSource selected = new NamedNoopSource("selected");
    WebIdentityTokenSource firstDuplicate = new NamedNoopSource("dup");
    WebIdentityTokenSource secondDuplicate = new NamedNoopSource("dup");

    IllegalStateException error =
        assertThrows(
            IllegalStateException.class,
            () ->
                WebIdentityTokenSources.create(
                    props, Arrays.asList(selected, firstDuplicate, secondDuplicate)));
    assertTrue(error.getMessage().contains("Multiple WebIdentity token sources"));
    assertTrue(error.getMessage().contains("dup"));
  }

  @Test
  void throwsWhenSourceNameIsBlank() {
    IllegalStateException error =
        assertThrows(
            IllegalStateException.class,
            () ->
                WebIdentityTokenSources.create(
                    new HashMap<>(), Arrays.asList(new NamedNoopSource(null))));
    assertTrue(error.getMessage().contains("must return a non-blank name"));
  }

  private static final class NamedNoopSource implements WebIdentityTokenSource {
    private final String name;

    NamedNoopSource(String name) {
      this.name = name;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public void initialize(Map<String, String> properties) {}

    @Override
    public String getToken() {
      return "noop";
    }

    @Override
    public void close() {}
  }
}
