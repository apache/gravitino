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

package org.apache.gravitino.gcs.credential;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGCSTokenProvider {

  @Test
  void testGetAllResources() {
    Map<String, List<String>> checkResults =
        ImmutableMap.of(
            "a/b", Arrays.asList("a", "a/", "a/b"),
            "a/b/", Arrays.asList("a", "a/", "a/b"),
            "a", Arrays.asList("a"),
            "a/", Arrays.asList("a"),
            "", Arrays.asList(""),
            "/", Arrays.asList(""));

    checkResults.forEach(
        (key, value) -> {
          List<String> parentResources = GCSTokenProvider.getAllResources(key);
          Assertions.assertArrayEquals(value.toArray(), parentResources.toArray());
        });
  }

  @Test
  void testNormalizePath() {
    Map<String, String> checkResults =
        ImmutableMap.of(
            "/a/b/", "a/b/",
            "/a/b", "a/b/",
            "a/b", "a/b/",
            "a/b/", "a/b/",
            "/", "",
            "", "");

    checkResults.forEach(
        (k, v) -> {
          String normalizedPath = GCSTokenProvider.normalizeUriPath(k);
          Assertions.assertEquals(v, normalizedPath);
        });
  }
}
