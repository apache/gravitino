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

package org.apache.gravitino.s3.credential;

import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Verifies the {@code s3:prefix} condition for {@code ListBucket} statements. By default it is
 * scoped to the location and its descendants, so a vended credential cannot enumerate keys in
 * adjacent locations sharing the same string prefix (e.g. {@code path/to/table_new} for location
 * {@code path/to/table}). When the location prefix is enabled (fileset catalogs), the bare location
 * prefix is additionally allowed so a directory-root {@code getFileStatus} HEAD returns 404.
 */
public class TestS3PolicyPrefix {

  @Test
  void testListPrefixesExcludeAdjacentLocations() {
    // Default (secure): the bare path "path/to/table" must NOT be allowed, otherwise ListBucket
    // with that prefix would enumerate keys under the sibling "path/to/table_new".
    List<String> prefixes = S3TokenGenerator.listPrefixes("path/to/table", false);
    Assertions.assertEquals(2, prefixes.size());
    Assertions.assertTrue(prefixes.contains("path/to/table/"));
    Assertions.assertTrue(prefixes.contains("path/to/table/*"));
    Assertions.assertFalse(prefixes.contains("path/to/table"));

    // The IRSA generator shares the same scoping rule.
    Assertions.assertEquals(
        prefixes, AwsIrsaCredentialGenerator.listPrefixes("path/to/table", false));
  }

  @Test
  void testListPrefixesIncludeLocationPrefix() {
    // With the location prefix enabled, the bare path is added so the directory-root HEAD returns
    // 404 instead of 403, while the descendant prefixes remain.
    List<String> prefixes = S3TokenGenerator.listPrefixes("path/to/table", true);
    Assertions.assertEquals(3, prefixes.size());
    Assertions.assertTrue(prefixes.contains("path/to/table"));
    Assertions.assertTrue(prefixes.contains("path/to/table/"));
    Assertions.assertTrue(prefixes.contains("path/to/table/*"));

    Assertions.assertEquals(
        prefixes, AwsIrsaCredentialGenerator.listPrefixes("path/to/table", true));
  }

  @Test
  void testListPrefixesWithTrailingSlash() {
    // A trailing slash already denotes a directory; enabling the location prefix adds nothing new.
    List<String> prefixes = S3TokenGenerator.listPrefixes("path/to/table/", true);
    Assertions.assertEquals(2, prefixes.size());
    Assertions.assertTrue(prefixes.contains("path/to/table/"));
    Assertions.assertTrue(prefixes.contains("path/to/table/*"));
  }

  @Test
  void testListPrefixesForBucketRoot() {
    // For the bucket root the empty prefix must be preserved so listing the whole bucket still
    // works; "/*" alone would not match the empty list prefix.
    List<String> prefixes = S3TokenGenerator.listPrefixes("", true);
    Assertions.assertEquals(2, prefixes.size());
    Assertions.assertTrue(prefixes.contains(""));
    Assertions.assertTrue(prefixes.contains("/*"));
  }
}
