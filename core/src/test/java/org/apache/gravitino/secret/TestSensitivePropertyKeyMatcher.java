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
package org.apache.gravitino.secret;

import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSensitivePropertyKeyMatcher {

  @AfterEach
  void resetMatcher() {
    SensitivePropertyKeyMatcher.resetToDefaults();
  }

  @Test
  void testBuiltinSensitiveKeywordsStillMatch() {
    Assertions.assertTrue(SensitivePropertyKeyMatcher.isSensitive("jdbc-password"));
    Assertions.assertTrue(SensitivePropertyKeyMatcher.isSensitive("s3-secret-access-key"));
    Assertions.assertFalse(SensitivePropertyKeyMatcher.isSensitive("warehouse"));
  }

  @Test
  void testConfiguredTypoSubstringMatches() {
    SensitivePropertyKeyMatcher.configure(List.of("passwrod", "secert"), 0);
    Assertions.assertTrue(SensitivePropertyKeyMatcher.isSensitive("jdbc-passwrod"));
    Assertions.assertTrue(SensitivePropertyKeyMatcher.isSensitive("catalog.secert"));
    Assertions.assertFalse(SensitivePropertyKeyMatcher.isSensitive("jdbc-passord"));
  }

  @Test
  void testFuzzyMatchesCommonPasswordTypoWithoutExplicitConfig() {
    SensitivePropertyKeyMatcher.configure(List.of(), 1);
    Assertions.assertTrue(SensitivePropertyKeyMatcher.isSensitive("jdbc-passwrod"));
    Assertions.assertTrue(SensitivePropertyKeyMatcher.isSensitive("oauth2.tokne"));
  }

  @Test
  void testFuzzyCanBeDisabled() {
    SensitivePropertyKeyMatcher.configure(List.of(), 0);
    Assertions.assertFalse(SensitivePropertyKeyMatcher.isSensitive("jdbc-passwrod"));
    Assertions.assertTrue(SensitivePropertyKeyMatcher.isSensitive("jdbc-password"));
  }

  @Test
  void testConfiguredTypoExpandsFuzzyReferences() {
    SensitivePropertyKeyMatcher.configure(List.of("passwd"), 1);
    Assertions.assertTrue(SensitivePropertyKeyMatcher.isSensitive("jdbc-passwd"));
    Assertions.assertTrue(SensitivePropertyKeyMatcher.isSensitive("jdbc-passwrd"));
  }

  @Test
  void testShortTokensAreNotFuzzyMatched() {
    SensitivePropertyKeyMatcher.configure(List.of(), 1);
    Assertions.assertFalse(SensitivePropertyKeyMatcher.isSensitive("db-key"));
  }
}
