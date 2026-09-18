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
  void testConfiguredTypoSubstringMatches() {
    SensitivePropertyKeyMatcher.configure(List.of("passwrod", "secert", "tokne"));
    Assertions.assertTrue(
        SensitivePropertyKeyMatcher.matchesTypoPattern("jdbc-passwrod".toLowerCase()));
    Assertions.assertTrue(
        SensitivePropertyKeyMatcher.matchesTypoPattern("catalog.secert".toLowerCase()));
    Assertions.assertTrue(
        SensitivePropertyKeyMatcher.matchesTypoPattern("oauth2.tokne".toLowerCase()));
    Assertions.assertFalse(
        SensitivePropertyKeyMatcher.matchesTypoPattern("jdbc-passord".toLowerCase()));
  }

  @Test
  void testTypoPatternsAreCaseInsensitive() {
    SensitivePropertyKeyMatcher.configure(List.of("PASSWROD"));
    Assertions.assertTrue(
        SensitivePropertyKeyMatcher.matchesTypoPattern("jdbc-passwrod".toLowerCase()));
  }

  @Test
  void testUnconfiguredTypoDoesNotMatch() {
    SensitivePropertyKeyMatcher.resetToDefaults();
    Assertions.assertFalse(
        SensitivePropertyKeyMatcher.matchesTypoPattern("jdbc-passwrod".toLowerCase()));
  }

  @Test
  void testRejectsBuiltinKeywordTypoPattern() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> SensitivePropertyKeyMatcher.configure(List.of("password")));
    Assertions.assertTrue(
        exception.getMessage().contains(SensitivePropertyKeyKeywords.invalidTypoPatternMessage()));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> SensitivePropertyKeyMatcher.configure(List.of("TOKEN")));
  }

  @Test
  void testTypoMatcherSupplementsBuiltinPattern() {
    SensitivePropertyKeyMatcher.configure(List.of("passwrod"));
    Assertions.assertTrue(SecretPropertyUtils.isSensitivePropertyKey("jdbc-passwrod"));
    Assertions.assertTrue(SecretPropertyUtils.isSensitivePropertyKey("jdbc-password"));
    SensitivePropertyKeyMatcher.resetToDefaults();
    Assertions.assertFalse(SecretPropertyUtils.isSensitivePropertyKey("jdbc-passwrod"));
    Assertions.assertTrue(SecretPropertyUtils.isSensitivePropertyKey("jdbc-password"));
  }
}
