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
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSensitivePropertyKeyMatcher {

  @AfterEach
  void resetMatcher() {
    SensitivePropertyKeyMatcher.resetToDefaults();
  }

  @Test
  void testConfiguredAdditionalSubstringMatchesTypo() {
    SensitivePropertyKeyMatcher.configure(List.of("passwrod", "secert", "tokne"));
    Assertions.assertTrue(
        SensitivePropertyKeyMatcher.matchesAdditionalKeyword("jdbc-passwrod".toLowerCase()));
    Assertions.assertTrue(
        SensitivePropertyKeyMatcher.matchesAdditionalKeyword("catalog.secert".toLowerCase()));
    Assertions.assertTrue(
        SensitivePropertyKeyMatcher.matchesAdditionalKeyword("oauth2.tokne".toLowerCase()));
    Assertions.assertFalse(
        SensitivePropertyKeyMatcher.matchesAdditionalKeyword("jdbc-passord".toLowerCase()));
  }

  @Test
  void testConfiguredAdditionalSubstringMatchesExtraKeyword() {
    SensitivePropertyKeyMatcher.configure(List.of("private"));
    Assertions.assertTrue(
        SensitivePropertyKeyMatcher.matchesAdditionalKeyword("jdbc-private-key".toLowerCase()));
    Assertions.assertFalse(
        SensitivePropertyKeyMatcher.matchesAdditionalKeyword("jdbc-user".toLowerCase()));
    Assertions.assertTrue(SecretPropertyUtils.isSensitivePropertyKey("jdbc-private-key"));
  }

  @Test
  void testAdditionalPatternsAreCaseInsensitive() {
    SensitivePropertyKeyMatcher.configure(List.of("PASSWROD"));
    Assertions.assertTrue(
        SensitivePropertyKeyMatcher.matchesAdditionalKeyword("jdbc-passwrod".toLowerCase()));
  }

  @Test
  void testUnconfiguredAdditionalPatternDoesNotMatch() {
    SensitivePropertyKeyMatcher.resetToDefaults();
    Assertions.assertFalse(
        SensitivePropertyKeyMatcher.matchesAdditionalKeyword("jdbc-passwrod".toLowerCase()));
  }

  @Test
  void testRejectsBuiltinKeywordAdditionalPattern() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> SensitivePropertyKeyMatcher.configure(List.of("password")));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains(SensitivePropertyKeyKeywords.invalidAdditionalKeywordMessage()));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> SensitivePropertyKeyMatcher.configure(List.of("TOKEN")));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> SensitivePropertyKeyMatcher.configure(List.of("my-password")));
    Config config = new Config(false) {};
    config.set(Configs.SENSITIVE_KEY_ADDITIONAL_KEYWORDS, List.of("secrets"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> SecretPropertyUtils.configureSensitiveKeyAdditionalKeywords(config));
  }

  @Test
  void testAdditionalMatcherSupplementsBuiltinPattern() {
    SensitivePropertyKeyMatcher.configure(List.of("passwrod"));
    Assertions.assertTrue(SecretPropertyUtils.isSensitivePropertyKey("jdbc-passwrod"));
    Assertions.assertTrue(SecretPropertyUtils.isSensitivePropertyKey("jdbc-password"));
    SensitivePropertyKeyMatcher.resetToDefaults();
    Assertions.assertFalse(SecretPropertyUtils.isSensitivePropertyKey("jdbc-passwrod"));
    Assertions.assertTrue(SecretPropertyUtils.isSensitivePropertyKey("jdbc-password"));
  }
}
