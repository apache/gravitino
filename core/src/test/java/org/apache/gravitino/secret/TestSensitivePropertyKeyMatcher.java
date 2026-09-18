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
  void testDefaultKeywordsMatchBuiltinNames() {
    Assertions.assertTrue(SecretPropertyUtils.isSensitivePropertyKey("jdbc-password"));
    Assertions.assertTrue(SecretPropertyUtils.isSensitivePropertyKey("aws-access-key-id"));
    Assertions.assertFalse(SecretPropertyUtils.isSensitivePropertyKey("jdbc-passwrod"));
  }

  @Test
  void testConfiguredKeywordsReplaceDefaults() {
    SensitivePropertyKeyMatcher.configure(List.of("passwrod", "secert", "private"));
    Assertions.assertTrue(SensitivePropertyKeyMatcher.matches("jdbc-passwrod"));
    Assertions.assertTrue(SensitivePropertyKeyMatcher.matches("catalog.secert"));
    Assertions.assertTrue(SecretPropertyUtils.isSensitivePropertyKey("jdbc-private-key"));
    Assertions.assertFalse(SensitivePropertyKeyMatcher.matches("jdbc-passord"));
    Assertions.assertFalse(SecretPropertyUtils.isSensitivePropertyKey("jdbc-password"));
    Assertions.assertFalse(SecretPropertyUtils.isSensitivePropertyKey("aws-access-key-id"));
  }

  @Test
  void testKeywordsAreCaseInsensitive() {
    SensitivePropertyKeyMatcher.configure(List.of("PASSWROD"));
    Assertions.assertTrue(SensitivePropertyKeyMatcher.matches("jdbc-passwrod"));
  }

  @Test
  void testEmptyKeywordListDisablesNameMatching() {
    SensitivePropertyKeyMatcher.configure(List.of());
    Assertions.assertFalse(SecretPropertyUtils.isSensitivePropertyKey("jdbc-password"));
    Assertions.assertFalse(SecretPropertyUtils.isSensitivePropertyKey("aws-access-key-id"));
    SensitivePropertyKeyMatcher.resetToDefaults();
    Assertions.assertTrue(SecretPropertyUtils.isSensitivePropertyKey("jdbc-password"));
  }

  @Test
  void testRejectsBlankKeyword() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> SensitivePropertyKeyMatcher.configure(List.of(" ")));
    Assertions.assertTrue(
        exception.getMessage().contains(SensitivePropertyKeyKeywords.invalidKeywordMessage()));
    Assertions.assertTrue(SecretPropertyUtils.isSensitivePropertyKey("jdbc-password"));
  }

  @Test
  void testConfigReplacesDefaultKeywords() {
    Config config = new Config(false) {};
    config.set(Configs.SENSITIVE_KEY_KEYWORDS, List.of("private"));
    SecretPropertyUtils.configureSensitiveKeyKeywords(config);
    Assertions.assertTrue(SecretPropertyUtils.isSensitivePropertyKey("jdbc-private-key"));
    Assertions.assertFalse(SecretPropertyUtils.isSensitivePropertyKey("jdbc-password"));
  }
}
