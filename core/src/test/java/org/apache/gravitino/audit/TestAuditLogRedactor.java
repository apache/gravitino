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

package org.apache.gravitino.audit;

import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAuditLogRedactor {

  @Test
  public void testNullCustomInfoReturnsEmptyMap() {
    Assertions.assertTrue(AuditLogRedactor.redactCustomInfo(null).isEmpty());
  }

  @Test
  public void testNonSensitiveKeyKeptAsIs() {
    Map<String, String> info = Collections.singletonMap("details", "true");
    Assertions.assertEquals("true", AuditLogRedactor.redactCustomInfo(info).get("details"));
  }

  // ─── exact-match (MASKED_CUSTOM_INFO_KEYS) ───────────────────────────────────

  @Test
  public void testExactMatchKeyIsRedacted() {
    Map<String, String> info = Collections.singletonMap("authorization", "Bearer secret-token");
    Assertions.assertEquals(
        AuditLogRedactor.REDACTED_VALUE,
        AuditLogRedactor.redactCustomInfo(info).get("authorization"));
  }

  @Test
  public void testExactMatchIsCaseInsensitive() {
    Map<String, String> info = Collections.singletonMap("AUTHORIZATION", "Bearer secret-token");
    Assertions.assertEquals(
        AuditLogRedactor.REDACTED_VALUE,
        AuditLogRedactor.redactCustomInfo(info).get("AUTHORIZATION"));
  }

  /**
   * "cookie" is a known internal key with no sensitive substring in its name (it doesn't contain
   * "password", "token", etc.), so only the exact-match list catches it.
   */
  @Test
  public void testExactMatchCatchesKeyWithNoSensitiveSubstring() {
    Map<String, String> info = Collections.singletonMap("cookie", "session=abc123");
    Assertions.assertEquals(
        AuditLogRedactor.REDACTED_VALUE, AuditLogRedactor.redactCustomInfo(info).get("cookie"));
  }

  // ─── substring match (SENSITIVE_KEY_SUBSTRINGS) ──────────────────────────────

  /**
   * Request query-parameter names become customInfo entries via {@link
   * org.apache.gravitino.listener.api.event.Event#customInfo()} with whatever naming convention the
   * caller chose, so redaction here must also catch names the exact-match list was never designed
   * for (e.g. "accessToken" isn't in MASKED_CUSTOM_INFO_KEYS, but it contains "token").
   */
  @Test
  public void testSubstringMatchCatchesCallerSuppliedKeyNames() {
    Map<String, String> info = Collections.singletonMap("accessToken", "abcd1234");
    Assertions.assertEquals(
        AuditLogRedactor.REDACTED_VALUE,
        AuditLogRedactor.redactCustomInfo(info).get("accessToken"));
  }

  @Test
  public void testSubstringMatchCatchesHyphenatedKeyName() {
    Map<String, String> info = Collections.singletonMap("s3-secret-access-key", "abcd1234");
    Assertions.assertEquals(
        AuditLogRedactor.REDACTED_VALUE,
        AuditLogRedactor.redactCustomInfo(info).get("s3-secret-access-key"));
  }

  @Test
  public void testSubstringMatchIsCaseInsensitive() {
    Map<String, String> info = Collections.singletonMap("PASSWORD", "hunter2");
    Assertions.assertEquals(
        AuditLogRedactor.REDACTED_VALUE, AuditLogRedactor.redactCustomInfo(info).get("PASSWORD"));
  }

  @Test
  public void testKeyIsPreservedEvenWhenValueIsRedacted() {
    Map<String, String> info = Collections.singletonMap("token", "secretvalue");
    Assertions.assertTrue(AuditLogRedactor.redactCustomInfo(info).containsKey("token"));
  }

  // ─── NEVER_SENSITIVE_KEYS (codebase-chosen keys that coincidentally contain a substring) ────

  /**
   * "auth.method"/"auth.expression" are fixed key literals AuthorizationDenialFailureEvent always
   * uses — not caller-supplied data — but they contain "auth", which is in
   * SENSITIVE_KEY_SUBSTRINGS. Without an explicit exemption, the substring check (added to also
   * cover caller-supplied query-parameter names) would redact genuinely useful audit data: which
   * method was denied and what expression was evaluated, exactly what this event exists to record.
   */
  @Test
  public void testCodebaseChosenAuthKeysAreNeverRedacted() {
    Map<String, String> info = Collections.singletonMap("auth.method", "loadTable");
    Assertions.assertEquals(
        "loadTable", AuditLogRedactor.redactCustomInfo(info).get("auth.method"));

    Map<String, String> info2 = Collections.singletonMap("auth.expression", "TABLE:LOAD");
    Assertions.assertEquals(
        "TABLE:LOAD", AuditLogRedactor.redactCustomInfo(info2).get("auth.expression"));
  }

  @Test
  public void testCodebaseChosenHttpKeysAreNeverRedacted() {
    Map<String, String> info = Collections.singletonMap("http.method", "GET");
    Assertions.assertEquals("GET", AuditLogRedactor.redactCustomInfo(info).get("http.method"));
  }
}
