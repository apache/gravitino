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

package org.apache.gravitino.spark.connector.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests the rule the vended credentials follow: a vended key is written unless that key is
 * configured, in the catalog options or in the session's Hadoop configuration.
 */
@Timeout(60)
public class TestVendedCredentialOptions {

  private static final String APP_NAME = "test-vended-credential-options";

  private static final String VENDED_PAIR =
      "the vended pair must be written exactly, with no blank entry left beside it";

  @Test
  void testPairIsWrittenWhenNeitherHalfIsConfigured() {
    Map<String, String> options = new HashMap<>();

    VendedCredentialOptions.putPairUnlessConfigured(options, null, "access", "a", "secret", "s");

    assertEquals(ImmutableMap.of("access", "a", "secret", "s"), options, "the pair is written");
  }

  @Test
  void testPairIsSkippedWhenEitherHalfIsConfiguredInTheOptions() {
    for (Map<String, String> configured :
        ImmutableList.of(
            ImmutableMap.of("access", "user-a"),
            ImmutableMap.of("secret", "user-s"),
            ImmutableMap.of("Access", "user-a"),
            ImmutableMap.of("ACCESS", "user-a", "SECRET", "user-s"))) {
      Map<String, String> options = new HashMap<>(configured);

      VendedCredentialOptions.putPairUnlessConfigured(options, null, "access", "a", "secret", "s");

      assertEquals(configured, options, "a half-configured pair must not be completed");
    }
  }

  @Test
  void testPairIsSkippedWhenEitherHalfIsConfiguredInTheSession() {
    for (String key : new String[] {"access", "secret"}) {
      Configuration session = new Configuration(false);
      session.set(key, "user");
      Map<String, String> options = new HashMap<>();

      VendedCredentialOptions.putPairUnlessConfigured(
          options, session, "access", "a", "secret", "s");

      assertTrue(options.isEmpty(), "a key configured at session level keeps winning: " + key);
    }
  }

  @Test
  void testDifferentlyCasedSessionKeyDoesNotCount() {
    // The Hadoop configuration is read exactly, as the filesystems read it: this key is not
    // theirs, whichever way it is spelled.
    Configuration session = new Configuration(false);
    session.set("ACCESS", "user");
    Map<String, String> options = new HashMap<>();

    VendedCredentialOptions.putPairUnlessConfigured(options, session, "access", "a", "secret", "s");

    assertEquals(
        ImmutableMap.of("access", "a", "secret", "s"),
        options,
        "a session key under another case is not read by the filesystem either");
  }

  @Test
  void testBlankValuesDoNotCountAsConfigured() {
    Map<String, String> options = new HashMap<>(ImmutableMap.of("access", " "));
    Configuration session = new Configuration(false);
    session.set("secret", "");

    VendedCredentialOptions.putPairUnlessConfigured(options, session, "access", "a", "secret", "s");

    assertEquals(ImmutableMap.of("access", "a", "secret", "s"), options, VENDED_PAIR);
  }

  @Test
  void testDifferentlyCasedBlankEntriesAreReplaced() {
    Map<String, String> options = new HashMap<>(ImmutableMap.of("ACCESS", "", "Secret", "  "));

    VendedCredentialOptions.putPairUnlessConfigured(options, null, "access", "a", "secret", "s");

    assertEquals(ImmutableMap.of("access", "a", "secret", "s"), options, VENDED_PAIR);
  }

  @Test
  void testBlankEntryIsDroppedWhenTheKeyIsConfiguredInTheSession() {
    // The catalog applies its options after the session's configuration, so the blank entry left
    // in place would hide the session's value.
    Configuration session = new Configuration(false);
    session.set("access", "user");
    Map<String, String> options = new HashMap<>(ImmutableMap.of("access", " "));

    VendedCredentialOptions.putPairUnlessConfigured(options, session, "access", "a", "secret", "s");

    assertTrue(options.isEmpty(), "nothing is written and the blank entry must not stay behind");
  }

  @Test
  void testSingleKeyIsWrittenOnlyWhenNotConfigured() {
    Map<String, String> absent = new HashMap<>();
    VendedCredentialOptions.putKeyUnlessConfigured(absent, null, "key", "vended");
    assertEquals(ImmutableMap.of("key", "vended"), absent, "nothing configured");

    Map<String, String> configuredInOptions = new HashMap<>(ImmutableMap.of("KEY", "user"));
    VendedCredentialOptions.putKeyUnlessConfigured(configuredInOptions, null, "key", "vended");
    assertEquals(ImmutableMap.of("KEY", "user"), configuredInOptions, "configured in the options");

    Map<String, String> blankInOptions = new HashMap<>(ImmutableMap.of("KEY", ""));
    VendedCredentialOptions.putKeyUnlessConfigured(blankInOptions, null, "key", "vended");
    assertEquals(ImmutableMap.of("key", "vended"), blankInOptions, "a blank value is replaced");

    Configuration session = new Configuration(false);
    session.set("key", "user");
    Map<String, String> configuredInSession = new HashMap<>();
    VendedCredentialOptions.putKeyUnlessConfigured(configuredInSession, session, "key", "vended");
    assertTrue(configuredInSession.isEmpty(), "a key configured at session level keeps winning");
  }

  @Test
  void testSingleKeyIsSkippedWhenTheSameSettingIsConfiguredUnderAnotherName() {
    Map<String, String> configuredInOptions = new HashMap<>(ImmutableMap.of("OTHER", "user"));
    VendedCredentialOptions.putKeyUnlessConfigured(
        configuredInOptions, null, "key", "vended", "other");
    assertEquals(
        ImmutableMap.of("OTHER", "user"), configuredInOptions, "configured in the options");

    Configuration session = new Configuration(false);
    session.set("other", "user");
    Map<String, String> configuredInSession = new HashMap<>();
    VendedCredentialOptions.putKeyUnlessConfigured(
        configuredInSession, session, "key", "vended", "other");
    assertTrue(
        configuredInSession.isEmpty(), "a key configured at session level under another name wins");
  }

  @Test
  void testBlankEntryOfAnotherNameIsDroppedWhenThatNameIsConfiguredInTheSession() {
    Configuration session = new Configuration(false);
    session.set("other", "user");
    Map<String, String> options = new HashMap<>(ImmutableMap.of("other", " "));

    VendedCredentialOptions.putKeyUnlessConfigured(options, session, "key", "vended", "other");

    assertTrue(
        options.isEmpty(),
        "nothing is written and the blank entry must not stay behind to hide the session's value");
  }

  @Test
  void testOptionValueIsReadCaseInsensitivelyAndSkipsBlanks() {
    Map<String, String> options =
        new HashMap<>(ImmutableMap.of("KEY", "user", "BLANK", " ", "SPACES", "  "));

    assertEquals("user", VendedCredentialOptions.optionValue(options, "key"), "any case matches");
    assertNull(
        VendedCredentialOptions.optionValue(options, "blank"), "a blank value is not a value");
    assertNull(
        VendedCredentialOptions.optionValue(options, "absent"), "an absent key has no value");

    // A Spark option arrives lower-cased and a bypass property keeps its case; when both spell
    // the same key, the one spelled like the key decides, whichever way the map iterates.
    Map<String, String> bothSpellings =
        new HashMap<>(
            ImmutableMap.of("fs.oss.accessKeyId", "bypass", "fs.oss.accesskeyid", "option"));
    assertEquals(
        "bypass",
        VendedCredentialOptions.optionValue(bothSpellings, "fs.oss.accessKeyId"),
        "the entry spelled like the key wins");
    Map<String, String> blankExact =
        new HashMap<>(ImmutableMap.of("fs.oss.accessKeyId", " ", "fs.oss.accesskeyid", "option"));
    assertEquals(
        "option",
        VendedCredentialOptions.optionValue(blankExact, "fs.oss.accessKeyId"),
        "a blank entry spelled like the key gives way to another spelling");
  }

  @Test
  void testIsConfiguredInSessionReadsTheSessionConfigurationExactly() {
    Configuration session = new Configuration(false);
    session.set("key", "session-value");
    session.set("blank", " ");

    assertTrue(
        VendedCredentialOptions.isConfiguredInSession(session, "key", "other"),
        "one configured key of the list is enough");
    assertTrue(
        !VendedCredentialOptions.isConfiguredInSession(session, "KEY"),
        "the Hadoop configuration is exact-case, as the filesystems read it");
    assertTrue(
        !VendedCredentialOptions.isConfiguredInSession(session, "blank", "other"),
        "a blank value does not count");
    assertTrue(
        !VendedCredentialOptions.isConfiguredInSession(null, "key"),
        "without a session nothing is configured there");
  }

  @Test
  void testSessionHadoopConfIsNullWithoutALiveSession() {
    assertNull(
        VendedCredentialOptions.sessionHadoopConf(),
        "with no live SparkSession registered there is no session Hadoop configuration to build");
  }

  @Test
  void testStoppedSessionIsNotUsed() {
    try (LocalSparkSessions.Session local =
        LocalSparkSessions.create(APP_NAME, ImmutableMap.of())) {
      SparkSession spark = local.spark();
      spark.stop();

      // On Spark 3.5 the stopped session stays registered as the thread's active session.
      assertNull(
          VendedCredentialOptions.sessionHadoopConf(), "a stopped active session must not be read");

      SparkSession.clearActiveSession();
      assertNull(
          VendedCredentialOptions.sessionHadoopConf(),
          "a stopped default session must not be read");
    }
  }

  @Test
  void testActiveSessionIsPreferredOverTheDefaultOne() {
    try (LocalSparkSessions.Session local =
        LocalSparkSessions.create(APP_NAME, ImmutableMap.of())) {
      SparkSession second = local.spark().newSession();
      second.conf().set("fs.s3a.access.key", "set-on-the-second-session");
      SparkSession.setActiveSession(second);

      Configuration conf = VendedCredentialOptions.sessionHadoopConf();

      assertEquals(
          "set-on-the-second-session",
          conf.get("fs.s3a.access.key"),
          "the thread's active session must be read, not the default one");
    }
  }

  /**
   * The session looked up is the one {@code SparkSession.active()} resolves: with no active session
   * on the thread, the default one.
   */
  @Test
  void testDefaultSessionIsUsedWhenTheThreadHasNoActiveSession() {
    try (LocalSparkSessions.Session local =
        LocalSparkSessions.create(APP_NAME, ImmutableMap.of())) {
      SparkSession spark = local.spark();
      spark.conf().set("fs.s3a.access.key", "set-on-the-default-session");

      SparkSession.clearActiveSession();
      try {
        assertEquals(
            "set-on-the-default-session",
            VendedCredentialOptions.sessionHadoopConf().get("fs.s3a.access.key"),
            "the default session must be consulted when the thread has no active session");
      } finally {
        SparkSession.setActiveSession(spark);
      }
    }
  }

  @Test
  void testSessionHadoopConfHoldsStartupAndSessionSettings() {
    Map<String, String> startup = ImmutableMap.of("spark.hadoop.fs.s3a.access.key", "startup-ak");
    try (LocalSparkSessions.Session local = LocalSparkSessions.create(APP_NAME, startup)) {
      SparkSession spark = local.spark();
      spark.conf().set("fs.oss.accessKeyId", "runtime-oss-id");
      // Copied as written, prefix included: this is not a way to set a Hadoop key on a session.
      spark.conf().set("spark.hadoop.fs.oss.accessKeySecret", "inert");

      Configuration conf = VendedCredentialOptions.sessionHadoopConf();

      assertEquals("startup-ak", conf.get("fs.s3a.access.key"), "spark.hadoop.* given at startup");
      assertEquals("runtime-oss-id", conf.get("fs.oss.accessKeyId"), "a setting of the session");
      assertNull(conf.get("fs.oss.accessKeySecret"), "spark.hadoop.* set on the session");

      // A copy: looking must not write the session's settings into the SparkContext's
      // configuration.
      assertNotSame(
          spark.sparkContext().hadoopConfiguration(), conf, "a copy is built, not the original");
      assertNull(
          spark.sparkContext().hadoopConfiguration().get("fs.oss.accessKeyId"),
          "the SparkContext's configuration stays as it was");
    }
  }
}
