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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests the session helper the credential tests rely on: it refuses to run beside a live session
 * another test left, and a session it closed leaves nothing registered.
 */
@Timeout(60)
public class TestLocalSparkSessions {

  @Test
  void testAClosedSessionIsStoppedAndNotLeftRegistered() {
    LocalSparkSessions.Session local =
        LocalSparkSessions.create("test-local-spark-sessions", ImmutableMap.of());
    local.close();

    assertTrue(local.spark().sparkContext().isStopped(), "closing stops the session");
    // On Spark 3.5 stop() alone would leave the stopped session registered.
    assertTrue(
        SparkSession.getActiveSession().isEmpty(), "the closed session must not stay active");
    assertTrue(
        SparkSession.getDefaultSession().isEmpty(), "the closed session must not stay default");
  }

  @Test
  void testASessionAnotherTestLeftIsNotReused() {
    SparkSession foreign =
        SparkSession.builder()
            .master("local[1]")
            .appName("test-local-spark-sessions-foreign")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate();
    try {
      // Reusing it would ignore the helper's settings, and closing the result would stop it.
      assertThrows(
          AssertionError.class,
          () -> LocalSparkSessions.create("test-local-spark-sessions", ImmutableMap.of()),
          "a live session another test left must be reported, not reused");
    } finally {
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
      foreign.stop();
    }
  }

  @Test
  void testAStoppedSessionAnotherTestLeftIsNotHandedOut() {
    SparkSession stopped =
        SparkSession.builder()
            .master("local[1]")
            .appName("test-local-spark-sessions-stopped")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate();
    // On Spark 3.5 the stopped session stays registered after this.
    stopped.stop();

    try (LocalSparkSessions.Session local =
        LocalSparkSessions.create("test-local-spark-sessions", ImmutableMap.of())) {
      assertNotNull(local.spark().sparkContext(), "a fresh session is created regardless");
      assertTrue(
          local.spark() != stopped, "the stopped session another test left is not handed out");
      assertTrue(
          SparkSession.getActiveSession().nonEmpty()
              && SparkSession.getActiveSession().get() == local.spark(),
          "the fresh session is the one registered as active");
    }
  }
}
