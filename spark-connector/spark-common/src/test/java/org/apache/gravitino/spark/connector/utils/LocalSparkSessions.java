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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.spark.sql.SparkSession;
import scala.Option;

/**
 * Spark sessions for tests that need a real one, created and removed one at a time so that tests
 * cannot pick up each other's sessions.
 */
public final class LocalSparkSessions {

  private LocalSparkSessions() {}

  /**
   * Creates a local session for a single test, to be used in try-with-resources. Fails when another
   * test left a live session registered: {@code getOrCreate()} would hand that one back instead of
   * creating one with these settings, and closing the result would then stop a session that is not
   * this test's.
   *
   * @param appName the application name of the session
   * @param configs the settings of the session
   * @return the session, stopped and unregistered when closed
   */
  public static Session create(String appName, Map<String, String> configs) {
    assertTrue(
        noLiveSession(SparkSession.getActiveSession())
            && noLiveSession(SparkSession.getDefaultSession()),
        "another test left a live SparkSession registered");
    SparkSession.Builder builder =
        SparkSession.builder()
            .master("local[1]")
            .appName(appName)
            .config("spark.ui.enabled", "false")
            // Keep the driver reachable whatever address the machine's hostname resolves to.
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1");
    configs.forEach(builder::config);
    return new Session(builder.getOrCreate());
  }

  private static boolean noLiveSession(Option<SparkSession> session) {
    return session.isEmpty() || session.get().sparkContext().isStopped();
  }

  /** A session of one test, created by {@link LocalSparkSessions#create(String, Map)}. */
  public static final class Session implements AutoCloseable {

    private final SparkSession spark;

    private Session(SparkSession spark) {
      this.spark = spark;
    }

    /**
     * Returns the Spark session.
     *
     * @return the session
     */
    public SparkSession spark() {
      return spark;
    }

    /**
     * Stops the session and unregisters it. {@link SparkSession#close()} only stops the
     * SparkContext: on Spark 3.5 the stopped session stays registered as the thread's active
     * session afterwards, and {@code SparkSession.active()} would then hand it to whoever asks
     * next.
     */
    @Override
    public void close() {
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
      spark.stop();
    }
  }
}
