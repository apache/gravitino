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

package org.apache.gravitino.spark.connector.plugin;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import org.apache.gravitino.auth.AuthProperties;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;

/** Unit tests for Spark connector Basic authentication client configuration. */
public class TestGravitinoSparkBasicAuth {

  private static final String GRAVITINO_URI = "http://127.0.0.1:8090";
  private static final String METALAKE = "spark_basic_auth";
  private static final String SPARK_USER = "spark-user";

  @Test
  void testCreateClientWithBasicAuth() {
    SparkConf sparkConf = basicAuthSparkConf("alice", "secret");
    GravitinoAdminClient client =
        GravitinoAdminClient.builder(GRAVITINO_URI)
            .withBasicAuth(
                sparkConf.get(GravitinoSparkConfig.GRAVITINO_BASIC_USERNAME),
                sparkConf.get(GravitinoSparkConfig.GRAVITINO_BASIC_PASSWORD))
            .build();
    assertNotNull(client);
  }

  @Test
  void testCreateClientWithBasicAuthMissingUsername() {
    SparkConf sparkConf =
        new SparkConf()
            .set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, AuthProperties.BASIC_AUTH_TYPE)
            .set(GravitinoSparkConfig.GRAVITINO_BASIC_PASSWORD, "secret");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GravitinoDriverPlugin.createGravitinoClient(
                GRAVITINO_URI, METALAKE, sparkConf, SPARK_USER, Collections.emptyMap()));
  }

  @Test
  void testCreateClientWithBasicAuthMissingPassword() {
    SparkConf sparkConf =
        new SparkConf()
            .set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, AuthProperties.BASIC_AUTH_TYPE)
            .set(GravitinoSparkConfig.GRAVITINO_BASIC_USERNAME, "alice");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GravitinoDriverPlugin.createGravitinoClient(
                GRAVITINO_URI, METALAKE, sparkConf, SPARK_USER, Collections.emptyMap()));
  }

  @Test
  void testCreateClientWithBasicAuthBlankCredentials() {
    SparkConf blankUsernameConf = basicAuthSparkConf(" ", "secret");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GravitinoDriverPlugin.createGravitinoClient(
                GRAVITINO_URI, METALAKE, blankUsernameConf, SPARK_USER, Collections.emptyMap()));

    SparkConf blankPasswordConf = basicAuthSparkConf("alice", " ");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            GravitinoDriverPlugin.createGravitinoClient(
                GRAVITINO_URI, METALAKE, blankPasswordConf, SPARK_USER, Collections.emptyMap()));
  }

  private static SparkConf basicAuthSparkConf(String username, String password) {
    return new SparkConf()
        .set(GravitinoSparkConfig.GRAVITINO_AUTH_TYPE, AuthProperties.BASIC_AUTH_TYPE)
        .set(GravitinoSparkConfig.GRAVITINO_BASIC_USERNAME, username)
        .set(GravitinoSparkConfig.GRAVITINO_BASIC_PASSWORD, password);
  }
}
