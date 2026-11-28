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

import java.util.Arrays;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConverters;

/**
 * Places vended object-store credentials into a catalog's options without overriding what the user
 * configured.
 *
 * <p>A vended key is written only when the setting it stands for is not configured, and a vended
 * key pair only when neither half is. A setting is configured by a non-blank value in the catalog's
 * own options (including keys arriving through {@code spark.bypass.*} catalog properties) or in the
 * Hadoop configuration Spark builds for the session, which {@link #sessionHadoopConf()} describes.
 * That configuration is read when the catalog is initialized, the first time a session uses the
 * catalog. A deployment that sets its own keys therefore keeps working when the catalog also vends
 * credentials.
 *
 * <p>How a written key ranks against the other ways a filesystem can authenticate is left to the
 * filesystem: the key behaves as if the user had set it. A key the filesystem reads from elsewhere,
 * such as a Hadoop credential provider, is not seen as configured here, so the vended key is still
 * written and the filesystem picks between the two.
 *
 * <p>The option maps handed to this class must be modifiable.
 */
public final class VendedCredentialOptions {

  private static final Logger LOG = LoggerFactory.getLogger(VendedCredentialOptions.class);

  private VendedCredentialOptions() {}

  /**
   * Builds a copy of the Hadoop configuration a catalog starts from, before it applies its options:
   * the SparkContext's Hadoop configuration, which holds the {@code spark.hadoop.*} settings given
   * at startup and what Hadoop loaded from its own configuration files, with every setting of the
   * session written under its own key. Spark builds a session's configuration this way ({@code
   * SessionState.newHadoopConf()}), and the catalog takes it from there before it applies its
   * options, so a key set on the session with {@code spark.conf().set} is seen here where the
   * catalog's data access looks for it.
   *
   * <p>The session is the one {@code SparkSession.active()} resolves: the thread's active session,
   * or else the default session. A stopped session is not used: on Spark 3.5 it can stay registered
   * as the thread's active session, and reading the configuration of one whose session state was
   * never built fails.
   *
   * @return a copy of the session's Hadoop configuration, or null when there is no live Spark
   *     session
   */
  // JavaConverters is deprecated in Scala 2.13; the connector is compiled against 2.12 and 2.13.
  @Nullable
  @SuppressWarnings("deprecation")
  public static Configuration sessionHadoopConf() {
    SparkSession session = liveSession();
    if (session == null) {
      return null;
    }
    Configuration hadoopConf = new Configuration(session.sparkContext().hadoopConfiguration());
    JavaConverters.mapAsJavaMapConverter(session.conf().getAll())
        .asJava()
        .forEach(
            (key, value) -> {
              if (value != null) {
                hadoopConf.set(key, value);
              }
            });
    return hadoopConf;
  }

  /**
   * Returns the value a key is configured with in the catalog options. Catalog options reach the
   * catalog in lower case, whichever case they were written in, so an entry under another case of
   * the name is the same key once there; the entry written under the exact name is read first, so
   * that the result does not depend on the order of the map when both spellings are present.
   *
   * @param options the catalog options
   * @param key the option key
   * @return the non-blank value configured for the key, or null when there is none
   */
  @Nullable
  public static String optionValue(Map<String, String> options, String key) {
    String exact = options.get(key);
    if (isSet(exact)) {
      return exact;
    }
    for (Map.Entry<String, String> entry : options.entrySet()) {
      if (entry.getKey().equalsIgnoreCase(key) && isSet(entry.getValue())) {
        return entry.getValue();
      }
    }
    return null;
  }

  /**
   * Returns whether any of the keys is configured at session level, that is in the Hadoop
   * configuration Spark builds for the session. It is looked up exactly, the way the filesystems
   * look their keys up.
   *
   * @param sessionHadoopConf the session's Hadoop configuration; may be null when there is none
   * @param keys the keys
   * @return whether at least one of the keys holds a non-blank value there
   */
  public static boolean isConfiguredInSession(
      @Nullable Configuration sessionHadoopConf, String... keys) {
    if (sessionHadoopConf == null) {
      return false;
    }
    for (String key : keys) {
      // Only presence matters, so the value is read as written, without expanding variables.
      if (isSet(sessionHadoopConf.getRaw(key))) {
        return true;
      }
    }
    return false;
  }

  /**
   * Places a vended key pair into the options unless either half is configured. The pair is treated
   * as a unit, because one vended half combined with one configured half would not authenticate.
   * Blank entries of the two keys are removed, whether or not the pair is written: a blank value
   * does not configure a key, and the catalog receiving these options applies them, in lower case,
   * after the session's configuration, so a blank entry written in another case would overwrite the
   * vended key, and any blank entry would overwrite a key configured at session level.
   *
   * @param options the catalog options to update; must be modifiable
   * @param sessionHadoopConf the session's Hadoop configuration; may be null when there is none
   * @param accessKey the option key of the access key
   * @param accessValue the vended access key
   * @param secretKey the option key of the secret key
   * @param secretValue the vended secret key
   */
  public static void putPairUnlessConfigured(
      Map<String, String> options,
      @Nullable Configuration sessionHadoopConf,
      String accessKey,
      String accessValue,
      String secretKey,
      String secretValue) {
    removeBlankEntries(options, accessKey, secretKey);
    String configured = firstConfigured(options, sessionHadoopConf, accessKey, secretKey);
    if (configured != null) {
      LOG.info(
          "The vended credentials for keys '{}' and '{}' are not used: key '{}' is configured",
          accessKey,
          secretKey,
          configured);
      return;
    }
    options.put(accessKey, accessValue);
    options.put(secretKey, secretValue);
    LOG.info("The vended credentials are written for keys '{}' and '{}'", accessKey, secretKey);
  }

  /**
   * Places a vended key into the options unless the setting it stands for is configured: under the
   * key itself, or under one of the other names the filesystem reads the same setting from. Blank
   * entries of all these names are removed, whether or not the key is written, for the reason
   * {@link #putPairUnlessConfigured} gives.
   *
   * @param options the catalog options to update; must be modifiable
   * @param sessionHadoopConf the session's Hadoop configuration; may be null when there is none
   * @param key the option key
   * @param value the vended value
   * @param otherKeys the other names the filesystem reads the same setting from
   */
  public static void putKeyUnlessConfigured(
      Map<String, String> options,
      @Nullable Configuration sessionHadoopConf,
      String key,
      String value,
      String... otherKeys) {
    String[] keys = Arrays.copyOf(otherKeys, otherKeys.length + 1);
    keys[otherKeys.length] = key;
    removeBlankEntries(options, keys);
    String configured = firstConfigured(options, sessionHadoopConf, keys);
    if (configured != null) {
      LOG.info(
          "The vended credential for key '{}' is not used: key '{}' is configured",
          key,
          configured);
      return;
    }
    options.put(key, value);
    LOG.info("The vended credential is written for key '{}'", key);
  }

  @Nullable
  private static SparkSession liveSession() {
    Option<SparkSession> session = SparkSession.getActiveSession();
    if (session.isEmpty()) {
      session = SparkSession.getDefaultSession();
    }
    if (session.isEmpty() || session.get().sparkContext().isStopped()) {
      return null;
    }
    return session.get();
  }

  /** Returns the first of the keys that is configured, or null when none is. */
  @Nullable
  private static String firstConfigured(
      Map<String, String> options, @Nullable Configuration sessionHadoopConf, String... keys) {
    for (String key : keys) {
      if (optionValue(options, key) != null || isConfiguredInSession(sessionHadoopConf, key)) {
        return key;
      }
    }
    return null;
  }

  private static void removeBlankEntries(Map<String, String> options, String... keys) {
    options
        .entrySet()
        .removeIf(entry -> matchesAny(entry.getKey(), keys) && !isSet(entry.getValue()));
  }

  private static boolean matchesAny(String key, String... candidates) {
    for (String candidate : candidates) {
      if (key.equalsIgnoreCase(candidate)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isSet(@Nullable String value) {
    return value != null && !value.trim().isEmpty();
  }
}
