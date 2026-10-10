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
package org.apache.gravitino.job.k8s;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/**
 * The configurations of the {@code k8s} job executor, set with the {@code
 * gravitino.jobExecutor.k8s.} prefix in the Gravitino server configuration.
 */
public class K8sJobExecutorConfigs {

  /** The name of the job executor. */
  public static final String K8S_JOB_EXECUTOR_NAME = "k8s";

  /** The name of the Kubernetes cluster the jobs run in, which is part of the job execution id. */
  public static final String CLUSTER = "cluster";

  /** The default cluster name. */
  public static final String DEFAULT_CLUSTER = "default";

  /** The kubeconfig file of the job cluster. */
  public static final String KUBECONFIG = "kubeconfig";

  /** The kubeconfig context to use, the current context if not set. */
  public static final String CONTEXT = "context";

  /** The API server URL of the job cluster, an alternative to the kubeconfig. */
  public static final String MASTER_URL = "masterUrl";

  /** The CA certificate file of the API server, used with {@link #MASTER_URL}. */
  public static final String CA_CERT_FILE = "caCertFile";

  /**
   * The bearer token file to access the API server, used with {@link #MASTER_URL}. It is read again
   * whenever it changes, so the token can be rotated.
   */
  public static final String TOKEN_FILE = "tokenFile";

  /** The default namespace of the jobs. */
  public static final String NAMESPACE = "namespace";

  /** The default namespace. */
  public static final String DEFAULT_NAMESPACE = "default";

  /** The prefix of the names of the Kubernetes resources created for the jobs. */
  public static final String NAME_PREFIX = "namePrefix";

  /** The default name prefix. */
  public static final String DEFAULT_NAME_PREFIX = "gravitino-job";

  /** How long the resources listed in a namespace are cached for the job status queries. */
  public static final String STATUS_CACHE_TTL_MS = "statusCacheTtlMs";

  /** The default status cache TTL, 5 seconds. */
  public static final long DEFAULT_STATUS_CACHE_TTL_MS = 5_000L;

  /** A job whose resource still has no status after this time is considered failed. */
  public static final String NO_STATUS_TIMEOUT_MS = "noStatusTimeoutMs";

  /** The default no status timeout, 10 minutes. */
  public static final long DEFAULT_NO_STATUS_TIMEOUT_MS = 10 * 60 * 1000L;

  /** The Spark image of the driver and executors, the default of a job's container image. */
  public static final String SPARK_IMAGE = "spark.image";

  /** The Spark version of the image, set as the SparkApplication runtime version. */
  public static final String SPARK_VERSION = "spark.sparkVersion";

  /** The service account of the Spark driver, the default of a job's driver service account. */
  public static final String SPARK_SERVICE_ACCOUNT = "spark.serviceAccount";

  /** The default Spark driver service account. */
  public static final String DEFAULT_SPARK_SERVICE_ACCOUNT = "spark";

  /** How long the pods of a finished Spark job are retained, so that its output can be read. */
  public static final String SPARK_RESOURCE_RETAIN_DURATION_MS = "spark.resourceRetainDurationMs";

  /** The default pod retain duration, 1 day. */
  public static final long DEFAULT_SPARK_RESOURCE_RETAIN_DURATION_MS = 24 * 60 * 60 * 1000L;

  /** The time to live of a SparkApplication after it stops. */
  public static final String SPARK_TTL_AFTER_STOP_MS = "spark.ttlAfterStopMs";

  /** The default SparkApplication time to live after it stops, 7 days. */
  public static final long DEFAULT_SPARK_TTL_AFTER_STOP_MS = 7 * 24 * 60 * 60 * 1000L;

  /** How long the operator waits for the driver pod to start. */
  public static final String SPARK_DRIVER_START_TIMEOUT_MS = "spark.driverStartTimeoutMs";

  /** How long the operator waits for the driver to be ready. */
  public static final String SPARK_DRIVER_READY_TIMEOUT_MS = "spark.driverReadyTimeoutMs";

  /**
   * The default of the operator's start timeouts, 1 hour, so that a job waits for resources instead
   * of failing after the operator's default of 5 minutes.
   */
  public static final long DEFAULT_SPARK_START_TIMEOUT_MS = 60 * 60 * 1000L;

  /**
   * The prefix of the default Spark configurations of every Spark job. The job's own Spark
   * configurations take precedence.
   */
  public static final String SPARK_CONF_PREFIX = "spark.conf.";

  // The resource name is the prefix, a dash and a job id of up to 19 digits. Keep it within 44
  // characters, so that the operator doesn't hash-truncate the Spark app id derived from it.
  private static final int MAX_NAME_PREFIX_LENGTH = 24;

  // A DNS-1123 label, which the resource name must be.
  private static final Pattern NAME_PREFIX_PATTERN = Pattern.compile("[a-z]([-a-z0-9]*[a-z0-9])?");

  private final String cluster;
  @Nullable private final String kubeconfig;
  @Nullable private final String context;
  @Nullable private final String masterUrl;
  @Nullable private final String caCertFile;
  @Nullable private final String tokenFile;
  private final String namespace;
  private final String namePrefix;
  private final long statusCacheTtlMs;
  private final long noStatusTimeoutMs;
  private final String sparkImage;
  private final String sparkVersion;
  private final String sparkServiceAccount;
  private final long sparkResourceRetainDurationMs;
  private final long sparkTtlAfterStopMs;
  private final long sparkDriverStartTimeoutMs;
  private final long sparkDriverReadyTimeoutMs;
  private final Map<String, String> sparkConf;

  /**
   * Parses and validates the job executor configurations.
   *
   * @param configs the configurations without the {@code gravitino.jobExecutor.k8s.} prefix
   * @throws IllegalArgumentException if a configuration is invalid
   */
  public K8sJobExecutorConfigs(Map<String, String> configs) {
    this.cluster = stringValue(configs, CLUSTER, DEFAULT_CLUSTER);
    Preconditions.checkArgument(
        !cluster.contains("/"), "%s must not contain '/': %s", CLUSTER, cluster);

    this.kubeconfig = stringValue(configs, KUBECONFIG, null);
    this.context = stringValue(configs, CONTEXT, null);
    this.masterUrl = stringValue(configs, MASTER_URL, null);
    this.caCertFile = stringValue(configs, CA_CERT_FILE, null);
    this.tokenFile = stringValue(configs, TOKEN_FILE, null);
    Preconditions.checkArgument(
        masterUrl == null || (kubeconfig == null && context == null),
        "%s can't be set together with %s or %s",
        MASTER_URL,
        KUBECONFIG,
        CONTEXT);
    Preconditions.checkArgument(
        masterUrl != null || (caCertFile == null && tokenFile == null),
        "%s and %s can only be set together with %s",
        CA_CERT_FILE,
        TOKEN_FILE,
        MASTER_URL);

    this.namespace = stringValue(configs, NAMESPACE, DEFAULT_NAMESPACE);
    Preconditions.checkArgument(
        K8sJobResourceUtils.isValidNamespace(namespace),
        "%s isn't a valid namespace name: %s",
        NAMESPACE,
        namespace);
    this.namePrefix = stringValue(configs, NAME_PREFIX, DEFAULT_NAME_PREFIX);
    Preconditions.checkArgument(
        namePrefix.length() <= MAX_NAME_PREFIX_LENGTH
            && NAME_PREFIX_PATTERN.matcher(namePrefix).matches(),
        "%s must be at most %s characters of lowercase letters, digits and '-', starting with a"
            + " letter and ending with a letter or digit: %s",
        NAME_PREFIX,
        MAX_NAME_PREFIX_LENGTH,
        namePrefix);

    this.statusCacheTtlMs = longValue(configs, STATUS_CACHE_TTL_MS, DEFAULT_STATUS_CACHE_TTL_MS);
    this.noStatusTimeoutMs =
        positiveLongValue(configs, NO_STATUS_TIMEOUT_MS, DEFAULT_NO_STATUS_TIMEOUT_MS);

    this.sparkImage = stringValue(configs, SPARK_IMAGE, null);
    Preconditions.checkArgument(sparkImage != null, "%s must be set", SPARK_IMAGE);
    this.sparkVersion = stringValue(configs, SPARK_VERSION, null);
    Preconditions.checkArgument(sparkVersion != null, "%s must be set", SPARK_VERSION);
    this.sparkServiceAccount =
        stringValue(configs, SPARK_SERVICE_ACCOUNT, DEFAULT_SPARK_SERVICE_ACCOUNT);
    this.sparkResourceRetainDurationMs =
        positiveLongValue(
            configs, SPARK_RESOURCE_RETAIN_DURATION_MS, DEFAULT_SPARK_RESOURCE_RETAIN_DURATION_MS);
    this.sparkTtlAfterStopMs =
        positiveLongValue(configs, SPARK_TTL_AFTER_STOP_MS, DEFAULT_SPARK_TTL_AFTER_STOP_MS);
    this.sparkDriverStartTimeoutMs =
        positiveLongValue(configs, SPARK_DRIVER_START_TIMEOUT_MS, DEFAULT_SPARK_START_TIMEOUT_MS);
    this.sparkDriverReadyTimeoutMs =
        positiveLongValue(configs, SPARK_DRIVER_READY_TIMEOUT_MS, DEFAULT_SPARK_START_TIMEOUT_MS);

    Map<String, String> conf = new TreeMap<>();
    configs.forEach(
        (key, value) -> {
          if (key.startsWith(SPARK_CONF_PREFIX) && key.length() > SPARK_CONF_PREFIX.length()) {
            conf.put(key.substring(SPARK_CONF_PREFIX.length()), value);
          }
        });
    this.sparkConf = ImmutableMap.copyOf(conf);
  }

  /**
   * Returns the name of the Kubernetes cluster the jobs run in.
   *
   * @return the cluster name
   */
  public String cluster() {
    return cluster;
  }

  /**
   * Returns the kubeconfig file of the job cluster.
   *
   * @return the kubeconfig file, or null if not set
   */
  @Nullable
  public String kubeconfig() {
    return kubeconfig;
  }

  /**
   * Returns the kubeconfig context to use.
   *
   * @return the kubeconfig context, or null for the current context
   */
  @Nullable
  public String context() {
    return context;
  }

  /**
   * Returns the API server URL of the job cluster.
   *
   * @return the API server URL, or null if not set
   */
  @Nullable
  public String masterUrl() {
    return masterUrl;
  }

  /**
   * Returns the CA certificate file of the API server.
   *
   * @return the CA certificate file, or null if not set
   */
  @Nullable
  public String caCertFile() {
    return caCertFile;
  }

  /**
   * Returns the bearer token file to access the API server.
   *
   * @return the token file, or null if not set
   */
  @Nullable
  public String tokenFile() {
    return tokenFile;
  }

  /**
   * Returns the default namespace of the jobs.
   *
   * @return the default namespace
   */
  public String namespace() {
    return namespace;
  }

  /**
   * Returns the prefix of the resource names.
   *
   * @return the name prefix
   */
  public String namePrefix() {
    return namePrefix;
  }

  /**
   * Returns how long the resources listed in a namespace are cached.
   *
   * @return the status cache TTL in milliseconds, caching is disabled if not positive
   */
  public long statusCacheTtlMs() {
    return statusCacheTtlMs;
  }

  /**
   * Returns the time after which a job whose resource has no status is considered failed.
   *
   * @return the no status timeout in milliseconds
   */
  public long noStatusTimeoutMs() {
    return noStatusTimeoutMs;
  }

  /**
   * Returns the default Spark image.
   *
   * @return the Spark image
   */
  public String sparkImage() {
    return sparkImage;
  }

  /**
   * Returns the Spark version of the image.
   *
   * @return the Spark version
   */
  public String sparkVersion() {
    return sparkVersion;
  }

  /**
   * Returns the default service account of the Spark driver.
   *
   * @return the driver service account
   */
  public String sparkServiceAccount() {
    return sparkServiceAccount;
  }

  /**
   * Returns how long the pods of a finished Spark job are retained.
   *
   * @return the pod retain duration in milliseconds
   */
  public long sparkResourceRetainDurationMs() {
    return sparkResourceRetainDurationMs;
  }

  /**
   * Returns the time to live of a SparkApplication after it stops.
   *
   * @return the time to live in milliseconds
   */
  public long sparkTtlAfterStopMs() {
    return sparkTtlAfterStopMs;
  }

  /**
   * Returns how long the operator waits for the driver pod to start.
   *
   * @return the timeout in milliseconds
   */
  public long sparkDriverStartTimeoutMs() {
    return sparkDriverStartTimeoutMs;
  }

  /**
   * Returns how long the operator waits for the driver to be ready.
   *
   * @return the timeout in milliseconds
   */
  public long sparkDriverReadyTimeoutMs() {
    return sparkDriverReadyTimeoutMs;
  }

  /**
   * Returns the default Spark configurations of every Spark job.
   *
   * @return the default Spark configurations
   */
  public Map<String, String> sparkConf() {
    return sparkConf;
  }

  @Nullable
  private static String stringValue(
      Map<String, String> configs, String key, @Nullable String defaultValue) {
    String value = configs.get(key);
    return StringUtils.isNotBlank(value) ? value.trim() : defaultValue;
  }

  private static long longValue(Map<String, String> configs, String key, long defaultValue) {
    String value = stringValue(configs, key, null);
    if (value == null) {
      return defaultValue;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("%s must be a number of milliseconds: %s", key, value), e);
    }
  }

  private static long positiveLongValue(
      Map<String, String> configs, String key, long defaultValue) {
    long value = longValue(configs, key, defaultValue);
    Preconditions.checkArgument(value > 0, "%s must be positive: %s", key, value);
    return value;
  }
}
