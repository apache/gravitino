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
package org.apache.gravitino.job.k8s.spark;

import com.google.common.collect.ImmutableMap;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.GenericKubernetesResourceBuilder;
import io.fabric8.kubernetes.client.dsl.base.ResourceDefinitionContext;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.connector.job.JobContext;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.job.k8s.K8sJobExecutorConfigs;
import org.apache.gravitino.job.k8s.K8sJobResourceUtils;

/**
 * Builds the SparkApplication of a Spark job, a custom resource of <a
 * href="https://github.com/apache/spark-kubernetes-operator">apache/spark-kubernetes-operator</a>
 * 0.9.x.
 */
public final class SparkApplicationUtils {

  /** The SparkApplication resource definition. */
  public static final ResourceDefinitionContext SPARK_APPLICATION =
      new ResourceDefinitionContext.Builder()
          .withGroup("spark.apache.org")
          .withVersion("v1")
          .withKind("SparkApplication")
          .withPlural("sparkapplications")
          .withNamespaced(true)
          .build();

  /** The label the operator sets on the pods of a SparkApplication, whose value is its name. */
  public static final String LABEL_SPARK_APP_NAME = "spark.operator/spark-app-name";

  /** The label Spark sets on its pods, whose value is the role of the pod. */
  public static final String LABEL_SPARK_ROLE = "spark-role";

  /** The {@link #LABEL_SPARK_ROLE} value of the driver pod. */
  public static final String SPARK_ROLE_DRIVER = "driver";

  /** The name Spark gives the driver container, unless a pod template renames it. */
  public static final String DRIVER_CONTAINER_NAME = "spark-kubernetes-driver";

  /** The Spark configuration of the namespace a job runs in. */
  public static final String SPARK_NAMESPACE = "spark.kubernetes.namespace";

  private static final String SPARK_MASTER = "spark.master";
  private static final String SPARK_DEPLOY_MODE = "spark.submit.deployMode";
  private static final String SPARK_IMAGE = "spark.kubernetes.container.image";
  private static final String SPARK_DRIVER_SERVICE_ACCOUNT =
      "spark.kubernetes.authenticate.driver.serviceAccountName";
  private static final String SPARK_JARS = "spark.jars";
  private static final String SPARK_FILES = "spark.files";
  private static final String SPARK_ARCHIVES = "spark.archives";

  private SparkApplicationUtils() {}

  /**
   * Builds the SparkApplication of a Spark job.
   *
   * @param context the context of the job run
   * @param template the runtime job template, whose resources are the URIs given in the template
   * @param configs the job executor configurations
   * @return the SparkApplication
   * @throws IllegalArgumentException if the job can't run on Kubernetes
   */
  public static GenericKubernetesResource buildSparkApplication(
      JobContext context, SparkJobTemplate template, K8sJobExecutorConfigs configs) {
    if (!template.environments().isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "Job template %s sets environment variables %s, which the k8s job executor doesn't"
                  + " support yet. Pass them as Spark configurations instead, for example"
                  + " spark.kubernetes.driverEnv.<name> and spark.executorEnv.<name>",
              template.name(), template.environments().keySet()));
    }

    checkClusterReachable(template.executable());
    template.jars().forEach(SparkApplicationUtils::checkClusterReachable);
    template.files().forEach(SparkApplicationUtils::checkClusterReachable);
    template.archives().forEach(SparkApplicationUtils::checkClusterReachable);

    Map<String, String> sparkConf = new TreeMap<>(configs.sparkConf());
    sparkConf.putAll(template.configs());
    // The operator decides the master and the deploy mode.
    sparkConf.remove(SPARK_MASTER);
    sparkConf.remove(SPARK_DEPLOY_MODE);
    putIfBlank(sparkConf, SPARK_IMAGE, configs.sparkImage());
    putIfBlank(sparkConf, SPARK_DRIVER_SERVICE_ACCOUNT, configs.sparkServiceAccount());
    String namespace = StringUtils.trimToNull(sparkConf.get(SPARK_NAMESPACE));
    if (namespace == null) {
      namespace = configs.namespace();
    } else if (!K8sJobResourceUtils.isValidNamespace(namespace)) {
      throw new IllegalArgumentException(
          String.format(
              "%s of job template %s isn't a valid namespace name: %s",
              SPARK_NAMESPACE, template.name(), namespace));
    }
    sparkConf.put(SPARK_NAMESPACE, namespace);
    appendList(sparkConf, SPARK_JARS, template.jars());
    appendList(sparkConf, SPARK_FILES, template.files());
    appendList(sparkConf, SPARK_ARCHIVES, template.archives());

    Map<String, Object> spec = new LinkedHashMap<>();
    if (StringUtils.isNotBlank(template.className())) {
      spec.put("mainClass", template.className());
    }
    // The operator takes the main resource of a Python application from pyFiles.
    spec.put(isPythonFile(template.executable()) ? "pyFiles" : "jars", template.executable());
    spec.put("driverArgs", new ArrayList<>(template.arguments()));
    spec.put("sparkConf", sparkConf);
    spec.put("runtimeVersions", ImmutableMap.of("sparkVersion", configs.sparkVersion()));
    spec.put(
        "applicationTolerations",
        ImmutableMap.of(
            // A Gravitino job run is one execution. Restarts would also make the failed states
            // non-final, while Gravitino never changes a job's final status.
            "restartConfig", ImmutableMap.of("restartPolicy", "Never"),
            // Retain the driver pod after the application stops, so its output can be read.
            "resourceRetainPolicy", "Always",
            "resourceRetainDurationMillis", configs.sparkResourceRetainDurationMs(),
            "ttlAfterStopMillis", configs.sparkTtlAfterStopMs(),
            // The executor start timeout isn't set: the operator only checks it against the
            // minimum number of executors of instanceConfig, which isn't set either.
            "applicationTimeoutConfig",
                ImmutableMap.of(
                    "driverStartTimeoutMillis", configs.sparkDriverStartTimeoutMs(),
                    "driverReadyTimeoutMillis", configs.sparkDriverReadyTimeoutMs())));

    GenericKubernetesResource app =
        new GenericKubernetesResourceBuilder()
            .withApiVersion(SPARK_APPLICATION.getGroup() + "/" + SPARK_APPLICATION.getVersion())
            .withKind(SPARK_APPLICATION.getKind())
            .withNewMetadata()
            .withName(K8sJobResourceUtils.resourceName(configs, context.jobId()))
            .withNamespace(namespace)
            .withLabels(K8sJobResourceUtils.jobLabels(context.jobId()))
            .addToAnnotations(K8sJobResourceUtils.ANNOTATION_METALAKE, context.metalake())
            .endMetadata()
            .build();
    app.setAdditionalProperty("spec", spec);
    return app;
  }

  private static void checkClusterReachable(String uri) {
    String scheme;
    try {
      scheme = new URI(uri).getScheme();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid resource URI of the job: " + uri, e);
    }
    if (scheme == null || "file".equalsIgnoreCase(scheme)) {
      throw new IllegalArgumentException(
          String.format(
              "Resource %s of the job is a path on the Gravitino server, which the Spark job on"
                  + " Kubernetes can't reach. Use a URI reachable from the cluster instead, such"
                  + " as local:// for a file in the image, https://, s3a:// or hdfs://",
              uri));
    }
  }

  private static boolean isPythonFile(String uri) {
    // Look at the path only, the URI may carry a query or a fragment.
    String path = URI.create(uri).getPath();
    return path != null && path.toLowerCase(Locale.ROOT).endsWith(".py");
  }

  private static void putIfBlank(Map<String, String> sparkConf, String key, String value) {
    if (StringUtils.isBlank(sparkConf.get(key))) {
      sparkConf.put(key, value);
    }
  }

  private static void appendList(Map<String, String> sparkConf, String key, List<String> values) {
    if (values.isEmpty()) {
      return;
    }
    String joined = String.join(",", values);
    String existing = sparkConf.get(key);
    sparkConf.put(key, StringUtils.isBlank(existing) ? joined : existing + "," + joined);
  }
}
