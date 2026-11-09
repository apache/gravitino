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

import com.google.common.collect.ImmutableMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

/** The naming and labeling of the Kubernetes resources created for the jobs. */
public final class K8sJobResourceUtils {

  /** The label that marks the resources managed by Gravitino. */
  public static final String LABEL_MANAGED_BY = "app.kubernetes.io/managed-by";

  /** The {@link #LABEL_MANAGED_BY} value of the resources managed by Gravitino. */
  public static final String MANAGED_BY_GRAVITINO = "gravitino";

  /** The label whose value is the Gravitino job id of the resource. */
  public static final String LABEL_JOB_ID = "gravitino.apache.org/job-id";

  /** The annotation whose value is the metalake of the job. */
  public static final String ANNOTATION_METALAKE = "gravitino.apache.org/metalake";

  /** The annotation set when Gravitino requests to cancel the job. */
  public static final String ANNOTATION_CANCEL_REQUESTED = "gravitino.apache.org/cancel-requested";

  private static final Pattern URI_SCHEME = Pattern.compile("^([A-Za-z][A-Za-z0-9+.-]*):");

  // A DNS-1123 label, which a namespace name must be.
  private static final Pattern NAMESPACE_PATTERN =
      Pattern.compile("[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?");

  private K8sJobResourceUtils() {}

  /**
   * Returns whether the given name is a valid name of a Kubernetes namespace.
   *
   * @param namespace the namespace name
   * @return true if the name is a DNS-1123 label
   */
  public static boolean isValidNamespace(String namespace) {
    return namespace != null && NAMESPACE_PATTERN.matcher(namespace).matches();
  }

  /**
   * Returns whether a resource of a job is a URI that the job cluster can reach. A resource without
   * a scheme, with a Windows drive letter or with the {@code file} scheme is a path on the
   * Gravitino server, which the pods of the job can't reach.
   *
   * @param uri the URI of the resource
   * @return true if the URI has a scheme other than {@code file}
   */
  public static boolean isClusterReachable(String uri) {
    Matcher scheme = URI_SCHEME.matcher(uri.trim());
    return scheme.find()
        && scheme.group(1).length() > 1
        && !"file".equalsIgnoreCase(scheme.group(1));
  }

  /**
   * Returns the name of the Kubernetes resource of a job.
   *
   * @param configs the job executor configurations
   * @param jobId the Gravitino job id
   * @return the resource name
   */
  public static String resourceName(K8sJobExecutorConfigs configs, long jobId) {
    return configs.namePrefix() + "-" + jobId;
  }

  /**
   * Returns the labels of the Kubernetes resource of a job.
   *
   * @param jobId the Gravitino job id
   * @return the labels
   */
  public static Map<String, String> jobLabels(long jobId) {
    return ImmutableMap.of(
        LABEL_MANAGED_BY, MANAGED_BY_GRAVITINO, LABEL_JOB_ID, String.valueOf(jobId));
  }

  /**
   * Returns whether the Kubernetes resource was created for the given job.
   *
   * @param resource the resource
   * @param jobId the Gravitino job id
   * @return true if the resource carries the job id label of the job
   */
  public static boolean isOfJob(HasMetadata resource, long jobId) {
    Map<String, String> labels =
        resource.getMetadata() == null ? null : resource.getMetadata().getLabels();
    return labels != null && String.valueOf(jobId).equals(labels.get(LABEL_JOB_ID));
  }

  /**
   * Returns whether the Kubernetes resource is the one Gravitino created for the job its name was
   * made for: it is managed by Gravitino, and its job id label is the job id its name ends with.
   * The name alone may be used by another system, or by another Gravitino deployment with the same
   * name prefix in the namespace, for another of its jobs.
   *
   * @param resource the resource
   * @return true if the resource is a job resource of Gravitino matching its name
   */
  public static boolean isJobResource(HasMetadata resource) {
    ObjectMeta metadata = resource.getMetadata();
    Map<String, String> labels = metadata == null ? null : metadata.getLabels();
    if (labels == null || !MANAGED_BY_GRAVITINO.equals(labels.get(LABEL_MANAGED_BY))) {
      return false;
    }
    String jobId = labels.get(LABEL_JOB_ID);
    return StringUtils.isNotBlank(jobId)
        && metadata.getName() != null
        && metadata.getName().endsWith("-" + jobId);
  }

  /**
   * Returns whether Gravitino requested to cancel the job of the Kubernetes resource.
   *
   * @param resource the resource
   * @return true if the cancel annotation is set
   */
  public static boolean isCancelRequested(HasMetadata resource) {
    Map<String, String> annotations =
        resource.getMetadata() == null ? null : resource.getMetadata().getAnnotations();
    return annotations != null && "true".equals(annotations.get(ANNOTATION_CANCEL_REQUESTED));
  }
}
