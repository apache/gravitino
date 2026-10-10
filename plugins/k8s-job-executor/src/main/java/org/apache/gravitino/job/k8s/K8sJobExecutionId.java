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
import java.util.Arrays;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.exceptions.NoSuchJobException;

/**
 * The job execution id of the {@code k8s} job executor, {@code
 * <cluster>/<kind>/<namespace>/<name>}. It locates the Kubernetes resource of a job: the cluster it
 * runs in, the kind of the resource, and the resource's namespace and name.
 */
@Getter
@Accessors(fluent = true)
@EqualsAndHashCode
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class K8sJobExecutionId {

  /** The kind of the Kubernetes resource a job runs as. */
  @Getter
  @Accessors(fluent = true)
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  public enum Kind {
    /** A Spark job, run as a SparkApplication of the Spark operator. */
    SPARK_APPLICATION("sparkapp");

    /** The kind in the job execution id. */
    private final String id;

    /**
     * Returns the kind of the given id in a job execution id.
     *
     * @param id the kind in the job execution id
     * @return the kind
     * @throws IllegalArgumentException if the kind is unknown
     */
    public static Kind fromId(String id) {
      return Arrays.stream(values())
          .filter(kind -> kind.id.equals(id))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("Unknown kind of a job: " + id));
    }
  }

  private static final String SEPARATOR = "/";

  /** The cluster the resource is in. */
  private final String cluster;

  /** The kind of the resource. */
  private final Kind kind;

  /** The namespace of the resource. */
  private final String namespace;

  /** The name of the resource. */
  private final String name;

  /**
   * Creates the job execution id of a Kubernetes resource.
   *
   * @param cluster the cluster the resource is in
   * @param kind the kind of the resource
   * @param namespace the namespace of the resource
   * @param name the name of the resource
   * @return the job execution id
   */
  public static K8sJobExecutionId of(String cluster, Kind kind, String namespace, String name) {
    Preconditions.checkArgument(kind != null, "kind must not be null");
    for (String part : new String[] {cluster, namespace, name}) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(part) && !part.contains(SEPARATOR),
          "Invalid part of a job execution id: %s",
          part);
    }
    return new K8sJobExecutionId(cluster, kind, namespace, name);
  }

  /**
   * Parses a job execution id.
   *
   * @param jobExecutionId the job execution id
   * @return the parsed job execution id
   * @throws NoSuchJobException if the job execution id is malformed or of an unknown kind, so no
   *     such job exists
   */
  public static K8sJobExecutionId parse(String jobExecutionId) throws NoSuchJobException {
    String[] parts = StringUtils.defaultString(jobExecutionId).split(SEPARATOR, -1);
    if (parts.length != 4 || StringUtils.isAnyBlank(parts)) {
      throw new NoSuchJobException(
          "Invalid job execution id %s of the k8s job executor, expected"
              + " <cluster>/<kind>/<namespace>/<name>",
          jobExecutionId);
    }

    Kind kind;
    try {
      kind = Kind.fromId(parts[1]);
    } catch (IllegalArgumentException e) {
      throw new NoSuchJobException(e, "Unknown kind %s of job %s", parts[1], jobExecutionId);
    }
    return new K8sJobExecutionId(parts[0], kind, parts[2], parts[3]);
  }

  @Override
  public String toString() {
    return String.join(SEPARATOR, cluster, kind.id(), namespace, name);
  }
}
