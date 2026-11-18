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

import org.apache.gravitino.exceptions.NoSuchJobException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestK8sJobExecutionId {

  @Test
  public void testFormatAndParse() {
    K8sJobExecutionId id =
        K8sJobExecutionId.of(
            "default", K8sJobExecutionId.Kind.SPARK_APPLICATION, "jobs", "gravitino-job-1");
    Assertions.assertEquals("default/sparkapp/jobs/gravitino-job-1", id.toString());

    K8sJobExecutionId parsed = K8sJobExecutionId.parse(id.toString());
    Assertions.assertEquals(id, parsed);
    Assertions.assertEquals(id.hashCode(), parsed.hashCode());
    Assertions.assertEquals("default", parsed.cluster());
    Assertions.assertEquals(K8sJobExecutionId.Kind.SPARK_APPLICATION, parsed.kind());
    Assertions.assertEquals("jobs", parsed.namespace());
    Assertions.assertEquals("gravitino-job-1", parsed.name());
  }

  @Test
  public void testKind() {
    Assertions.assertEquals("sparkapp", K8sJobExecutionId.Kind.SPARK_APPLICATION.id());
    Assertions.assertEquals(
        K8sJobExecutionId.Kind.SPARK_APPLICATION, K8sJobExecutionId.Kind.fromId("sparkapp"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> K8sJobExecutionId.Kind.fromId("job"));
  }

  @Test
  public void testInvalid() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> K8sJobExecutionId.of("a/b", K8sJobExecutionId.Kind.SPARK_APPLICATION, "ns", "n"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> K8sJobExecutionId.of("default", K8sJobExecutionId.Kind.SPARK_APPLICATION, "", "n"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> K8sJobExecutionId.of("default", null, "ns", "n"));

    for (String invalid :
        new String[] {
          null,
          "",
          "ns/name",
          "sparkapp/ns/name",
          "default/sparkapp/ns/",
          "a/b/c/d/e",
          "default/job/ns/name"
        }) {
      Assertions.assertThrows(NoSuchJobException.class, () -> K8sJobExecutionId.parse(invalid));
    }
  }
}
