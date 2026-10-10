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
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestK8sJobResourceUtils {

  @Test
  public void testResourceName() {
    K8sJobExecutorConfigs configs =
        new K8sJobExecutorConfigs(TestK8sJobExecutorConfigs.requiredConfigs());
    Assertions.assertEquals("gravitino-job-42", K8sJobResourceUtils.resourceName(configs, 42L));
  }

  @Test
  public void testJobLabels() {
    Assertions.assertEquals(
        ImmutableMap.of(
            "app.kubernetes.io/managed-by", "gravitino", "gravitino.apache.org/job-id", "42"),
        K8sJobResourceUtils.jobLabels(42L));

    ConfigMap resource =
        new ConfigMapBuilder()
            .withNewMetadata()
            .withLabels(K8sJobResourceUtils.jobLabels(42L))
            .endMetadata()
            .build();
    Assertions.assertTrue(K8sJobResourceUtils.isOfJob(resource, 42L));
    Assertions.assertFalse(K8sJobResourceUtils.isOfJob(resource, 43L));
    Assertions.assertFalse(
        K8sJobResourceUtils.isOfJob(
            new ConfigMapBuilder().withNewMetadata().endMetadata().build(), 42L));
  }

  @Test
  public void testIsCancelRequested() {
    Assertions.assertFalse(
        K8sJobResourceUtils.isCancelRequested(
            new ConfigMapBuilder().withNewMetadata().endMetadata().build()));
    Assertions.assertFalse(K8sJobResourceUtils.isCancelRequested(annotated("false")));
    Assertions.assertTrue(K8sJobResourceUtils.isCancelRequested(annotated("true")));
  }

  @Test
  public void testIsValidNamespace() {
    for (String valid : new String[] {"default", "team-a", "a", "ns1", "1ns"}) {
      Assertions.assertTrue(K8sJobResourceUtils.isValidNamespace(valid), valid);
    }
    for (String invalid :
        new String[] {null, "", "Team", "team_a", "-team", "team-", "a/b", "a".repeat(64)}) {
      Assertions.assertFalse(K8sJobResourceUtils.isValidNamespace(invalid), invalid);
    }
  }

  private static ConfigMap annotated(String cancelRequested) {
    return new ConfigMapBuilder()
        .withNewMetadata()
        .addToAnnotations(K8sJobResourceUtils.ANNOTATION_CANCEL_REQUESTED, cancelRequested)
        .endMetadata()
        .build();
  }
}
