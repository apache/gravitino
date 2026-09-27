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
package org.apache.gravitino.policy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestIcebergRewriteManifestsContent {
  @Test
  void testDefaultsAndRegistration() {
    IcebergRewriteManifestsContent content = PolicyContents.icebergRewriteManifests();
    content.validate();
    Assertions.assertEquals(500L, content.manifestCountCritical());
    Assertions.assertEquals(100L, content.manifestCountWarning());
    Assertions.assertEquals(8388608L, content.avgManifestSizeThresholdBytes());
    Assertions.assertNull(content.specId());
    Assertions.assertNull(content.useCaching());
    Assertions.assertEquals(content, PolicyContents.icebergRewriteManifests());
    Assertions.assertEquals(
        content.hashCode(), PolicyContents.icebergRewriteManifests().hashCode());
    Assertions.assertEquals(
        IcebergRewriteManifestsContent.class,
        Policy.BuiltInType.fromPolicyType("system_iceberg_rewrite_manifests").contentClass());
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> content.rules().put("spec_id", 1));
  }

  @Test
  void testValidationAndOptions() {
    for (IcebergRewriteManifestsContent content :
        new IcebergRewriteManifestsContent[] {
          PolicyContents.icebergRewriteManifests(99L, 100L, null, null, null),
          PolicyContents.icebergRewriteManifests(null, 0L, null, null, null),
          PolicyContents.icebergRewriteManifests(null, null, 0L, null, null),
          PolicyContents.icebergRewriteManifests(null, null, null, -1, null)
        }) {
      Assertions.assertThrows(IllegalArgumentException.class, content::validate);
    }
    IcebergRewriteManifestsContent content =
        PolicyContents.icebergRewriteManifests(7L, 7L, 1L, 0, false);
    content.validate();
    Assertions.assertEquals(0, content.rules().get("spec_id"));
    Assertions.assertEquals(false, content.rules().get("use_caching"));
    Assertions.assertNotEquals(content, PolicyContents.icebergRewriteManifests());
  }
}
