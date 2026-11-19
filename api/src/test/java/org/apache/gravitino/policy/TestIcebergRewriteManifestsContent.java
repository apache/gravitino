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

import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestIcebergRewriteManifestsContent {
  @Test
  void testDefaultsAndRegistration() {
    IcebergRewriteManifestsContent content = new IcebergRewriteManifestsContent();
    content.validate();
    Assertions.assertEquals(100L, content.manifestCountThreshold());
    Assertions.assertNull(content.specId());
    Assertions.assertNull(content.useCaching());
    Assertions.assertTrue(
        content
            .rules()
            .get(IcebergRewriteManifestsContent.TRIGGER_EXPR_KEY)
            .toString()
            .contains(IcebergRewriteManifestsContent.MANIFEST_NUMBER_METRIC));
    Assertions.assertTrue(
        content.rules().containsKey(IcebergRewriteManifestsContent.MANIFEST_COUNT_THRESHOLD));
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
          PolicyContents.icebergRewriteManifests(0L, (Integer) null, (Boolean) null),
          PolicyContents.icebergRewriteManifests(null, -1, (Boolean) null)
        }) {
      Assertions.assertThrows(IllegalArgumentException.class, content::validate);
    }
    IcebergRewriteManifestsContent content = PolicyContents.icebergRewriteManifests(7L, 0, false);
    content.validate();
    Assertions.assertEquals(0, content.rules().get("spec_id"));
    Assertions.assertEquals(false, content.rules().get("use_caching"));
    Assertions.assertNotEquals(content, PolicyContents.icebergRewriteManifests());
    IcebergRewriteManifestsContent largestSpec =
        PolicyContents.icebergRewriteManifests(null, Integer.MAX_VALUE, (Boolean) null);
    largestSpec.validate();
    Assertions.assertEquals(Integer.MAX_VALUE, largestSpec.specId());
  }

  @Test
  void testParseAndResolveDefaultSpecId() {
    Assertions.assertNull(IcebergRewriteManifestsContent.parseDefaultSpecId(null));
    Assertions.assertNull(IcebergRewriteManifestsContent.parseDefaultSpecId(Map.of()));
    Assertions.assertNull(
        IcebergRewriteManifestsContent.parseDefaultSpecId(Map.of("default-spec-id", "x")));
    Assertions.assertEquals(
        7, IcebergRewriteManifestsContent.parseDefaultSpecId(Map.of("default-spec-id", "7")));
    Assertions.assertEquals(
        3, IcebergRewriteManifestsContent.resolveSpecId(3, Map.of("default-spec-id", "7")));
    Assertions.assertEquals(
        7, IcebergRewriteManifestsContent.resolveSpecId(null, Map.of("default-spec-id", "7")));
    Assertions.assertNull(IcebergRewriteManifestsContent.resolveSpecId(null, Map.of()));
  }
}
