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
package org.apache.gravitino.authorization.ranger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.MetadataObjectChange;
import org.apache.gravitino.authorization.ranger.reference.RangerDefines.PolicyResource;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.util.SearchFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestRangerAuthorizationHadoopSQLPlugin {

  private final List<RangerPolicy> policies = new ArrayList<>();
  private RangerAuthorizationHadoopSQLPlugin plugin;

  @BeforeEach
  public void setUp() throws Exception {
    policies.clear();
    // The constructor connects to Ranger, so only the tested methods run for real.
    plugin = Mockito.mock(RangerAuthorizationHadoopSQLPlugin.class, Mockito.CALLS_REAL_METHODS);
    RangerClientExtension rangerClient = Mockito.mock(RangerClientExtension.class);
    // An in-memory Ranger: a search returns the policies whose resources match every filter.
    Mockito.when(rangerClient.findPolicies(any()))
        .thenAnswer(invocation -> findPolicies(invocation.getArgument(0)));
    Mockito.when(rangerClient.updatePolicy(anyLong(), any()))
        .thenAnswer(invocation -> invocation.getArgument(1));
    plugin.setRangerClient(rangerClient);
  }

  @Test
  public void testRenameTableAcrossSchemasMovesOnlyThatTablesPolicies() {
    RangerPolicy schemaPolicy = addPolicy(1, "s1", "s1", null, null);
    RangerPolicy tablePolicy = addPolicy(2, "s1.t1", "s1", "t1", null);
    RangerPolicy columnPolicy = addPolicy(3, "s1.t1.*", "s1", "t1", "*");
    RangerPolicy otherTablePolicy = addPolicy(4, "s1.t9", "s1", "t9", null);

    plugin.onMetadataUpdated(
        MetadataObjectChange.rename(
            MetadataObjects.parse("catalog.s1.t1", MetadataObject.Type.TABLE),
            MetadataObjects.parse("catalog.s2.t2", MetadataObject.Type.TABLE),
            null));

    // The renamed table's policies follow it to the new schema and name.
    Assertions.assertEquals("s2.t2", tablePolicy.getName());
    Assertions.assertEquals(resources("s2", "t2", null), resourcesOf(tablePolicy));
    Assertions.assertEquals(resources("s2", "t2", "*"), resourcesOf(columnPolicy));
    // The old schema and its other tables keep their policies.
    Assertions.assertEquals("s1", schemaPolicy.getName());
    Assertions.assertEquals(resources("s1", null, null), resourcesOf(schemaPolicy));
    Assertions.assertEquals("s1.t9", otherTablePolicy.getName());
    Assertions.assertEquals(resources("s1", "t9", null), resourcesOf(otherTablePolicy));
  }

  @Test
  public void testRenameTableInSameSchemaRenamesOnlyThatTablesPolicies() {
    RangerPolicy schemaPolicy = addPolicy(1, "s1", "s1", null, null);
    RangerPolicy tablePolicy = addPolicy(2, "s1.t1", "s1", "t1", null);
    RangerPolicy otherTablePolicy = addPolicy(3, "s1.t9", "s1", "t9", null);

    plugin.onMetadataUpdated(
        MetadataObjectChange.rename(
            MetadataObjects.parse("catalog.s1.t1", MetadataObject.Type.TABLE),
            MetadataObjects.parse("catalog.s1.t2", MetadataObject.Type.TABLE),
            null));

    Assertions.assertEquals("s1.t2", tablePolicy.getName());
    Assertions.assertEquals(resources("s1", "t2", null), resourcesOf(tablePolicy));
    Assertions.assertEquals(resources("s1", null, null), resourcesOf(schemaPolicy));
    Assertions.assertEquals(resources("s1", "t9", null), resourcesOf(otherTablePolicy));
  }

  private RangerPolicy addPolicy(long id, String name, String db, String table, String column) {
    RangerPolicy policy = new RangerPolicy();
    policy.setId(id);
    policy.setName(name);
    Map<String, RangerPolicy.RangerPolicyResource> policyResources = new HashMap<>();
    resources(db, table, column)
        .forEach((k, v) -> policyResources.put(k, new RangerPolicy.RangerPolicyResource(v)));
    policy.setResources(policyResources);
    policies.add(policy);
    return policy;
  }

  private List<RangerPolicy> findPolicies(Map<String, String> filters) {
    return policies.stream()
        .filter(
            policy ->
                filters.entrySet().stream()
                    .filter(e -> e.getKey().startsWith(SearchFilter.RESOURCE_PREFIX))
                    .allMatch(
                        e ->
                            Objects.equals(
                                resourcesOf(policy)
                                    .get(
                                        e.getKey()
                                            .substring(SearchFilter.RESOURCE_PREFIX.length())),
                                e.getValue())))
        .collect(Collectors.toList());
  }

  private static Map<String, String> resources(String db, String table, String column) {
    Map<String, String> resources = new HashMap<>();
    resources.put(PolicyResource.DATABASE.getName(), db);
    if (table != null) {
      resources.put(PolicyResource.TABLE.getName(), table);
    }
    if (column != null) {
      resources.put(PolicyResource.COLUMN.getName(), column);
    }
    return resources;
  }

  private static Map<String, String> resourcesOf(RangerPolicy policy) {
    return policy.getResources().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getValues().get(0)));
  }
}
