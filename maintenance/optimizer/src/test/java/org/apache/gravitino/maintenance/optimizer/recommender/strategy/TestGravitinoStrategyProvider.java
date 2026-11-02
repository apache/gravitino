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

package org.apache.gravitino.maintenance.optimizer.recommender.strategy;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.maintenance.optimizer.api.common.Strategy;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.SupportsPolicies;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestGravitinoStrategyProvider {
  @Test
  void testStrategiesUseApplicablePolicyInfos() throws IllegalAccessException {
    GravitinoClient client = mock(GravitinoClient.class);
    Catalog catalog = mock(Catalog.class);
    TableCatalog tableCatalog = mock(TableCatalog.class);
    Table table = mock(Table.class);
    SupportsPolicies supportsPolicies = mock(SupportsPolicies.class);
    Policy policy1 = mock(Policy.class);
    Policy policy2 = mock(Policy.class);
    NameIdentifier identifier = NameIdentifier.of("catalog", "schema", "table");

    when(client.loadCatalog("catalog")).thenReturn(catalog);
    when(catalog.asTableCatalog()).thenReturn(tableCatalog);
    when(tableCatalog.loadTable(NameIdentifier.of("schema", "table"))).thenReturn(table);
    when(table.supportsPolicies()).thenReturn(supportsPolicies);
    when(supportsPolicies.listPolicyInfos()).thenReturn(new Policy[] {policy1, null, policy2});
    when(policy1.name()).thenReturn("policy1");
    when(policy2.name()).thenReturn("policy2");

    GravitinoStrategyProvider provider = new GravitinoStrategyProvider();
    FieldUtils.writeField(provider, "gravitinoClient", client, true);

    List<Strategy> strategies = provider.strategies(identifier);

    Assertions.assertArrayEquals(
        new String[] {"policy1", "policy2"},
        strategies.stream().map(Strategy::name).toArray(String[]::new));
    verify(supportsPolicies).listPolicyInfos();
    verifyNoMoreInteractions(supportsPolicies);
  }
}
