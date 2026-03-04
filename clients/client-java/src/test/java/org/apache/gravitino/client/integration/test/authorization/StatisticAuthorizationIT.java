/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.client.integration.test.authorization;

import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.stats.PartitionRange;
import org.apache.gravitino.stats.PartitionStatisticsDrop;
import org.apache.gravitino.stats.PartitionStatisticsModification;
import org.apache.gravitino.stats.PartitionStatisticsUpdate;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.StatisticValues;
import org.apache.gravitino.stats.SupportsPartitionStatistics;
import org.apache.gravitino.stats.SupportsStatistics;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@Tag("gravitino-docker-test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class StatisticAuthorizationIT extends BaseRestApiAuthorizationIT {
  private static final String CATALOG = "catalog_stats";
  private static final String SCHEMA = "schema_stats";
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private static String hmsUri;
  private static String role = "role_stats";

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    containerSuite.startHiveContainer();
    super.startIntegrationTest();
    hmsUri =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);
    Map<String, String> properties = Maps.newHashMap();
    properties.put("metastore.uris", hmsUri);
    GravitinoMetalake metalake = client.loadMetalake(METALAKE);

    metalake
        .createCatalog(CATALOG, Catalog.Type.RELATIONAL, "hive", "comment", properties)
        .asSchemas()
        .createSchema(SCHEMA, "test", new HashMap<>());

    TableCatalog tableCatalog = client.loadMetalake(METALAKE).loadCatalog(CATALOG).asTableCatalog();
    tableCatalog.createTable(
        NameIdentifier.of(SCHEMA, "table1"), createColumns(), "test", new HashMap<>());

    List<SecurableObject> securableObjects = new ArrayList<>();
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(
            CATALOG, ImmutableList.of(Privileges.UseCatalog.allow(), Privileges.UseSchema.allow()));
    securableObjects.add(catalogObject);
    metalake.createRole(role, new HashMap<>(), securableObjects);
    metalake.grantRolesToUser(ImmutableList.of(role), NORMAL_USER);
  }

  private Column[] createColumns() {
    return new Column[] {Column.of("col1", Types.StringType.get())};
  }

  @Test
  @Order(1)
  public void testListStatistics() {
    TableCatalog tableCatalogNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asTableCatalog();
    // normal user can't load table1
    assertThrows(
        String.format("Can not access metadata {%s.%s.%s}.", CATALOG, SCHEMA, "table1"),
        ForbiddenException.class,
        () -> {
          tableCatalogNormalUser.loadTable(NameIdentifier.of(SCHEMA, "table1"));
        });

    // grant normal user privilege to use table1
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.grantPrivilegesToRole(
        role,
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA), MetadataObject.Type.SCHEMA),
        ImmutableList.of(Privileges.SelectTable.allow()));
    Table table = tableCatalogNormalUser.loadTable(NameIdentifier.of(SCHEMA, "table1"));

    // Can list statistics and partition statistics
    table.supportsStatistics().listStatistics();
    table
        .supportsPartitionStatistics()
        .listPartitionStatistics(PartitionRange.upTo("p0", PartitionRange.BoundType.CLOSED));
  }

  @Test
  @Order(2)
  public void testModifyStatistics() {
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    TableCatalog tableCatalogNormalUser =
        normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG).asTableCatalog();

    // normal user cannot modify table1 statistics (no privilege)
    Map<String, StatisticValue<?>> statistics = Maps.newHashMap();
    statistics.put("custom-k1", StatisticValues.longValue(100L));
    Table table = tableCatalogNormalUser.loadTable(NameIdentifier.of(SCHEMA, "table1"));
    SupportsStatistics supportsStatistics = table.supportsStatistics();
    SupportsPartitionStatistics supportsPartitionStatistics = table.supportsPartitionStatistics();

    assertThrows(
        String.format("Can not access metadata {%s.%s.%s}.", CATALOG, SCHEMA, "table1"),
        ForbiddenException.class,
        () -> {
          supportsStatistics.updateStatistics(statistics);
        });

    List<PartitionStatisticsUpdate> updates =
        Lists.newArrayList(PartitionStatisticsModification.update("p1", statistics));
    assertThrows(
        String.format("Can not access metadata {%s.%s.%s}.", CATALOG, SCHEMA, "table1"),
        ForbiddenException.class,
        () -> {
          supportsPartitionStatistics.updatePartitionStatistics(updates);
        });

    assertThrows(
        String.format("Can not access metadata {%s.%s.%s}.", CATALOG, SCHEMA, "table1"),
        ForbiddenException.class,
        () -> {
          supportsStatistics.dropStatistics(Lists.newArrayList("custom-k1"));
        });

    List<PartitionStatisticsDrop> drops =
        Lists.newArrayList(
            PartitionStatisticsModification.drop("p1", Lists.newArrayList("custom-k1")));
    assertThrows(
        String.format("Can not access metadata {%s.%s.%s}.", CATALOG, SCHEMA, "table1"),
        ForbiddenException.class,
        () -> {
          supportsPartitionStatistics.dropPartitionStatistics(drops);
        });

    // grant normal user owner privilege on table1
    gravitinoMetalake.setOwner(
        MetadataObjects.of(ImmutableList.of(CATALOG, SCHEMA, "table1"), MetadataObject.Type.TABLE),
        NORMAL_USER,
        Owner.Type.USER);

    // Can update and drop statistics and partition statistics

    supportsStatistics.updateStatistics(statistics);
    supportsPartitionStatistics.updatePartitionStatistics(updates);

    supportsStatistics.dropStatistics(Lists.newArrayList("custom-k1"));
    supportsPartitionStatistics.dropPartitionStatistics(drops);
  }
}
