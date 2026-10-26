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
package org.apache.gravitino.maintenance.integration.test;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.policy.AllValuesSelector;
import org.apache.gravitino.policy.IcebergDataCompactionContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.relational.service.PolicyMetaService;
import org.apache.gravitino.storage.relational.service.TableMetaService;
import org.apache.gravitino.utils.NameIdentifierUtil;

/**
 * Shared seed helpers for maintenance scheduler BaseITs: Iceberg catalog (H2 JDBC, no Docker), tag,
 * policy, and table association.
 */
final class MaintenanceSchedulerITSupport {

  private MaintenanceSchedulerITSupport() {}

  static SeededTable seedTaggedCompactionTable(
      GravitinoMetalake metalake,
      String metalakeName,
      @Nullable String timeTrigger,
      @Nullable Boolean commitTrigger)
      throws Exception {
    String catalogName = "ice_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    String schemaName = "db";
    String tableName = "t";
    String tagName = "tms_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    String policyName = "pol_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);

    Map<String, String> catalogProperties = new HashMap<>();
    catalogProperties.put("catalog-backend", "jdbc");
    catalogProperties.put("warehouse", "/tmp/" + metalakeName + "_" + catalogName);
    catalogProperties.put(
        "uri", "jdbc:h2:mem:" + metalakeName + "_" + catalogName + ";DB_CLOSE_DELAY=-1;MODE=MYSQL");
    catalogProperties.put("jdbc-driver", "org.h2.Driver");
    catalogProperties.put("jdbc-initialize", "true");

    Catalog catalog =
        metalake.createCatalog(
            catalogName,
            Catalog.Type.RELATIONAL,
            "lakehouse-iceberg",
            "maintenance scheduler IT",
            catalogProperties);
    catalog.asSchemas().createSchema(schemaName, "schema", ImmutableMap.of());
    catalog
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(schemaName, tableName),
            new Column[] {Column.of("id", Types.IntegerType.get())},
            "table",
            ImmutableMap.of());

    metalake.createTag(tagName, "maintenance IT tag", ImmutableMap.of());
    metalake.createPolicy(
        policyName,
        Policy.BuiltInType.ICEBERG_COMPACTION.policyType(),
        "maintenance scheduler IT policy",
        true,
        PolicyContents.icebergDataCompaction(
            IcebergDataCompactionContent.DEFAULT_MIN_DATA_FILE_MSE,
            IcebergDataCompactionContent.DEFAULT_MIN_DELETE_FILE_NUMBER,
            IcebergDataCompactionContent.DEFAULT_DATA_FILE_MSE_WEIGHT,
            IcebergDataCompactionContent.DEFAULT_DELETE_FILE_NUMBER_WEIGHT,
            IcebergDataCompactionContent.DEFAULT_MAX_PARTITION_NUM,
            IcebergDataCompactionContent.DEFAULT_REWRITE_STRATEGY,
            IcebergDataCompactionContent.DEFAULT_SORT_ORDER,
            Map.of(),
            timeTrigger,
            commitTrigger,
            /*minIntervalMs=*/ 0L,
            Map.of("spark_executor_memory", "2g"),
            Map.of()));
    metalake.addPolicyForTag(tagName, policyName, AllValuesSelector.get());

    catalog
        .asTableCatalog()
        .loadTable(NameIdentifier.of(schemaName, tableName))
        .supportsTags()
        .associateTags(new String[] {tagName}, null);

    TableEntity table =
        TableMetaService.getInstance()
            .getTableByIdentifier(
                NameIdentifierUtil.ofTable(metalakeName, catalogName, schemaName, tableName));
    PolicyEntity policy =
        PolicyMetaService.getInstance()
            .getPolicyByIdentifier(NameIdentifierUtil.ofPolicy(metalakeName, policyName));
    return new SeededTable(table.id(), policy.id(), catalogName, schemaName, tableName);
  }

  static final class SeededTable {
    final long tableId;
    final long policyId;
    final String catalogName;
    final String schemaName;
    final String tableName;

    SeededTable(
        long tableId, long policyId, String catalogName, String schemaName, String tableName) {
      this.tableId = tableId;
      this.policyId = policyId;
      this.catalogName = catalogName;
      this.schemaName = schemaName;
      this.tableName = tableName;
    }
  }
}
