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
package org.apache.gravitino.catalog.hive;

import static org.apache.gravitino.catalog.hive.HiveConstants.COMMENT;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.ToString;
import org.apache.gravitino.Audit;
import org.apache.gravitino.connector.BaseTable;
import org.apache.gravitino.connector.TableOperations;
import org.apache.gravitino.hive.CachedClientPool;
import org.apache.gravitino.hive.converter.HiveDataTypeConverter;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.SupportsPartitions;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

/** Represents an Apache Hive Table entity in the Hive Metastore catalog. */
@ToString
public class HiveTable extends BaseTable {

  // A set of supported Hive table types.
  public static final Set<String> SUPPORT_TABLE_TYPES =
      Sets.newHashSet(TableType.MANAGED_TABLE.name(), TableType.EXTERNAL_TABLE.name());
  public static final String ICEBERG_TABLE_TYPE_VALUE = "ICEBERG";
  public static final String TABLE_TYPE_PROP = "table_type";
  protected String schemaName;
  protected CachedClientPool clientPool;

  protected HiveTable() {}

  /**
   * Creates a new HiveTable instance from a Table and a Builder.
   *
   * @param table The inner Table representing the HiveTable.
   * @return A new HiveTable instance Builder.
   */
  public static HiveTable.Builder fromHiveTable(String dbName, Table table) {
    Audit audit = table.auditInfo();
    AuditInfo auditInfo =
        audit != null
            ? AuditInfo.builder()
                .withCreator(audit.creator())
                .withCreateTime(audit.createTime())
                .withLastModifier(audit.lastModifier())
                .withLastModifiedTime(audit.lastModifiedTime())
                .build()
            : AuditInfo.EMPTY;

    return HiveTable.builder()
        .withName(table.name())
        .withComment(table.comment())
        .withProperties(table.properties())
        .withColumns(table.columns())
        .withDistribution(table.distribution())
        .withSortOrders(table.sortOrder())
        .withAuditInfo(auditInfo)
        .withPartitioning(table.partitioning())
        .withSchemaName(dbName);
  }

  public CachedClientPool clientPool() {
    return clientPool;
  }

  public void close() {
    if (clientPool != null) {
      // Note: Cannot close the client pool here because the client pool is shared by catalog
      clientPool = null;
    }
  }

  public String schemaName() {
    return schemaName;
  }

  public List<FieldSchema> buildPartitionKeys() {
    return Arrays.stream(partitioning)
        .map(p -> getPartitionKey(((Transforms.IdentityTransform) p).fieldName()))
        .collect(Collectors.toList());
  }

  private FieldSchema getPartitionKey(String[] fieldName) {
    List<Column> partitionColumns =
        Arrays.stream(columns)
            .filter(c -> c.name().equals(fieldName[0]))
            .collect(Collectors.toList());
    return new FieldSchema(
        partitionColumns.get(0).name(),
        HiveDataTypeConverter.CONVERTER
            .fromGravitino(partitionColumns.get(0).dataType())
            .getQualifiedName(),
        partitionColumns.get(0).comment());
  }

  @Override
  protected TableOperations newOps() {
    return new HiveTableOperations(this);
  }

  @Override
  public SupportsPartitions supportPartitions() throws UnsupportedOperationException {
    return (SupportsPartitions) ops();
  }

  /** A builder class for constructing HiveTable instances. */
  public static class Builder extends BaseTableBuilder<Builder, HiveTable> {

    private String schemaName;
    private CachedClientPool clientPool;

    /**
     * Sets the Hive schema (database) name to be used for building the HiveTable.
     *
     * @param schemaName The string schema name of the HiveTable.
     * @return This Builder instance.
     */
    public Builder withSchemaName(String schemaName) {
      this.schemaName = schemaName;
      return this;
    }

    /**
     * Sets the HiveClientPool to be used for operate partition.
     *
     * @param clientPool The HiveClientPool instance.
     * @return This Builder instance.
     */
    public Builder withClientPool(CachedClientPool clientPool) {
      this.clientPool = clientPool;
      return this;
    }

    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    /**
     * Internal method to build a HiveTable instance using the provided values.
     *
     * @return A new HiveTable instance with the configured values.
     */
    @Override
    protected HiveTable internalBuild() {
      HiveTable hiveTable = new HiveTable();
      hiveTable.name = name;
      hiveTable.comment = comment;
      hiveTable.properties = properties != null ? Maps.newHashMap(properties) : Maps.newHashMap();
      hiveTable.auditInfo = auditInfo;
      hiveTable.columns = columns;
      hiveTable.distribution = distribution;
      hiveTable.sortOrders = sortOrders;
      hiveTable.partitioning = partitioning;
      hiveTable.schemaName = schemaName;
      hiveTable.clientPool = clientPool;
      hiveTable.proxyPlugin = proxyPlugin;

      // HMS put table comment in parameters
      if (comment != null) {
        hiveTable.properties.put(COMMENT, comment);
      }

      return hiveTable;
    }
  }

  /**
   * Creates a new instance of {@link Builder}.
   *
   * @return The new instance.
   */
  public static Builder builder() {
    return new Builder();
  }
}
