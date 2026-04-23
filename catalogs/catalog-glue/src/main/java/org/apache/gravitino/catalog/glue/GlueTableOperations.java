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
package org.apache.gravitino.catalog.glue;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.gravitino.connector.TableOperations;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.exceptions.PartitionAlreadyExistsException;
import org.apache.gravitino.rel.SupportsPartitions;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.partitions.IdentityPartition;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.CreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.DeletePartitionRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;

/**
 * Table-level partition operations for the AWS Glue Data Catalog.
 *
 * <p>Implements {@link SupportsPartitions} for Hive-format identity-partitioned tables. Partition
 * names follow the Hive convention: {@code col=val/col2=val2}.
 *
 * <p>Only {@link IdentityPartition} is supported; other partition types throw {@link
 * IllegalArgumentException}.
 */
class GlueTableOperations implements TableOperations, SupportsPartitions {

  private static final Logger LOG = LoggerFactory.getLogger(GlueTableOperations.class);

  private final GlueClient glueClient;
  /** Nullable — when null, Glue uses the caller's AWS account ID. */
  private final String catalogId;

  private final String dbName;
  private final String tableName;
  /** Ordered partition column names, matching the table's {@code partitionKeys()} order. */
  private final String[] partitionColNames;

  GlueTableOperations(
      GlueClient glueClient,
      String catalogId,
      String dbName,
      String tableName,
      String[] partitionColNames) {
    this.glueClient = glueClient;
    this.catalogId = catalogId;
    this.dbName = dbName;
    this.tableName = tableName;
    this.partitionColNames = partitionColNames;
  }

  @Override
  public String[] listPartitionNames() {
    List<String> names = new ArrayList<>();
    String nextToken = null;
    try {
      do {
        GetPartitionsRequest.Builder req =
            GetPartitionsRequest.builder().databaseName(dbName).tableName(tableName);
        if (catalogId != null) req.catalogId(catalogId);
        if (nextToken != null) req.nextToken(nextToken);
        GetPartitionsResponse resp = glueClient.getPartitions(req.build());
        for (software.amazon.awssdk.services.glue.model.Partition p : resp.partitions()) {
          names.add(buildPartitionName(p.values()));
        }
        nextToken = resp.nextToken();
      } while (nextToken != null);
    } catch (GlueException e) {
      throw new RuntimeException("Failed to list partitions for table " + tableName, e);
    }
    return names.toArray(new String[0]);
  }

  @Override
  public Partition[] listPartitions() {
    List<Partition> partitions = new ArrayList<>();
    String nextToken = null;
    try {
      do {
        GetPartitionsRequest.Builder req =
            GetPartitionsRequest.builder().databaseName(dbName).tableName(tableName);
        if (catalogId != null) req.catalogId(catalogId);
        if (nextToken != null) req.nextToken(nextToken);
        GetPartitionsResponse resp = glueClient.getPartitions(req.build());
        for (software.amazon.awssdk.services.glue.model.Partition p : resp.partitions()) {
          partitions.add(toGravitinoPartition(p));
        }
        nextToken = resp.nextToken();
      } while (nextToken != null);
    } catch (GlueException e) {
      throw new RuntimeException("Failed to list partitions for table " + tableName, e);
    }
    return partitions.toArray(new Partition[0]);
  }

  @Override
  public Partition getPartition(String partitionName) throws NoSuchPartitionException {
    List<String> values = parsePartitionName(partitionName);
    GetPartitionRequest.Builder req =
        GetPartitionRequest.builder()
            .databaseName(dbName)
            .tableName(tableName)
            .partitionValues(values);
    if (catalogId != null) req.catalogId(catalogId);
    try {
      return toGravitinoPartition(glueClient.getPartition(req.build()).partition());
    } catch (EntityNotFoundException e) {
      throw new NoSuchPartitionException(
          e, "Partition %s does not exist in table %s", partitionName, tableName);
    } catch (GlueException e) {
      throw new RuntimeException("Failed to get partition " + partitionName, e);
    }
  }

  @Override
  public Partition addPartition(Partition partition) throws PartitionAlreadyExistsException {
    Preconditions.checkArgument(
        partition instanceof IdentityPartition, "Glue only supports identity partitions");
    IdentityPartition ip = (IdentityPartition) partition;
    Preconditions.checkArgument(
        ip.values().length == partitionColNames.length,
        "Partition values count (%s) must match partition columns count (%s)",
        ip.values().length,
        partitionColNames.length);

    List<String> values = new ArrayList<>(ip.values().length);
    for (Literal<?> v : ip.values()) {
      values.add(v.value() != null ? v.value().toString() : null);
    }

    PartitionInput input =
        PartitionInput.builder()
            .values(values)
            .storageDescriptor(StorageDescriptor.builder().build())
            .build();

    CreatePartitionRequest.Builder req =
        CreatePartitionRequest.builder()
            .databaseName(dbName)
            .tableName(tableName)
            .partitionInput(input);
    if (catalogId != null) req.catalogId(catalogId);

    try {
      glueClient.createPartition(req.build());
    } catch (AlreadyExistsException e) {
      throw new PartitionAlreadyExistsException(
          e, "Partition %s already exists in table %s", partition.name(), tableName);
    } catch (GlueException e) {
      throw new RuntimeException("Failed to add partition " + partition.name(), e);
    }

    LOG.info("Added partition {} to {}.{}", partition.name(), dbName, tableName);
    return Partitions.identity(
        partition.name(), ip.fieldNames(), ip.values(), partition.properties());
  }

  @Override
  public void close() {
    // GlueClient lifecycle is managed by GlueCatalogOperations; nothing to close here.
  }

  @Override
  public boolean dropPartition(String partitionName) {
    List<String> values = parsePartitionName(partitionName);
    DeletePartitionRequest.Builder req =
        DeletePartitionRequest.builder()
            .databaseName(dbName)
            .tableName(tableName)
            .partitionValues(values);
    if (catalogId != null) req.catalogId(catalogId);
    try {
      glueClient.deletePartition(req.build());
      LOG.info("Dropped partition {} from {}.{}", partitionName, dbName, tableName);
      return true;
    } catch (EntityNotFoundException e) {
      return false;
    } catch (GlueException e) {
      throw new RuntimeException("Failed to drop partition " + partitionName, e);
    }
  }

  /**
   * Builds a Hive-style partition name (e.g. {@code dt=2024-01-01/country=us}) from an ordered list
   * of Glue partition values.
   */
  private String buildPartitionName(List<String> values) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < partitionColNames.length && i < values.size(); i++) {
      if (i > 0) sb.append('/');
      sb.append(partitionColNames[i]).append('=').append(values.get(i));
    }
    return sb.toString();
  }

  /**
   * Parses a Hive-style partition name (e.g. {@code dt=2024-01-01/country=us}) into an ordered list
   * of values, validating that the keys match the table's partition columns in order.
   */
  private List<String> parsePartitionName(String partitionName) {
    String[] parts = partitionName.split("/");
    Preconditions.checkArgument(
        parts.length == partitionColNames.length,
        "Partition name '%s' has %s segment(s) but table has %s partition column(s)",
        partitionName,
        parts.length,
        partitionColNames.length);
    List<String> values = new ArrayList<>(parts.length);
    for (int i = 0; i < parts.length; i++) {
      int eq = parts[i].indexOf('=');
      Preconditions.checkArgument(
          eq >= 0 && parts[i].substring(0, eq).equals(partitionColNames[i]),
          "Partition segment '%s' does not match expected column '%s'",
          parts[i],
          partitionColNames[i]);
      values.add(parts[i].substring(eq + 1));
    }
    return values;
  }

  private IdentityPartition toGravitinoPartition(
      software.amazon.awssdk.services.glue.model.Partition gluePartition) {
    List<String> values = gluePartition.values();
    String name = buildPartitionName(values);

    String[][] fieldNames = new String[partitionColNames.length][];
    Literal<?>[] literals = new Literal<?>[partitionColNames.length];
    for (int i = 0; i < partitionColNames.length; i++) {
      fieldNames[i] = new String[] {partitionColNames[i]};
      literals[i] = Literals.stringLiteral(i < values.size() ? values.get(i) : null);
    }
    return Partitions.identity(name, fieldNames, literals, Collections.emptyMap());
  }
}
