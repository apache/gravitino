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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.connector.TableOperations;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.PartitionAlreadyExistsException;
import org.apache.gravitino.rel.SupportsPartitions;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.partitions.IdentityPartition;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.CreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.DeletePartitionRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;

/**
 * Table-level partition operations for the AWS Glue Data Catalog.
 *
 * <p>Implements {@link SupportsPartitions} for Hive-format identity-partitioned tables. Partition
 * names follow the Hive convention: {@code col=val/col2=val2}.
 *
 * <p>Only {@link IdentityPartition} is supported because the Glue partition model is Hive-style
 * key=value, which maps directly to identity partitions. Other partition types (bucket, truncate,
 * etc.) have no equivalent representation in the Glue partition API.
 */
class GlueTableOperations implements TableOperations, SupportsPartitions {

  private static final Logger LOG = LoggerFactory.getLogger(GlueTableOperations.class);

  private static final int MAX_PARTITION_VALUE_LENGTH = 1024;

  /** Characters Hive escapes in partition path names, besides control characters. */
  private static final String ESCAPED_PATH_CHARS = "\"#%'*/:=?\\{[]^";

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
        GlueCatalogOperations.applyCatalogId(catalogId, req::catalogId);
        if (nextToken != null) req.nextToken(nextToken);
        GetPartitionsResponse resp = glueClient.getPartitions(req.build());
        for (software.amazon.awssdk.services.glue.model.Partition p : resp.partitions()) {
          names.add(buildPartitionName(p.values()));
        }
        nextToken = resp.nextToken();
      } while (nextToken != null);
    } catch (SdkException e) {
      throw GlueExceptionConverter.toGenericException(
          e, "listing partitions of table " + tableName, glueClient);
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
        GlueCatalogOperations.applyCatalogId(catalogId, req::catalogId);
        if (nextToken != null) req.nextToken(nextToken);
        GetPartitionsResponse resp = glueClient.getPartitions(req.build());
        for (software.amazon.awssdk.services.glue.model.Partition p : resp.partitions()) {
          partitions.add(toGravitinoPartition(p));
        }
        nextToken = resp.nextToken();
      } while (nextToken != null);
    } catch (SdkException e) {
      throw GlueExceptionConverter.toGenericException(
          e, "listing partitions of table " + tableName, glueClient);
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
    GlueCatalogOperations.applyCatalogId(catalogId, req::catalogId);
    try {
      return toGravitinoPartition(glueClient.getPartition(req.build()).partition());
    } catch (EntityNotFoundException e) {
      throw new NoSuchPartitionException(
          e, "Partition %s does not exist in table %s", partitionName, tableName);
    } catch (SdkException e) {
      throw GlueExceptionConverter.toGenericException(
          e, "partition " + partitionName + " of table " + tableName, glueClient);
    }
  }

  @Override
  public Partition addPartition(Partition partition) throws PartitionAlreadyExistsException {
    Preconditions.checkArgument(
        partition instanceof IdentityPartition, "Glue only supports identity partitions");
    IdentityPartition ip = (IdentityPartition) partition;
    Preconditions.checkArgument(
        ip.values().length == partitionColNames.length
            && ip.fieldNames().length == ip.values().length,
        "Partition must give one value for each partition column %s",
        Arrays.toString(partitionColNames));

    // Glue stores values by position, so place each value under the column its field name names.
    String[] values = new String[partitionColNames.length];
    Literal<?>[] literals = new Literal<?>[partitionColNames.length];
    for (int i = 0; i < ip.values().length; i++) {
      String[] fieldName = ip.fieldNames()[i];
      int column = fieldName.length == 1 ? partitionColumnIndex(fieldName[0]) : -1;
      Preconditions.checkArgument(
          column >= 0,
          "Partition field %s is not a partition column of table %s; partition columns: %s",
          String.join(".", fieldName),
          tableName,
          Arrays.toString(partitionColNames));
      Preconditions.checkArgument(
          values[column] == null, "Partition column %s is given twice", partitionColNames[column]);
      Object value = ip.values()[i].value();
      // Glue limits partition values to 1024 characters. Null and empty values are rejected because
      // a Hive-style partition name cannot represent them.
      Preconditions.checkArgument(
          value != null && !value.toString().isEmpty(),
          "Value of partition column %s must not be null or empty",
          partitionColNames[column]);
      Preconditions.checkArgument(
          value.toString().length() <= MAX_PARTITION_VALUE_LENGTH,
          "Value of partition column %s exceeds the Glue limit of %s characters",
          partitionColNames[column],
          MAX_PARTITION_VALUE_LENGTH);
      values[column] = value.toString();
      literals[column] = ip.values()[i];
    }
    List<String> valueList = Arrays.asList(values);
    String name = buildPartitionName(valueList);
    // A given name may be written with or without Hive escaping, but must match the values.
    Preconditions.checkArgument(
        partition.name() == null
            || matchesEscapedName(partition.name(), valueList)
            || matchesUnescapedName(partition.name(), valueList),
        "Partition name %s does not match its values; expected %s",
        partition.name(),
        name);

    PartitionInput input =
        PartitionInput.builder()
            .values(valueList)
            .parameters(partition.properties())
            .storageDescriptor(partitionStorageDescriptor(name))
            .build();

    CreatePartitionRequest.Builder req =
        CreatePartitionRequest.builder()
            .databaseName(dbName)
            .tableName(tableName)
            .partitionInput(input);
    GlueCatalogOperations.applyCatalogId(catalogId, req::catalogId);

    try {
      glueClient.createPartition(req.build());
    } catch (AlreadyExistsException e) {
      throw new PartitionAlreadyExistsException(
          e, "Partition %s already exists in table %s", name, tableName);
    } catch (SdkException e) {
      throw GlueExceptionConverter.toGenericException(
          e, "adding partition " + name + " to table " + tableName, glueClient);
    }

    LOG.info("Added partition {} to {}.{}", name, dbName, tableName);
    // The partition is built from the validated request: reading it back could fail after the
    // partition was created, and Glue returns every value as a string.
    String[][] fieldNames = new String[partitionColNames.length][];
    for (int i = 0; i < partitionColNames.length; i++) {
      fieldNames[i] = new String[] {partitionColNames[i]};
    }
    return Partitions.identity(name, fieldNames, literals, partition.properties());
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
    GlueCatalogOperations.applyCatalogId(catalogId, req::catalogId);
    try {
      glueClient.deletePartition(req.build());
      LOG.info("Dropped partition {} from {}.{}", partitionName, dbName, tableName);
      return true;
    } catch (EntityNotFoundException e) {
      return false;
    } catch (SdkException e) {
      throw GlueExceptionConverter.toGenericException(
          e, "dropping partition " + partitionName + " of table " + tableName, glueClient);
    }
  }

  /**
   * Builds a Hive-style partition name (e.g. {@code dt=2024-01-01/country=us}) from an ordered list
   * of Glue partition values. Values are escaped as Hive does, so the name parses back to them.
   */
  private String buildPartitionName(List<String> values) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < partitionColNames.length && i < values.size(); i++) {
      if (i > 0) sb.append('/');
      sb.append(escapePathName(partitionColNames[i]))
          .append('=')
          .append(escapePathName(values.get(i)));
    }
    return sb.toString();
  }

  /**
   * Parses a Hive-style partition name (e.g. {@code dt=2024-01-01/country=us}) into an ordered list
   * of unescaped values, validating that the keys match the table's partition columns in order.
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
          eq >= 0
              && unescapePathName(parts[i].substring(0, eq)).equalsIgnoreCase(partitionColNames[i]),
          "Partition segment '%s' does not match expected column '%s'",
          parts[i],
          partitionColNames[i]);
      values.add(unescapePathName(parts[i].substring(eq + 1)));
    }
    return values;
  }

  /** Whether a Hive-escaped partition name names the values; column names ignore case. */
  private boolean matchesEscapedName(String name, List<String> values) {
    try {
      return parsePartitionName(name).equals(values);
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /**
   * Whether an unescaped partition name, such as {@code p=a/b} for the value {@code a/b}, names the
   * values. Column names ignore case and values are compared as is.
   */
  private boolean matchesUnescapedName(String name, List<String> values) {
    int pos = 0;
    for (int i = 0; i < partitionColNames.length; i++) {
      if (i > 0) {
        if (!name.startsWith("/", pos)) {
          return false;
        }
        pos++;
      }
      String column = partitionColNames[i];
      if (!name.regionMatches(true, pos, column, 0, column.length())
          || !name.startsWith("=", pos + column.length())) {
        return false;
      }
      pos += column.length() + 1;
      if (!name.startsWith(values.get(i), pos)) {
        return false;
      }
      pos += values.get(i).length();
    }
    return pos == name.length();
  }

  private int partitionColumnIndex(String name) {
    for (int i = 0; i < partitionColNames.length; i++) {
      if (partitionColNames[i].equalsIgnoreCase(name)) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Builds the storage descriptor of a new partition from the table's, as Hive does, so engines can
   * read it: the table's columns, formats and SerDe, and the table location plus the partition
   * path. Glue does not fill any of these in itself.
   */
  private StorageDescriptor partitionStorageDescriptor(String partitionName) {
    GetTableRequest.Builder req = GetTableRequest.builder().databaseName(dbName).name(tableName);
    GlueCatalogOperations.applyCatalogId(catalogId, req::catalogId);
    StorageDescriptor tableSd;
    try {
      tableSd = glueClient.getTable(req.build()).table().storageDescriptor();
    } catch (EntityNotFoundException e) {
      throw new NoSuchTableException(e, "Table %s.%s does not exist", dbName, tableName);
    } catch (SdkException e) {
      throw GlueExceptionConverter.toGenericException(e, "table " + tableName, glueClient);
    }
    Preconditions.checkArgument(
        tableSd != null && StringUtils.isNotBlank(tableSd.location()),
        "Cannot add a partition to table %s: the table has no storage location",
        tableName);
    return tableSd.toBuilder()
        .location(StringUtils.stripEnd(tableSd.location(), "/") + "/" + partitionName)
        .build();
  }

  /** Escapes a partition path segment the way Hive's FileUtils.escapePathName does. */
  @VisibleForTesting
  static String escapePathName(String value) {
    StringBuilder sb = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (needsEscaping(c)) {
        sb.append('%').append(String.format("%02X", (int) c));
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  /** Reverses {@link #escapePathName(String)}; text that is not a valid escape is kept as is. */
  @VisibleForTesting
  static String unescapePathName(String value) {
    StringBuilder sb = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (c == '%'
          && i + 2 < value.length()
          && Character.digit(value.charAt(i + 1), 16) >= 0
          && Character.digit(value.charAt(i + 2), 16) >= 0) {
        sb.append((char) Integer.parseInt(value.substring(i + 1, i + 3), 16));
        i += 2;
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  private static boolean needsEscaping(char c) {
    return c < ' ' || c == '\u007F' || ESCAPED_PATH_CHARS.indexOf(c) >= 0;
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
    Map<String, String> props =
        gluePartition.hasParameters() ? gluePartition.parameters() : Collections.emptyMap();
    return Partitions.identity(name, fieldNames, literals, props);
  }
}
