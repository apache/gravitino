/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.hive;

import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import static org.apache.hadoop.hive.metastore.TableType.MANAGED_TABLE;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.catalog.hive.converter.FromHiveType;
import com.datastrato.graviton.catalog.hive.converter.ToHiveType;
import com.datastrato.graviton.meta.rel.BaseTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.ToString;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

/** Represents a Hive Table entity in the Hive Metastore catalog. */
@ToString
@Getter
public class HiveTable extends BaseTable {

  // A set of supported Hive table types.
  public static final Set<String> SUPPORT_TABLE_TYPES =
      Sets.newHashSet(MANAGED_TABLE.name(), EXTERNAL_TABLE.name());

  // The key to store the Hive table comment in the parameters map.
  public static final String HMS_TABLE_COMMENT = "comment";

  private String location;

  private HiveTable() {}

  /**
   * Creates a new HiveTable instance from a Table and a Builder.
   *
   * @param table The inner Table representing the HiveTable.
   * @param builder The Builder used to construct the HiveTable.
   * @return A new HiveTable instance.
   */
  public static HiveTable fromInnerTable(Table table, Builder builder) {

    return builder
        .withComment(table.getParameters().get(HMS_TABLE_COMMENT))
        .withProperties(table.getParameters())
        .withColumns(
            table.getSd().getCols().stream()
                .map(
                    f ->
                        new HiveColumn.Builder()
                            .withName(f.getName())
                            .withType(FromHiveType.convert(f.getType()))
                            .withComment(f.getComment())
                            .build())
                .toArray(HiveColumn[]::new))
        .withLocation(table.getSd().getLocation())
        .build();
  }

  /**
   * Converts this HiveTable to its corresponding Table in the Hive Metastore.
   *
   * @return The converted Table.
   */
  public Table toInnerTable() {
    Table hiveTable = new Table();

    hiveTable.setTableName(name);
    hiveTable.setDbName(schemaIdentifier().name());
    hiveTable.setSd(buildStorageDescriptor());
    hiveTable.setParameters(properties);
    hiveTable.setPartitionKeys(Lists.newArrayList() /* TODO(Minghuang): Add partition support */);

    return hiveTable;
  }

  private StorageDescriptor buildStorageDescriptor() {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(
        Arrays.stream(columns)
            .map(
                c ->
                    new FieldSchema(
                        c.name(),
                        c.dataType().accept(ToHiveType.INSTANCE).getQualifiedName(),
                        c.comment()))
            .collect(Collectors.toList()));
    sd.setSerdeInfo(buildSerDeInfo());
    // `location` must not be null, otherwise it will result in an NPE when calling HMS `alterTable`
    // interface
    sd.setLocation(location);
    return sd;
  }

  private SerDeInfo buildSerDeInfo() {
    // TODO(minghuang): Build SerDeInfo object based on user specifications.
    return new SerDeInfo();
  }

  /**
   * Gets the schema identifier for this HiveTable.
   *
   * @return The schema identifier.
   */
  public NameIdentifier schemaIdentifier() {
    return NameIdentifier.of(nameIdentifier().namespace().levels());
  }

  /** A builder class for constructing HiveTable instances. */
  public static class Builder extends BaseTableBuilder<Builder, HiveTable> {

    // currently, load from HMS only
    // TODO(minghuang): Support user specify`location` property
    private String location;

    public Builder withLocation(String location) {
      this.location = location;
      return this;
    }

    /**
     * Internal method to build a HiveTable instance using the provided values.
     *
     * @return A new HiveTable instance with the configured values.
     */
    @Override
    protected HiveTable internalBuild() {
      HiveTable hiveTable = new HiveTable();
      hiveTable.id = id;
      hiveTable.schemaId = schemaId;
      hiveTable.namespace = namespace;
      hiveTable.name = name;
      hiveTable.comment = comment;
      hiveTable.properties = properties;
      hiveTable.auditInfo = auditInfo;
      hiveTable.columns = columns;
      hiveTable.location = location;

      // HMS put table comment in parameters
      hiveTable.properties.put(HMS_TABLE_COMMENT, comment);

      return hiveTable;
    }
  }
}
