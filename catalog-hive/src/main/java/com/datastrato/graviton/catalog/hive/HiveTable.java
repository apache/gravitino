/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.hive;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.catalog.hive.converter.FromHiveType;
import com.datastrato.graviton.catalog.hive.converter.ToHiveType;
import com.datastrato.graviton.meta.rel.BaseTable;
import java.util.Arrays;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.ToString;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

/** A Hive Table entity */
@ToString
@Getter
public class HiveTable extends BaseTable {

  public static final String HMS_TABLE_COMMENT = "comment";

  /** original view text, null for non-view */
  private String viewOriginalText;

  /** expanded view text, null for non-view */
  private String viewExpandedText;

  private String inputFormat;

  private String outputFormat;

  private String serLib;

  private HiveTable() {}

  public static HiveTable fromInnerTable(Table table, Builder builder) {

    return builder
        .withViewOriginalText(table.getViewOriginalText())
        .withViewExpandedText(table.getViewExpandedText())
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
        .build();
  }

  public Table toInnerTable() {
    Table hiveTable = new Table();

    hiveTable.setTableName(name);
    hiveTable.setDbName(schemaIdentifier().name());
    hiveTable.setOwner(auditInfo.creator());
    hiveTable.setSd(buildStorageDescriptor());

    return hiveTable;
  }

  private StorageDescriptor buildStorageDescriptor() {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(
        Arrays.stream(columns)
            .map(
                c ->
                    new FieldSchema(
                        c.name(), c.dataType().accept(ToHiveType.INSTANCE), c.comment()))
            .collect(Collectors.toList()));
    sd.setInputFormat(inputFormat);
    sd.setOutputFormat(outputFormat);
    sd.setSerdeInfo(buildSerDeInfo());
    return sd;
  }

  private SerDeInfo buildSerDeInfo() {
    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setName(name);
    serDeInfo.setSerializationLib(serLib);
    return serDeInfo;
  }

  public NameIdentifier schemaIdentifier() {
    return NameIdentifier.of(nameIdentifier().namespace().levels());
  }

  public static class Builder extends BaseTableBuilder<Builder, HiveTable> {

    private String viewOriginalText;
    private String viewExpandedText;

    private String inputFormat;

    private String outputFormat;

    private String serLib;

    public Builder withViewOriginalText(String viewOriginalText) {
      this.viewOriginalText = viewOriginalText;
      return this;
    }

    public Builder withViewExpandedText(String viewExpandedText) {
      this.viewExpandedText = viewExpandedText;
      return this;
    }

    public Builder withInputFormat(String inputFormat) {
      this.inputFormat = inputFormat;
      return this;
    }

    public Builder withOutputFormat(String outputFormat) {
      this.outputFormat = outputFormat;
      return this;
    }

    public Builder withSerLib(String serLib) {
      this.serLib = serLib;
      return this;
    }

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
      hiveTable.viewOriginalText = viewOriginalText;
      hiveTable.viewExpandedText = viewExpandedText;
      hiveTable.inputFormat = inputFormat;
      hiveTable.outputFormat = outputFormat;
      hiveTable.serLib = serLib;

      // HMS put table comment in parameters
      hiveTable.properties.put(HMS_TABLE_COMMENT, comment);

      return hiveTable;
    }
  }
}
