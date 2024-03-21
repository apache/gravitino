/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hive;

import static com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata.StorageFormat.TEXTFILE;
import static com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata.TableType.MANAGED_TABLE;
import static com.datastrato.gravitino.connector.PropertyEntry.booleanReservedPropertyEntry;
import static com.datastrato.gravitino.connector.PropertyEntry.enumImmutablePropertyEntry;
import static com.datastrato.gravitino.connector.PropertyEntry.stringImmutablePropertyEntry;
import static com.datastrato.gravitino.connector.PropertyEntry.stringReservedPropertyEntry;

import com.datastrato.gravitino.connector.BasePropertiesMetadata;
import com.datastrato.gravitino.connector.PropertyEntry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;

public class HiveTablePropertiesMetadata extends BasePropertiesMetadata {
  public static final String COMMENT = "comment";
  public static final String NUM_FILES = "numFiles";
  public static final String TOTAL_SIZE = "totalSize";
  public static final String EXTERNAL = "EXTERNAL";
  public static final String LOCATION = "location";
  public static final String FORMAT = "format";
  public static final String TABLE_TYPE = "table-type";
  public static final String INPUT_FORMAT = "input-format";
  public static final String OUTPUT_FORMAT = "output-format";
  public static final String SERDE_NAME = "serde-name";
  public static final String SERDE_LIB = "serde-lib";
  public static final String SERDE_PARAMETER_PREFIX = "serde.parameter.";
  public static final String TRANSIENT_LAST_DDL_TIME = "transient_lastDdlTime";

  public static final String TEXT_INPUT_FORMAT_CLASS = "org.apache.hadoop.mapred.TextInputFormat";
  public static final String IGNORE_KEY_OUTPUT_FORMAT_CLASS =
      "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat";
  public static final String LAZY_SIMPLE_SERDE_CLASS =
      "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";

  private static final String SEQUENCEFILE_INPUT_FORMAT_CLASS =
      "org.apache.hadoop.mapred.SequenceFileInputFormat";
  private static final String SEQUENCEFILE_OUTPUT_FORMAT_CLASS =
      "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat";

  @VisibleForTesting
  public static final String ORC_INPUT_FORMAT_CLASS =
      "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";

  @VisibleForTesting
  public static final String ORC_OUTPUT_FORMAT_CLASS =
      "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";

  @VisibleForTesting
  public static final String ORC_SERDE_CLASS = "org.apache.hadoop.hive.ql.io.orc.OrcSerde";

  private static final String PARQUET_INPUT_FORMAT_CLASS =
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
  private static final String PARQUET_OUTPUT_FORMAT_CLASS =
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
  private static final String PARQUET_SERDE_CLASS =
      "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
  private static final String COLUMNAR_SERDE_CLASS =
      "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe";
  private static final String RCFILE_INPUT_FORMAT_CLASS =
      "org.apache.hadoop.hive.ql.io.RCFileInputFormat";
  private static final String RCFILE_OUTPUT_FORMAT_CLASS =
      "org.apache.hadoop.hive.ql.io.RCFileOutputFormat";
  private static final String AVRO_INPUT_FORMAT_CLASS =
      "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat";
  private static final String AVRO_OUTPUT_FORMAT_CLASS =
      "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat";
  private static final String AVRO_SERDE_CLASS = "org.apache.hadoop.hive.serde2.avro.AvroSerDe";
  private static final String JSON_SERDE_CLASS = "org.apache.hive.hcatalog.data.JsonSerDe";

  @VisibleForTesting
  public static final String OPENCSV_SERDE_CLASS = "org.apache.hadoop.hive.serde2.OpenCSVSerde";

  private static final String REGEX_SERDE_CLASS = "org.apache.hadoop.hive.serde2.RegexSerDe";

  public enum TableType {
    MANAGED_TABLE,
    EXTERNAL_TABLE,
    VIRTUAL_VIEW,
    INDEX_TABLE,
    VIRTUAL_INDEX,
  }

  enum StorageFormat {
    SEQUENCEFILE(
        SEQUENCEFILE_INPUT_FORMAT_CLASS, SEQUENCEFILE_OUTPUT_FORMAT_CLASS, LAZY_SIMPLE_SERDE_CLASS),
    TEXTFILE(TEXT_INPUT_FORMAT_CLASS, IGNORE_KEY_OUTPUT_FORMAT_CLASS, LAZY_SIMPLE_SERDE_CLASS),
    RCFILE(RCFILE_INPUT_FORMAT_CLASS, RCFILE_OUTPUT_FORMAT_CLASS, COLUMNAR_SERDE_CLASS),
    ORC(ORC_INPUT_FORMAT_CLASS, ORC_OUTPUT_FORMAT_CLASS, ORC_SERDE_CLASS),
    PARQUET(PARQUET_INPUT_FORMAT_CLASS, PARQUET_OUTPUT_FORMAT_CLASS, PARQUET_SERDE_CLASS),
    AVRO(AVRO_INPUT_FORMAT_CLASS, AVRO_OUTPUT_FORMAT_CLASS, AVRO_SERDE_CLASS),
    JSON(TEXT_INPUT_FORMAT_CLASS, IGNORE_KEY_OUTPUT_FORMAT_CLASS, JSON_SERDE_CLASS),
    CSV(TEXT_INPUT_FORMAT_CLASS, IGNORE_KEY_OUTPUT_FORMAT_CLASS, OPENCSV_SERDE_CLASS),
    REGEX(TEXT_INPUT_FORMAT_CLASS, IGNORE_KEY_OUTPUT_FORMAT_CLASS, REGEX_SERDE_CLASS);

    private final String inputFormat;
    private final String outputFormat;
    private final String serde;

    StorageFormat(String inputFormat, String outputFormat, String serde) {
      Preconditions.checkArgument(inputFormat != null, "inputFormat must not be null");
      Preconditions.checkArgument(outputFormat != null, "outputFormat must not be null");
      Preconditions.checkArgument(serde != null, "serde must not be null");
      this.inputFormat = inputFormat;
      this.outputFormat = outputFormat;
      this.serde = serde;
    }

    public String getInputFormat() {
      return inputFormat;
    }

    public String getOutputFormat() {
      return outputFormat;
    }

    public String getSerde() {
      return serde;
    }
  }

  private static final Map<String, PropertyEntry<?>> propertiesMetadata;

  static {
    List<PropertyEntry<?>> propertyEntries =
        ImmutableList.of(
            stringReservedPropertyEntry(COMMENT, "table comment", true),
            stringReservedPropertyEntry(NUM_FILES, "number of files", false),
            stringReservedPropertyEntry(TOTAL_SIZE, "total size of the table", false),
            booleanReservedPropertyEntry(
                EXTERNAL, "Indicate whether it is an external table", false, true),
            stringImmutablePropertyEntry(
                LOCATION,
                "The location for table storage. Not required, HMS will use the database location as the parent directory by default",
                false,
                null,
                false,
                false),
            enumImmutablePropertyEntry(
                TABLE_TYPE,
                "Type of the table",
                false,
                TableType.class,
                MANAGED_TABLE,
                false,
                false),
            enumImmutablePropertyEntry(
                FORMAT,
                "The table file format",
                false,
                StorageFormat.class,
                TEXTFILE,
                false,
                false),
            stringImmutablePropertyEntry(
                INPUT_FORMAT,
                "The input format class for the table",
                false,
                TEXT_INPUT_FORMAT_CLASS,
                false,
                false),
            stringImmutablePropertyEntry(
                OUTPUT_FORMAT,
                "The output format class for the table",
                false,
                IGNORE_KEY_OUTPUT_FORMAT_CLASS,
                false,
                false),
            stringReservedPropertyEntry(TRANSIENT_LAST_DDL_TIME, "Last DDL time", false),
            stringImmutablePropertyEntry(
                SERDE_NAME, "Name of the serde, table name by default", false, null, false, false),
            stringImmutablePropertyEntry(
                SERDE_LIB,
                "The serde library class for the table",
                false,
                LAZY_SIMPLE_SERDE_CLASS,
                false,
                false));

    propertiesMetadata = Maps.uniqueIndex(propertyEntries, PropertyEntry::getName);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return propertiesMetadata;
  }
}
