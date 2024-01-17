/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.hive;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;

import com.datastrato.gravitino.trino.connector.catalog.HasPropertyMeta;
import com.google.common.collect.ImmutableList;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.VarcharType;
import java.util.Collections;
import java.util.List;

/** Implementation of {@link HasPropertyMeta} for Hive catalog. */
public class HivePropertyMeta implements HasPropertyMeta {

  private static final List<PropertyMetadata<?>> SCHEMA_PROPERTY_META =
      ImmutableList.of(
          stringProperty("location", "Hive storage location for the schema", null, false));

  public static final String HIVE_PARTITION_KEY = "partitioned_by";
  public static final String HIVE_BUCKET_KEY = "bucketed_by";
  public static final String HIVE_BUCKET_COUNT_KEY = "bucket_count";
  public static final String HIVE_SORT_ORDER_KEY = "sorted_by";

  private static final List<PropertyMetadata<?>> TABLE_PROPERTY_META =
      ImmutableList.of(
          stringProperty("format", "Hive storage format for the table", "TEXTFILE", false),
          stringProperty("total_size", "total size of the table", null, false),
          stringProperty("num_files", "number of files", null, false),
          stringProperty("external", "Indicate whether it is an external table", null, true),
          stringProperty("location", "HDFS location for table storage", null, false),
          stringProperty("table_type", "The type of Hive table", null, false),
          stringProperty(
              "input_format",
              "The input format class for the table",
              "org.apache.hadoop.mapred.TextInputFormat",
              false),
          stringProperty(
              "output_format",
              "The output format class for the table",
              "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
              false),
          stringProperty(
              "serde_lib",
              "The serde library class for the table",
              "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
              false),
          stringProperty("serde_name", "Name of the serde, table name by default", null, false),
          new PropertyMetadata<List>(
              HIVE_PARTITION_KEY,
              "The partition columns for the table",
              new ArrayType(VarcharType.createUnboundedVarcharType()),
              List.class,
              Collections.EMPTY_LIST,
              false,
              a -> (List<Object>) a,
              a -> a),
          new PropertyMetadata<List>(
              HIVE_BUCKET_KEY,
              "The bucket columns for the table",
              new ArrayType(VarcharType.createUnboundedVarcharType()),
              List.class,
              Collections.EMPTY_LIST,
              false,
              a -> (List<Object>) a,
              a -> a),
          new PropertyMetadata<List>(
              HIVE_SORT_ORDER_KEY,
              "The ordered by columns for the table",
              new ArrayType(VarcharType.createUnboundedVarcharType()),
              List.class,
              Collections.EMPTY_LIST,
              false,
              a -> (List<Object>) a,
              a -> a),
          integerProperty(
              HIVE_BUCKET_COUNT_KEY, "The number of buckets for the table", null, false));

  enum CatalogStorageFormat {
    AVRO,
    CSV,
    JSON,
    ORC,
    PARQUET,
    RCBINARY,
    RCTEXT,
    SEQUENCEFILE,
    TEXTFILE,
  }

  enum CompressionCodec {
    GZIP,
    SNAPPY,
    ZSTD,
    NONE,
  }

  enum InsertExistingPartitionsBehavior {
    ERROR,
    IGNORE,
    OVERWRITE,
    APPEND
  }

  // Hive catalog properties contain '.' and PropertyMetadata does not allow '.'
  // Those configurations are referred from Trino hive connector
  private static final List<PropertyMetadata<?>> CATALOG_PROPERTY_META =
      ImmutableList.of(
          enumProperty(
              "hive.storage-format".replace(".", "_"),
              "hive.storage-format",
              CatalogStorageFormat.class,
              CatalogStorageFormat.ORC,
              false),
          enumProperty(
              "hive.compression-codec".replace(".", "_"),
              "The compression codec to use when writing files.",
              CompressionCodec.class,
              CompressionCodec.GZIP,
              false),
          stringProperty(
              "hive.config.resources".replace(".", "_"),
              "An optional comma-separated list of HDFS configuration files. These files must exist on the machines running Trino. Only specify this if absolutely necessary to access HDFS. Example: /etc/hdfs-site.xml",
              null,
              false),
          booleanProperty(
              "hive.recursive-directories".replace(".", "_"),
              "Enable reading data from subdirectories of table or partition locations. If disabled, subdirectories are ignored. This is equivalent to the hive.mapred.supports.subdirectories property in Hive.",
              false,
              false),
          booleanProperty(
              "hive.ignore-absent-partitions".replace(".", "_"),
              "Ignore partitions when the file system location does not exist rather than failing the query. This skips data that may be expected to be part of the table.",
              false,
              false),
          booleanProperty(
              "hive.force-local-scheduling".replace(".", "_"),
              "Force splits to be scheduled on the same node as the Hadoop DataNode process serving the split data.",
              false,
              false),
          booleanProperty(
              "hive.respect-table-format".replace(".", "_"),
              "Should new partitions be written using the existing table format or the default Trino format?",
              false,
              false),
          booleanProperty(
              "hive.immutable-partitions".replace(".", "_"),
              "Can new data be inserted into existing partitions?",
              false,
              false),
          enumProperty(
              "hive.insert-existing-partitions-behavior".replace(".", "_"),
              "What happens when data is inserted into an existing partition",
              InsertExistingPartitionsBehavior.class,
              InsertExistingPartitionsBehavior.APPEND,
              false),
          stringProperty(
              "hive.target-max-file-size".replace(".", "_"),
              "Best effort maximum size of new files.",
              "1GB",
              false),
          booleanProperty(
              "hive.create-empty-bucket-files".replace(".", "_"),
              "Should empty files be created for buckets that have no data?",
              false,
              false),
          booleanProperty(
              "hive.validate-bucketing".replace(".", "_"),
              "Enables validation that data is in the correct bucket when reading bucketed tables.",
              true,
              false),
          stringProperty(
              "hive.partition-statistics-sample-size".replace(".", "_"),
              "Specifies the number of partitions to analyze when computing table statistics.",
              "100",
              false),
          stringProperty(
              "hive.max-partitions-per-writers".replace(".", "_"),
              "Specifies the number of partitions to analyze when computing table statistics.",
              "100",
              false),
          stringProperty(
              "hive.max-partitions-for-eager-load".replace(".", "_"),
              "The maximum number of partitions for a single table scan to load eagerly on the coordinator. Certain optimizations are not possible without eager loading.",
              "100000",
              false),
          stringProperty(
              "hive.max-partitions-per-scan".replace(".", "_"),
              "Maximum number of partitions for a single table scan.",
              "1000000",
              false));

  @Override
  public List<PropertyMetadata<?>> getSchemaPropertyMetadata() {
    return SCHEMA_PROPERTY_META;
  }

  @Override
  public List<PropertyMetadata<?>> getTablePropertyMetadata() {
    return TABLE_PROPERTY_META;
  }

  @Override
  public List<PropertyMetadata<?>> getColumnPropertyMetadata() {
    return HasPropertyMeta.super.getColumnPropertyMetadata();
  }

  @Override
  public List<PropertyMetadata<?>> getCatalogPropertyMeta() {
    return CATALOG_PROPERTY_META;
  }
}
