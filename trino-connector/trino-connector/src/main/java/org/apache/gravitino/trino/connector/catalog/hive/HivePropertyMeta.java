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

package org.apache.gravitino.trino.connector.catalog.hive;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;

import com.google.common.collect.ImmutableList;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;
import java.util.List;
import org.apache.gravitino.trino.connector.catalog.HasPropertyMeta;

/** Implementation of {@link HasPropertyMeta} for Apache Hive catalog. */
public class HivePropertyMeta implements HasPropertyMeta {

  static final String HIVE_SCHEMA_LOCATION = "location";

  private static final List<PropertyMetadata<?>> SCHEMA_PROPERTY_META =
      ImmutableList.of(
          stringProperty(
              HIVE_SCHEMA_LOCATION, "Hive storage location for the schema", null, false));

  static final String HIVE_TABLE_FORMAT = "format";
  static final String HIVE_TABLE_TOTAL_SIZE = "total_size";
  static final String HIVE_TABLE_NUM_FILES = "num_files";
  static final String HIVE_TABLE_EXTERNAL = "external";
  static final String HIVE_TABLE_LOCATION = "location";
  static final String HIVE_TABLE_TYPE = "table_type";
  static final String HIVE_TABLE_INPUT_FORMAT = "input_format";
  static final String HIVE_TABLE_OUTPUT_FORMAT = "output_format";
  static final String HIVE_TABLE_SERDE_LIB = "serde_lib";
  static final String HIVE_TABLE_SERDE_NAME = "serde_name";
  static final String HIVE_PARTITION_KEY = "partitioned_by";
  static final String HIVE_BUCKET_KEY = "bucketed_by";
  static final String HIVE_BUCKET_COUNT_KEY = "bucket_count";
  static final String HIVE_SORT_ORDER_KEY = "sorted_by";

  private static final List<PropertyMetadata<?>> TABLE_PROPERTY_META =
      ImmutableList.of(
          stringProperty(HIVE_TABLE_FORMAT, "Hive storage format for the table", "TEXTFILE", false),
          stringProperty(HIVE_TABLE_TOTAL_SIZE, "total size of the table", null, false),
          stringProperty(HIVE_TABLE_NUM_FILES, "number of files", null, false),
          stringProperty(
              HIVE_TABLE_EXTERNAL, "Indicate whether it is an external table", null, true),
          stringProperty(HIVE_TABLE_LOCATION, "HDFS location for table storage", null, false),
          stringProperty(HIVE_TABLE_TYPE, "The type of Hive table", null, false),
          stringProperty(
              HIVE_TABLE_INPUT_FORMAT, "The input format class for the table", null, false),
          stringProperty(
              HIVE_TABLE_OUTPUT_FORMAT, "The output format class for the table", null, false),
          stringProperty(
              HIVE_TABLE_SERDE_LIB, "The serde library class for the table", null, false),
          stringProperty(
              HIVE_TABLE_SERDE_NAME, "Name of the serde, table name by default", null, false),
          new PropertyMetadata<>(
              HIVE_PARTITION_KEY,
              "Partition columns",
              new ArrayType(VARCHAR),
              List.class,
              ImmutableList.of(),
              false,
              value ->
                  ((List<?>) value)
                      .stream()
                          .map(name -> ((String) name).toLowerCase(ENGLISH))
                          .collect(toImmutableList()),
              value -> value),
          new PropertyMetadata<>(
              HIVE_BUCKET_KEY,
              "Bucketing columns",
              new ArrayType(VARCHAR),
              List.class,
              ImmutableList.of(),
              false,
              value ->
                  ((List<?>) value)
                      .stream()
                          .map(name -> ((String) name).toLowerCase(ENGLISH))
                          .collect(toImmutableList()),
              value -> value),
          integerProperty(
              HIVE_BUCKET_COUNT_KEY, "The number of buckets for the table", null, false),
          new PropertyMetadata<>(
              HIVE_SORT_ORDER_KEY,
              "Bucket sorting columns",
              new ArrayType(VARCHAR),
              List.class,
              ImmutableList.of(),
              false,
              value ->
                  ((List<?>) value)
                      .stream()
                          .map(String.class::cast)
                          .map(String::toLowerCase)
                          .map(SortingColumn::sortingColumnFromString)
                          .collect(toImmutableList()),
              value ->
                  ((List<?>) value)
                      .stream()
                          .map(SortingColumn.class::cast)
                          .map(SortingColumn::sortingColumnToString)
                          .collect(toImmutableList())));

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
  // Those configurations are referred from Trino Hive connector
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
