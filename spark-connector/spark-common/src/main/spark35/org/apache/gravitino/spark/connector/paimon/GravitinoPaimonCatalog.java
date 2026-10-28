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

package org.apache.gravitino.spark.connector.paimon;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConstants;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonPropertiesUtils;
import org.apache.gravitino.credential.CredentialPropertyUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.spark.connector.ConnectorConstants;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.catalog.BaseCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.spark.SparkCatalog;
import org.apache.paimon.spark.SparkProcedures;
import org.apache.paimon.spark.SparkTable;
import org.apache.paimon.spark.analysis.NoSuchProcedureException;
import org.apache.paimon.spark.catalog.ProcedureCatalog;
import org.apache.paimon.spark.procedure.Procedure;
import org.apache.paimon.spark.procedure.ProcedureBuilder;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.BucketTransform;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.IdentityTransform;
import org.apache.spark.sql.connector.expressions.SortedBucketTransform;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class GravitinoPaimonCatalog extends BaseCatalog implements ProcedureCatalog {

  private static final String PRIMARY_KEY_INDEX_NAME = "PAIMON_PRIMARY_KEY_INDEX";

  /**
   * Creates Paimon table metadata with structured primary keys and bucket distribution.
   *
   * @param ident the Spark table identifier
   * @param schema the Spark table schema
   * @param transforms the requested partition and bucket transforms
   * @param properties the requested Paimon table properties
   * @return the created Gravitino table
   */
  @Override
  protected org.apache.gravitino.rel.Table createGravitinoTable(
      Identifier ident, StructType schema, Transform[] transforms, Map<String, String> properties) {
    List<Transform> partitions = new ArrayList<>();
    BucketTransform bucketTransform = null;
    if (transforms != null) {
      for (Transform transform : transforms) {
        if (transform instanceof SortedBucketTransform) {
          throw new UnsupportedOperationException("Paimon does not support SORTED BY.");
        }
        if (transform instanceof BucketTransform) {
          Preconditions.checkArgument(
              bucketTransform == null, "Paimon only supports one bucket transform.");
          bucketTransform = (BucketTransform) transform;
          continue;
        }
        if (!(transform instanceof IdentityTransform)) {
          throw new UnsupportedOperationException(
              "Paimon only supports identity partition transforms.");
        }
        String[] fields = ((IdentityTransform) transform).reference().fieldNames();
        Preconditions.checkArgument(
            fields.length == 1, "Paimon partitions must reference top-level columns.");
        String column = resolveColumns("partition", fields, schema)[0];
        partitions.add(Expressions.identity("`" + column.replace("`", "``") + "`"));
      }
    }

    if (!properties.containsKey(PaimonConstants.PRIMARY_KEY)
        && !properties.containsKey(PaimonConstants.BUCKET_NUM)
        && !properties.containsKey(PaimonConstants.BUCKET_KEY)
        && bucketTransform == null) {
      return super.createGravitinoTable(
          ident, schema, partitions.toArray(new Transform[0]), properties);
    }

    String[] primaryKeys =
        properties.containsKey(PaimonConstants.PRIMARY_KEY)
            ? parseColumns(PaimonConstants.PRIMARY_KEY, properties, schema)
            : new String[0];
    Index[] indexes =
        primaryKeys.length == 0
            ? Indexes.EMPTY_INDEXES
            : new Index[] {
              Indexes.primary(
                  PRIMARY_KEY_INDEX_NAME,
                  Arrays.stream(primaryKeys)
                      .map(key -> new String[] {key})
                      .toArray(String[][]::new))
            };
    Set<String> primaryKeyColumns = new HashSet<>(Arrays.asList(primaryKeys));
    Distribution distribution = toDistribution(properties, schema, bucketTransform);
    Column[] columns =
        Arrays.stream(schema.fields())
            .map(
                field ->
                    primaryKeyColumns.contains(field.name())
                        ? new StructField(field.name(), field.dataType(), false, field.metadata())
                        : field)
            .map(this::createGravitinoColumn)
            .toArray(Column[]::new);
    Map<String, String> tableProperties =
        propertiesConverter.toGravitinoTableProperties(properties);
    tableProperties.remove(PaimonConstants.PRIMARY_KEY);
    tableProperties.remove(PaimonConstants.BUCKET_NUM);
    tableProperties.remove(PaimonConstants.BUCKET_KEY);
    String comment = tableProperties.remove(ConnectorConstants.COMMENT);

    return gravitinoCatalogClient
        .asTableCatalog()
        .createTable(
            NameIdentifier.of(getDatabase(ident), ident.name()),
            columns,
            comment,
            tableProperties,
            sparkTransformConverter.toGravitinoPartitionings(partitions.toArray(new Transform[0])),
            distribution,
            new SortOrder[0],
            indexes);
  }

  @Override
  protected TableCatalog createAndInitSparkCatalog(
      String name, CaseInsensitiveStringMap options, Map<String, String> properties) {
    String catalogBackendName = PaimonPropertiesUtils.getCatalogBackendName(properties);
    TableCatalog paimonCatalog = new SparkCatalog();
    Map<String, String> all =
        getPropertiesConverter().toSparkCatalogProperties(options, properties);
    CredentialPropertyUtils.applyPaimonCredentials(
        CredentialPropertyUtils.getCredentials(gravitinoCatalogClient), all);
    paimonCatalog.initialize(catalogBackendName, new CaseInsensitiveStringMap(all));
    return paimonCatalog;
  }

  @Override
  protected Table createSparkTable(
      Identifier identifier,
      org.apache.gravitino.rel.Table gravitinoTable,
      Table sparkTable,
      TableCatalog sparkCatalog,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter,
      SparkTypeConverter sparkTypeConverter) {
    return new SparkPaimonTable(
        identifier,
        gravitinoTable,
        (SparkTable) sparkTable,
        propertiesConverter,
        sparkTransformConverter,
        sparkTypeConverter);
  }

  @Override
  protected PropertiesConverter getPropertiesConverter() {
    return PaimonPropertiesConverter.getInstance();
  }

  @Override
  protected SparkTransformConverter getSparkTransformConverter() {
    return new SparkTransformConverter(true);
  }

  @Override
  public boolean dropTable(Identifier ident) {
    sparkCatalog.invalidateTable(ident);
    return gravitinoCatalogClient
        .asTableCatalog()
        .purgeTable(NameIdentifier.of(getDatabase(ident), ident.name()));
  }

  /**
   * Procedures will validate the equality of the catalog registered to Spark catalogManager and the
   * catalog passed to {@code ProcedureBuilder} which invokes {@code loadProcedure()}. To meet the
   * requirement, override the method to pass {@code GravitinoPaimonCatalog} to the {@code
   * ProcedureBuilder} instead of the internal spark catalog.
   */
  @Override
  public Procedure loadProcedure(Identifier identifier) throws NoSuchProcedureException {
    if (Catalog.SYSTEM_DATABASE_NAME.equals(identifier.namespace()[0])) {
      ProcedureBuilder builder = SparkProcedures.newBuilder(identifier.name());
      if (builder != null) {
        return builder.withTableCatalog(this).build();
      }
    }
    throw new NoSuchProcedureException(identifier);
  }

  private static String[] parseColumns(
      String option, Map<String, String> properties, StructType schema) {
    String value = properties.get(option);
    Preconditions.checkArgument(StringUtils.isNotBlank(value), "%s must not be empty.", option);
    String[] columns = Arrays.stream(value.split(",", -1)).map(String::trim).toArray(String[]::new);
    return resolveColumns(option, columns, schema);
  }

  private static String[] resolveColumns(String option, String[] columns, StructType schema) {
    boolean caseSensitive = SQLConf.get().caseSensitiveAnalysis();
    return Arrays.stream(columns)
        .map(
            column -> {
              Preconditions.checkArgument(
                  !column.isEmpty(), "%s must contain non-empty, top-level column names.", option);
              String[] matches =
                  Arrays.stream(schema.fieldNames())
                      .filter(
                          name ->
                              caseSensitive ? name.equals(column) : name.equalsIgnoreCase(column))
                      .toArray(String[]::new);
              Preconditions.checkArgument(
                  matches.length > 0, "Column %s in %s does not exist.", column, option);
              Preconditions.checkArgument(
                  matches.length == 1, "Column %s in %s is ambiguous.", column, option);
              return matches[0];
            })
        .toArray(String[]::new);
  }

  private static Distribution toDistribution(
      Map<String, String> properties,
      StructType schema,
      @Nullable BucketTransform bucketTransform) {
    boolean hasBucket = properties.containsKey(PaimonConstants.BUCKET_NUM);
    boolean hasBucketKey = properties.containsKey(PaimonConstants.BUCKET_KEY);
    if (!hasBucket && !hasBucketKey && bucketTransform == null) {
      return Distributions.NONE;
    }

    String[] bucketKeys =
        hasBucketKey ? parseColumns(PaimonConstants.BUCKET_KEY, properties, schema) : new String[0];
    int buckets = Distributions.AUTO;
    if (hasBucket) {
      String value = properties.get(PaimonConstants.BUCKET_NUM);
      Preconditions.checkArgument(StringUtils.isNotBlank(value), "bucket must not be empty.");
      try {
        buckets = Integer.parseInt(value.trim());
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Paimon bucket must be a valid integer: " + value, e);
      }
    }
    if (bucketTransform != null) {
      int clusteredBuckets = (Integer) bucketTransform.numBuckets().value();
      Preconditions.checkArgument(
          clusteredBuckets > 0, "CLUSTERED BY requires a positive bucket number.");
      Preconditions.checkArgument(
          !hasBucket || buckets == clusteredBuckets,
          "CLUSTERED BY conflicts with the Paimon bucket property.");
      String[] clusteredColumns =
          Arrays.stream(bucketTransform.references())
              .map(
                  reference -> {
                    String[] fields = reference.fieldNames();
                    Preconditions.checkArgument(
                        fields.length == 1, "Paimon bucket keys must reference top-level columns.");
                    return fields[0];
                  })
              .toArray(String[]::new);
      Preconditions.checkArgument(
          clusteredColumns.length > 0, "CLUSTERED BY requires bucket keys.");
      String[] clusteredKeys = resolveColumns(PaimonConstants.BUCKET_KEY, clusteredColumns, schema);
      Preconditions.checkArgument(
          !hasBucketKey || Arrays.equals(bucketKeys, clusteredKeys),
          "CLUSTERED BY conflicts with the Paimon bucket-key property.");
      buckets = clusteredBuckets;
      bucketKeys = clusteredKeys;
    }
    Expression[] expressions =
        Arrays.stream(bucketKeys).map(NamedReference::field).toArray(Expression[]::new);
    return buckets == Distributions.AUTO
        ? Distributions.auto(Strategy.HASH, expressions)
        : Distributions.hash(buckets, expressions);
  }
}
