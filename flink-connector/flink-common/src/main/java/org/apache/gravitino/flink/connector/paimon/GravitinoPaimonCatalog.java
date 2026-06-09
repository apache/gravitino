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

package org.apache.gravitino.flink.connector.paimon;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogView;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConstants;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.gravitino.flink.connector.catalog.BaseCatalog;
import org.apache.gravitino.rel.Dialects;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalog;
import org.apache.paimon.flink.FlinkCatalogFactory;

/**
 * The GravitinoPaimonCatalog class is an implementation of the BaseCatalog class that is used to
 * proxy the PaimonCatalog class.
 *
 * <p>DDL operations (CREATE / ALTER / DROP) are routed through the Gravitino REST API, keeping
 * Gravitino as the single source of truth for metadata. The internal {@code paimonCatalog} is used
 * only by {@link #enrichCatalogTable} so that {@code getTable()} returns Paimon's native {@code
 * DataCatalogTable} — which carries a fully-initialised {@code CatalogEnvironment} (non-null {@code
 * catalogLoader}). Without this, the {@code AddPartitionCommitCallback} that syncs new partitions
 * to Hive Metastore is never registered, causing {@code SHOW PARTITIONS} to return empty results
 * even when {@code metastore.partitioned-table=true}.
 */
public class GravitinoPaimonCatalog extends BaseCatalog {

  private final AbstractCatalog paimonCatalog;

  protected GravitinoPaimonCatalog(
      CatalogFactory.Context context,
      String defaultDatabase,
      SchemaAndTablePropertiesConverter schemaAndTablePropertiesConverter,
      PartitionConverter partitionConverter) {
    super(
        context.getName(),
        context.getOptions(),
        defaultDatabase,
        schemaAndTablePropertiesConverter,
        partitionConverter);
    FlinkCatalogFactory flinkCatalogFactory = new FlinkCatalogFactory();
    this.paimonCatalog = flinkCatalogFactory.createCatalog(context);
  }

  // ---------------------------------------------------------------------------
  // Lifecycle — keep paimonCatalog in sync with the outer catalog
  // ---------------------------------------------------------------------------

  @Override
  public void open() throws CatalogException {
    super.open(); // opens realCatalog() == paimonCatalog, so paimonCatalog.open() is called here
  }

  @Override
  public void close() throws CatalogException {
    super.close(); // closes realCatalog() == paimonCatalog
  }

  @Override
  protected AbstractCatalog realCatalog() {
    return paimonCatalog;
  }

  // ---------------------------------------------------------------------------
  // DDL — route through Gravitino (single source of truth)
  // ---------------------------------------------------------------------------

  @Override
  protected List<String> viewDialectFallbackOrder() {
    return Arrays.asList(Dialects.FLINK, Dialects.HIVE, PaimonConstants.VIEW_QUERY_DIALECT);
  }

  @Override
  protected Representation[] buildViewRepresentations(ResolvedCatalogView view) {
    String sql = view.getExpandedQuery();
    return new Representation[] {
      SQLRepresentation.builder().withDialect(Dialects.FLINK).withSql(sql).build(),
      SQLRepresentation.builder()
          .withDialect(PaimonConstants.VIEW_QUERY_DIALECT)
          .withSql(sql)
          .build()
    };
  }

  @Override
  protected boolean dropTableEntry(NameIdentifier ident) {
    return catalog().asTableCatalog().purgeTable(ident);
  }

  @VisibleForTesting
  static Map<String, String> distributionToProperties(Distribution distribution) {
    if (distribution == null || distribution.strategy() == Strategy.NONE) {
      return new HashMap<>();
    }
    Map<String, String> properties = new HashMap<>();
    int number = distribution.number();
    Expression[] expressions = distribution.expressions();
    boolean hasExpressions = expressions != null && expressions.length > 0;

    if (number == Distributions.AUTO && !hasExpressions) {
      return properties;
    }

    String bucketKey =
        Arrays.stream(expressions)
            .map(
                e -> {
                  Preconditions.checkArgument(
                      e instanceof NamedReference,
                      "Paimon bucket-key expressions must be NamedReference, but got: %s",
                      e.getClass().getSimpleName());
                  return ((NamedReference) e).fieldName()[0];
                })
            .collect(Collectors.joining(","));
    if (StringUtils.isNotBlank(bucketKey)) {
      properties.put(PaimonConstants.BUCKET_KEY, bucketKey);
    }
    properties.put(PaimonConstants.BUCKET_NUM, String.valueOf(number));
    return properties;
  }

  // ---------------------------------------------------------------------------
  // getTable enrichment — return Paimon-native DataCatalogTable
  // ---------------------------------------------------------------------------

  @VisibleForTesting
  static Distribution getDistribution(Map<String, String> properties) {
    if (properties == null) {
      return Distributions.NONE;
    }

    String bucketKeyStr = properties.get(PaimonConstants.BUCKET_KEY);
    String bucketNumStr = properties.get(PaimonConstants.BUCKET_NUM);

    boolean hasBucketKey = StringUtils.isNotBlank(bucketKeyStr);
    boolean hasBucket = StringUtils.isNotBlank(bucketNumStr);

    if (!hasBucketKey && !hasBucket) {
      return Distributions.NONE;
    }

    Expression[] expressions = new Expression[0];
    if (hasBucketKey) {
      expressions =
          Arrays.stream(bucketKeyStr.split(","))
              .map(String::trim)
              .filter(StringUtils::isNotBlank)
              .map(NamedReference::field)
              .toArray(Expression[]::new);
    }

    if (!hasBucket) {
      return Distributions.auto(Strategy.HASH, expressions);
    }

    try {
      int parsedBucket = Integer.parseInt(bucketNumStr.trim());
      if (parsedBucket == -1) {
        return Distributions.auto(Strategy.HASH, expressions);
      }
      return Distributions.hash(parsedBucket, expressions);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format(
              "Paimon bucket number must be a valid integer, but was '%s'.", bucketNumStr),
          e);
    }
  }

  @Override
  public Optional<Factory> getFactory() {
    return paimonCatalog.getFactory();
  }

  @Override
  protected Distribution toGravitinoDistribution(Map<String, String> properties) {
    return getDistribution(properties);
  }

  @Override
  protected Map<String, String> fromGravitinoDistribution(Distribution distribution) {
    return distributionToProperties(distribution);
  }

  /**
   * Returns the Paimon-native {@code DataCatalogTable} for {@code tablePath}.
   *
   * <p>{@link BaseCatalog#getTable} has already verified via the Gravitino REST API that (a) the
   * caller is authorised and (b) the table exists before this hook is invoked. Therefore, a {@link
   * TableNotExistException} from {@code paimonCatalog.getTable()} indicates a metadata
   * inconsistency between Gravitino and the underlying Paimon store.
   *
   * <p>The returned {@code DataCatalogTable} wraps a {@code FileStoreTable} whose {@code
   * CatalogEnvironment} holds a valid {@code catalogLoader}. Paimon's write path uses this to
   * register {@code AddPartitionCommitCallback}, which in turn calls the Hive Metastore to record
   * new partitions after each checkpoint commit.
   */
  @Override
  protected CatalogBaseTable enrichCatalogTable(CatalogTable ignoredBaseTable, ObjectPath tablePath)
      throws CatalogException {
    try {
      return realCatalog().getTable(tablePath);
    } catch (TableNotExistException e) {
      throw new CatalogException(
          String.format(
              "Table '%s.%s' was found in Gravitino but is absent from the underlying Paimon "
                  + "catalog '%s'. The two metadata stores may be out of sync.",
              tablePath.getDatabaseName(), tablePath.getObjectName(), catalogName()),
          e);
    }
  }

  /**
   * Invalidates the Paimon native catalog cache for the given table.
   *
   * <p>When Paimon is initialised with a {@code CachingCatalog} (the default in production), table
   * and partition metadata is held in an in-memory cache. After DDL operations routed through
   * Gravitino (drop / rename / alter), the stale cache entry must be evicted so that the next
   * {@code getTable()} call picks up fresh metadata from the Hive Metastore or filesystem.
   *
   * <p>This implementation casts {@link #paimonCatalog} to {@link FlinkCatalog} and delegates to
   * {@code FlinkCatalog.catalog().invalidateTable(Identifier)}, which forwards to {@link
   * org.apache.paimon.catalog.CachingCatalog#invalidateTable} when caching is enabled. The call is
   * a no-op when the underlying catalog does not cache (e.g. in tests or filesystem mode).
   */
  @Override
  protected void invalidateNativeTableCache(ObjectPath tablePath) {
    AbstractCatalog nativeCatalog = realCatalog();
    if (nativeCatalog instanceof FlinkCatalog) {
      Identifier identifier =
          Identifier.create(tablePath.getDatabaseName(), tablePath.getObjectName());
      ((FlinkCatalog) nativeCatalog).catalog().invalidateTable(identifier);
    }
  }
}
