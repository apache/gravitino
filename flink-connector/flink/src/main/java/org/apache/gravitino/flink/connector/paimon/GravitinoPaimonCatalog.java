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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConstants;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.gravitino.flink.connector.catalog.BaseCatalog;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.FlinkTableFactory;

/**
 * The GravitinoPaimonCatalog class is an implementation of the BaseCatalog class that is used to
 * proxy the PaimonCatalog class.
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

  @Override
  protected AbstractCatalog realCatalog() {
    return paimonCatalog;
  }

  @Override
  public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    boolean dropped =
        catalog()
            .asTableCatalog()
            .purgeTable(NameIdentifier.of(tablePath.getDatabaseName(), tablePath.getObjectName()));
    if (!dropped && !ignoreIfNotExists) {
      throw new TableNotExistException(catalogName(), tablePath);
    }
  }

  @Override
  public Optional<Factory> getFactory() {
    return Optional.of(new FlinkTableFactory());
  }

  @Override
  protected Distribution toGravitinoDistribution(
      Map<String, String> properties, List<String> primaryKeys) {
    return getDistribution(properties, primaryKeys);
  }

  @Override
  protected Map<String, String> fromGravitinoDistribution(Distribution distribution) {
    return distributionToProperties(distribution);
  }

  @VisibleForTesting
  static Map<String, String> distributionToProperties(Distribution distribution) {
    if (distribution == null
        || distribution.strategy() == Strategy.NONE
        || distribution.expressions().length == 0) {
      return new HashMap<>();
    }
    Map<String, String> properties = new HashMap<>();
    Arrays.stream(distribution.expressions())
        .forEach(
            e ->
                Preconditions.checkArgument(
                    e instanceof NamedReference,
                    "Paimon bucket-key expressions must be NamedReference, but got: %s",
                    e.getClass().getSimpleName()));
    int number = distribution.number();
    // Paimon does not allow 'bucket-key' with bucket=-1 (dynamic mode).
    // In dynamic mode, Paimon uses the primary key as the bucket key automatically.
    if (number != Distributions.AUTO) {
      String bucketKey =
          Arrays.stream(distribution.expressions())
              .map(e -> ((NamedReference) e).fieldName()[0])
              .collect(Collectors.joining(","));
      if (StringUtils.isNotBlank(bucketKey)) {
        properties.put(PaimonConstants.BUCKET_KEY, bucketKey);
      }
    }
    properties.put(
        PaimonConstants.BUCKET_NUM, String.valueOf(number == Distributions.AUTO ? -1 : number));
    return properties;
  }

  @VisibleForTesting
  static Distribution getDistribution(Map<String, String> properties, List<String> primaryKeys) {
    if (properties == null) {
      return Distributions.NONE;
    }

    // Resolve bucket key columns: explicit bucket-key, else fall back to PK
    String bucketKeys = properties.get(PaimonConstants.BUCKET_KEY);
    List<String> bucketKeyList;
    if (StringUtils.isNotBlank(bucketKeys)) {
      bucketKeyList =
          Arrays.stream(bucketKeys.split(","))
              .map(String::trim)
              .filter(StringUtils::isNotBlank)
              .collect(Collectors.toList());
    } else {
      bucketKeyList = (primaryKeys != null) ? primaryKeys : Collections.emptyList();
    }

    String bucketNum = properties.get(PaimonConstants.BUCKET_NUM);

    if (bucketKeyList.isEmpty()) {
      if (StringUtils.isNotBlank(bucketNum)) {
        throw new IllegalArgumentException(
            "Paimon 'bucket' is set but no 'bucket-key' or primary key is defined. "
                + "Please specify 'bucket-key' or define a primary key.");
      }
      return Distributions.NONE;
    }

    Expression[] expressions =
        bucketKeyList.stream().map(NamedReference::field).toArray(Expression[]::new);

    if (StringUtils.isBlank(bucketNum)) {
      return Distributions.auto(Strategy.HASH, expressions);
    }

    String trimmedBucketValue = bucketNum.trim();
    try {
      int parsedBucket = Integer.parseInt(trimmedBucketValue);
      if (parsedBucket == -1) {
        return Distributions.auto(Strategy.HASH, expressions);
      }
      if (parsedBucket <= 0) {
        throw new IllegalArgumentException(
            String.format(
                "Paimon bucket number must be a positive integer or -1 (dynamic mode), but was '%s'.",
                bucketNum));
      }
      return Distributions.hash(parsedBucket, expressions);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("Paimon bucket number must be a valid integer, but was '%s'.", bucketNum),
          e);
    }
  }
}
