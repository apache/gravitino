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

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConstants;
import org.apache.gravitino.flink.connector.CatalogPropertiesConverter;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test for {@link PaimonPropertiesConverter} */
public class TestPaimonPropertiesConverter {

  private static final PaimonPropertiesConverter CONVERTER = PaimonPropertiesConverter.INSTANCE;

  private static final String localWarehouse = "file:///tmp/paimon_warehouse";

  @Test
  public void testToPaimonFileSystemCatalog() {
    Map<String, String> catalogProperties = ImmutableMap.of("warehouse", localWarehouse);
    Map<String, String> flinkCatalogProperties =
        CONVERTER.toFlinkCatalogProperties(catalogProperties);
    Assertions.assertEquals(
        GravitinoPaimonCatalogFactoryOptions.IDENTIFIER, flinkCatalogProperties.get("type"));
    Assertions.assertEquals(localWarehouse, flinkCatalogProperties.get("warehouse"));
  }

  @Test
  public void testToPaimonJdbcCatalog() {
    String testUser = "testUser";
    String testPassword = "testPassword";
    String testUri = "testUri";
    Map<String, String> catalogProperties =
        ImmutableMap.of(
            PaimonConstants.WAREHOUSE,
            localWarehouse,
            PaimonConstants.CATALOG_BACKEND,
            "jdbc",
            PaimonConstants.GRAVITINO_JDBC_USER,
            testUser,
            PaimonConstants.GRAVITINO_JDBC_PASSWORD,
            testPassword,
            CatalogPropertiesConverter.FLINK_PROPERTY_PREFIX + PaimonConstants.URI,
            testUri);
    Map<String, String> flinkCatalogProperties =
        CONVERTER.toFlinkCatalogProperties(catalogProperties);
    Assertions.assertEquals(
        GravitinoPaimonCatalogFactoryOptions.IDENTIFIER, flinkCatalogProperties.get("type"));
    Assertions.assertEquals(localWarehouse, flinkCatalogProperties.get(PaimonConstants.WAREHOUSE));
    Assertions.assertEquals(testUser, flinkCatalogProperties.get(PaimonConstants.PAIMON_JDBC_USER));
    Assertions.assertEquals(
        testPassword, flinkCatalogProperties.get(PaimonConstants.PAIMON_JDBC_PASSWORD));
    Assertions.assertEquals("jdbc", flinkCatalogProperties.get(PaimonConstants.METASTORE));
    Assertions.assertEquals(testUri, flinkCatalogProperties.get(PaimonConstants.URI));
  }

  @Test
  public void testToGravitinoCatalogProperties() {
    String testUser = "testUser";
    String testPassword = "testPassword";
    String testUri = "testUri";
    String testBackend = "jdbc";
    Configuration configuration =
        Configuration.fromMap(
            ImmutableMap.of(
                PaimonConstants.WAREHOUSE,
                localWarehouse,
                PaimonConstants.METASTORE,
                testBackend,
                PaimonConstants.PAIMON_JDBC_USER,
                testUser,
                PaimonConstants.PAIMON_JDBC_PASSWORD,
                testPassword,
                PaimonConstants.URI,
                testUri));
    Map<String, String> properties = CONVERTER.toGravitinoCatalogProperties(configuration);
    Assertions.assertEquals(localWarehouse, properties.get(PaimonConstants.WAREHOUSE));
    Assertions.assertEquals(testUser, properties.get(PaimonConstants.GRAVITINO_JDBC_USER));
    Assertions.assertEquals(testPassword, properties.get(PaimonConstants.GRAVITINO_JDBC_PASSWORD));
    Assertions.assertEquals(testUri, properties.get(PaimonConstants.URI));
    Assertions.assertEquals(testBackend, properties.get(PaimonConstants.CATALOG_BACKEND));
  }

  @Test
  public void testToGravitinoTablePropertiesStripesBucketProperties() {
    Map<String, String> flinkProperties = new HashMap<>();
    flinkProperties.put(PaimonConstants.BUCKET_KEY, "id");
    flinkProperties.put(PaimonConstants.BUCKET_NUM, "4");
    flinkProperties.put("some-other-key", "some-value");

    Map<String, String> result = CONVERTER.toGravitinoTableProperties(flinkProperties);

    Assertions.assertFalse(result.containsKey(PaimonConstants.BUCKET_KEY));
    Assertions.assertFalse(result.containsKey(PaimonConstants.BUCKET_NUM));
    Assertions.assertEquals("some-value", result.get("some-other-key"));
  }

  @Test
  public void testToGravitinoTablePropertiesWithoutBucketProperties() {
    Map<String, String> flinkProperties = ImmutableMap.of("some-key", "some-value");

    Map<String, String> result = CONVERTER.toGravitinoTableProperties(flinkProperties);

    Assertions.assertFalse(result.containsKey(PaimonConstants.BUCKET_KEY));
    Assertions.assertFalse(result.containsKey(PaimonConstants.BUCKET_NUM));
    Assertions.assertEquals("some-value", result.get("some-key"));
  }

  @Test
  public void testGetDistributionWithBlankBucketKey() {
    Map<String, String> options = ImmutableMap.of(PaimonConstants.BUCKET_KEY, "  ");
    Distribution distribution = GravitinoPaimonCatalog.getDistribution(options);
    Assertions.assertEquals(Distributions.NONE, distribution);
  }

  @Test
  public void testGetDistributionWithNullProperties() {
    Distribution distribution = GravitinoPaimonCatalog.getDistribution(null);
    Assertions.assertEquals(Distributions.NONE, distribution);
  }

  @Test
  public void testGetDistributionWithNoBucketKeyOrBucket() {
    Map<String, String> options = ImmutableMap.of();
    Distribution distribution = GravitinoPaimonCatalog.getDistribution(options);
    Assertions.assertEquals(Distributions.NONE, distribution);
  }

  @Test
  public void testGetDistributionWithBothBucketKeyAndBucket() {
    Map<String, String> options =
        ImmutableMap.of(PaimonConstants.BUCKET_KEY, "col_1,col_2", PaimonConstants.BUCKET_NUM, "4");
    Distribution distribution = GravitinoPaimonCatalog.getDistribution(options);
    Assertions.assertEquals(Strategy.HASH, distribution.strategy());
    Assertions.assertEquals(4, distribution.number());
    Assertions.assertEquals(2, distribution.expressions().length);
    Assertions.assertEquals(
        "col_1", ((NamedReference) distribution.expressions()[0]).fieldName()[0]);
    Assertions.assertEquals(
        "col_2", ((NamedReference) distribution.expressions()[1]).fieldName()[0]);
  }

  @Test
  public void testGetDistributionWithOnlyBucketKey() {
    Map<String, String> options = ImmutableMap.of(PaimonConstants.BUCKET_KEY, "col_a");
    Distribution distribution = GravitinoPaimonCatalog.getDistribution(options);
    Assertions.assertEquals(Strategy.HASH, distribution.strategy());
    Assertions.assertEquals(Distributions.AUTO, distribution.number());
    Assertions.assertEquals(1, distribution.expressions().length);
    Assertions.assertEquals(
        "col_a", ((NamedReference) distribution.expressions()[0]).fieldName()[0]);
  }

  @Test
  public void testGetDistributionWithOnlyBucket() {
    Map<String, String> options = ImmutableMap.of(PaimonConstants.BUCKET_NUM, "8");
    Distribution distribution = GravitinoPaimonCatalog.getDistribution(options);
    Assertions.assertEquals(Strategy.HASH, distribution.strategy());
    Assertions.assertEquals(8, distribution.number());
    Assertions.assertEquals(0, distribution.expressions().length);
  }

  @Test
  public void testGetDistributionWithOnlyBucketMinusOne() {
    Map<String, String> options = ImmutableMap.of(PaimonConstants.BUCKET_NUM, "-1");
    Distribution distribution = GravitinoPaimonCatalog.getDistribution(options);
    Assertions.assertEquals(Strategy.HASH, distribution.strategy());
    Assertions.assertEquals(Distributions.AUTO, distribution.number());
    Assertions.assertEquals(0, distribution.expressions().length);
  }

  @Test
  public void testGetDistributionWithBucketKeyAndExplicitMinusOne() {
    Map<String, String> options =
        ImmutableMap.of(PaimonConstants.BUCKET_KEY, "col_a", PaimonConstants.BUCKET_NUM, "-1");
    Distribution distribution = GravitinoPaimonCatalog.getDistribution(options);
    Assertions.assertEquals(Strategy.HASH, distribution.strategy());
    Assertions.assertEquals(Distributions.AUTO, distribution.number());
    Assertions.assertEquals(1, distribution.expressions().length);
    Assertions.assertEquals(
        "col_a", ((NamedReference) distribution.expressions()[0]).fieldName()[0]);
  }

  @Test
  public void testGetDistributionWithInvalidBucketNumber() {
    Map<String, String> options =
        ImmutableMap.of(
            PaimonConstants.BUCKET_KEY, "col_1", PaimonConstants.BUCKET_NUM, "not_a_number");
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> GravitinoPaimonCatalog.getDistribution(options));
    Assertions.assertTrue(
        exception.getMessage().contains("Paimon bucket number must be a valid integer"));
  }

  @Test
  public void testGetDistributionWithNegativeBucketNumberPassesThrough() {
    Map<String, String> options =
        ImmutableMap.of(PaimonConstants.BUCKET_KEY, "col_1", PaimonConstants.BUCKET_NUM, "-4");
    Distribution distribution = GravitinoPaimonCatalog.getDistribution(options);
    Assertions.assertEquals(Strategy.HASH, distribution.strategy());
    Assertions.assertEquals(-4, distribution.number());
    Assertions.assertEquals(1, distribution.expressions().length);
  }

  @Test
  public void testDistributionToPropertiesWithBucketAndBucketKey() {
    Distribution distribution = Distributions.hash(4, NamedReference.field("id"));
    Map<String, String> properties = GravitinoPaimonCatalog.distributionToProperties(distribution);
    Assertions.assertEquals("id", properties.get(PaimonConstants.BUCKET_KEY));
    Assertions.assertEquals("4", properties.get(PaimonConstants.BUCKET_NUM));
    Assertions.assertEquals(2, properties.size());
  }

  @Test
  public void testDistributionToPropertiesWithNoDistribution() {
    Map<String, String> properties =
        GravitinoPaimonCatalog.distributionToProperties(Distributions.NONE);
    Assertions.assertTrue(properties.isEmpty());
  }

  @Test
  public void testDistributionToPropertiesWithNullDistribution() {
    Map<String, String> properties = GravitinoPaimonCatalog.distributionToProperties(null);
    Assertions.assertTrue(properties.isEmpty());
  }

  @Test
  public void testDistributionToPropertiesWithAutoDistribution() {
    Distribution distribution = Distributions.auto(Strategy.HASH, NamedReference.field("col_a"));
    Map<String, String> properties = GravitinoPaimonCatalog.distributionToProperties(distribution);
    Assertions.assertEquals("col_a", properties.get(PaimonConstants.BUCKET_KEY));
    Assertions.assertEquals("-1", properties.get(PaimonConstants.BUCKET_NUM));
    Assertions.assertEquals(2, properties.size());
  }

  @Test
  public void testDistributionToPropertiesWithAutoNoExpressions() {
    // AUTO with no expressions is Paimon's default — nothing to output.
    Distribution distribution = Distributions.auto(Strategy.HASH);
    Map<String, String> properties = GravitinoPaimonCatalog.distributionToProperties(distribution);
    Assertions.assertTrue(properties.isEmpty());
  }

  @Test
  public void testDistributionRoundTripIdempotent() {
    Map<String, String> optionsBlank = ImmutableMap.of(PaimonConstants.BUCKET_KEY, "col_a");
    Map<String, String> optionsExplicit =
        ImmutableMap.of(PaimonConstants.BUCKET_KEY, "col_a", PaimonConstants.BUCKET_NUM, "-1");
    Map<String, String> fromBlank =
        GravitinoPaimonCatalog.distributionToProperties(
            GravitinoPaimonCatalog.getDistribution(optionsBlank));
    Map<String, String> fromExplicit =
        GravitinoPaimonCatalog.distributionToProperties(
            GravitinoPaimonCatalog.getDistribution(optionsExplicit));
    Assertions.assertEquals(fromBlank, fromExplicit);
  }
}
