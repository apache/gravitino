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

package org.apache.gravitino.flink.connector.iceberg;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.SupportsCredentials;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.gravitino.flink.connector.utils.CatalogCompat;
import org.junit.jupiter.api.Test;

class TestGravitinoIcebergCatalogOpen {

  private static final Map<String, String> HIVE_BACKEND_WITH_AWS_IRSA =
      ImmutableMap.of(
          IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND,
          "hive",
          CredentialConstants.CREDENTIAL_PROVIDERS,
          "aws-irsa");

  @Test
  void testOpenAllowsAwsIrsaWhenEffectiveCatalogTypeIsRestWithVendedCredentials() {
    AbstractCatalog innerCatalog = mock(AbstractCatalog.class);
    TestableIcebergCatalog catalog =
        new TestableIcebergCatalog(
            ImmutableMap.of(
                IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE,
                "rest",
                IcebergPropertiesConstants.ICEBERG_ACCESS_DELEGATION,
                IcebergPropertiesConstants.ICEBERG_DATA_ACCESS_VENDED_CREDENTIALS),
            mockGravitinoCatalog(HIVE_BACKEND_WITH_AWS_IRSA),
            innerCatalog);

    assertDoesNotThrow(catalog::open);
    verify(innerCatalog).open();
  }

  @Test
  void testOpenRejectsAwsIrsaWhenEffectiveCatalogTypeIsNotRest() {
    TestableIcebergCatalog catalog =
        new TestableIcebergCatalog(
            ImmutableMap.of(IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE, "hive"),
            mockGravitinoCatalog(HIVE_BACKEND_WITH_AWS_IRSA),
            mock(AbstractCatalog.class));

    assertThrows(CatalogException.class, catalog::open);
  }

  @Test
  void testOpenRejectsAwsIrsaWhenRestBackendHasNoVendedCredentials() {
    TestableIcebergCatalog catalog =
        new TestableIcebergCatalog(
            ImmutableMap.of(IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE, "rest"),
            mockGravitinoCatalog(HIVE_BACKEND_WITH_AWS_IRSA),
            mock(AbstractCatalog.class));

    assertThrows(CatalogException.class, catalog::open);
  }

  private static Catalog mockGravitinoCatalog(Map<String, String> properties) {
    Catalog gravitinoCatalog = mock(Catalog.class);
    SupportsCredentials supportsCredentials = mock(SupportsCredentials.class);
    when(gravitinoCatalog.properties()).thenReturn(properties);
    when(gravitinoCatalog.supportsCredentials()).thenReturn(supportsCredentials);
    when(supportsCredentials.getCredentials()).thenReturn(new Credential[0]);
    return gravitinoCatalog;
  }

  private static class TestableIcebergCatalog extends GravitinoIcebergCatalog {

    private final Catalog gravitinoCatalog;
    private final AbstractCatalog innerCatalog;

    TestableIcebergCatalog(
        Map<String, String> icebergProperties,
        Catalog gravitinoCatalog,
        AbstractCatalog innerCatalog) {
      super(
          "c1",
          "default",
          mock(SchemaAndTablePropertiesConverter.class),
          mock(PartitionConverter.class),
          icebergProperties,
          icebergProperties);
      this.gravitinoCatalog = gravitinoCatalog;
      this.innerCatalog = innerCatalog;
    }

    @Override
    protected Catalog catalog() {
      return gravitinoCatalog;
    }

    @Override
    protected Object createInnerIcebergCatalog(String catalogName, Map<String, String> properties) {
      return innerCatalog;
    }

    @Override
    protected CatalogCompat catalogCompat() {
      return mock(CatalogCompat.class);
    }
  }
}
