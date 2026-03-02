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
package org.apache.gravitino.catalog.jdbc;

import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import org.apache.gravitino.catalog.jdbc.operation.JdbcTableOperations;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.JdbcCredential;

/** Implementation of an Jdbc catalog in Gravitino. */
public abstract class JdbcCatalog extends BaseCatalog<JdbcCatalog> {

  private static final JdbcCatalogPropertiesMetadata CATALOG_PROPERTIES_META =
      new JdbcCatalogPropertiesMetadata();

  private static final JdbcSchemaPropertiesMetadata SCHEMA_PROPERTIES_META =
      new JdbcSchemaPropertiesMetadata();

  private static final JdbcTablePropertiesMetadata TABLE_PROPERTIES_META =
      new JdbcTablePropertiesMetadata() {
        @Override
        protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
          return Collections.emptyMap();
        }
      };

  /**
   * Creates a new instance of {@link JdbcCatalogOperations} with the provided configuration.
   *
   * @param config The configuration map for the Jdbc catalog operations.
   * @return A new instance of {@link JdbcCatalogOperations}.
   */
  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    JdbcTypeConverter jdbcTypeConverter = createJdbcTypeConverter();
    JdbcCatalogOperations ops =
        new JdbcCatalogOperations(
            createExceptionConverter(),
            jdbcTypeConverter,
            createJdbcDatabaseOperations(),
            createJdbcTableOperations(),
            createJdbcColumnDefaultValueConverter());
    return ops;
  }

  @Override
  public Capability newCapability() {
    return new JdbcCatalogCapability();
  }

  /**
   * @return The {@link JdbcExceptionConverter} to be used by the catalog.
   */
  protected JdbcExceptionConverter createExceptionConverter() {
    return new JdbcExceptionConverter() {};
  }

  /**
   * @return The {@link JdbcTypeConverter} to be used by the catalog.
   */
  protected abstract JdbcTypeConverter createJdbcTypeConverter();

  /**
   * @return The {@link JdbcDatabaseOperations} to be used by the catalog to manage databases in the
   */
  protected abstract JdbcDatabaseOperations createJdbcDatabaseOperations();

  /**
   * @return The {@link JdbcTableOperations} to be used by the catalog to manage tables in the
   */
  protected abstract JdbcTableOperations createJdbcTableOperations();

  /**
   * @return The {@link JdbcColumnDefaultValueConverter} to be used by the catalog
   */
  protected abstract JdbcColumnDefaultValueConverter createJdbcColumnDefaultValueConverter();

  @Override
  public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
    return CATALOG_PROPERTIES_META;
  }

  @Override
  public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
    return SCHEMA_PROPERTIES_META;
  }

  @Override
  public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
    return TABLE_PROPERTIES_META;
  }

  @Override
  public Map<String, String> propertiesWithCredentialProviders() {
    Map<String, String> properties = Maps.newHashMap(super.propertiesWithCredentialProviders());
    return buildCredentialProvidersIfNecessary(properties);
  }

  private Map<String, String> buildCredentialProvidersIfNecessary(Map<String, String> properties) {
    // If credential providers already set, return as is
    if (StringUtils.isNotBlank(properties.get(CredentialConstants.CREDENTIAL_PROVIDERS))) {
      return properties;
    }

    // Add JDBC credential provider if jdbc-user and jdbc-password are set
    String jdbcUser = properties.get(JdbcConfig.USERNAME.getKey());
    String jdbcPassword = properties.get(JdbcConfig.PASSWORD.getKey());
    if (StringUtils.isNotBlank(jdbcUser) && StringUtils.isNotBlank(jdbcPassword)) {
      properties.put(CredentialConstants.CREDENTIAL_PROVIDERS, JdbcCredential.JDBC_CREDENTIAL_TYPE);
    }

    return properties;
  }
}
