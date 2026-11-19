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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.util.Preconditions;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.credential.ADLSTokenCredential;
import org.apache.gravitino.credential.AwsIrsaCredential;
import org.apache.gravitino.credential.COSTokenCredential;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.CredentialPropertyUtils;
import org.apache.gravitino.credential.GCSTokenCredential;
import org.apache.gravitino.credential.OSSTokenCredential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.gravitino.flink.connector.catalog.BaseCatalog;
import org.apache.gravitino.storage.GCSProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Gravitino Iceberg Catalog. */
public abstract class GravitinoIcebergCatalog extends BaseCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoIcebergCatalog.class);

  private static final Set<String> PATH_SCOPED_TOKEN_PROVIDERS =
      ImmutableSet.of(
          S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE,
          GCSTokenCredential.GCS_TOKEN_CREDENTIAL_TYPE,
          ADLSTokenCredential.ADLS_TOKEN_CREDENTIAL_TYPE,
          OSSTokenCredential.OSS_TOKEN_CREDENTIAL_TYPE,
          COSTokenCredential.COS_TOKEN_CREDENTIAL_TYPE);

  private final String icebergCatalogName;
  // Mutable copy so credential injection in open() propagates to the inner Iceberg catalog.
  private final Map<String, String> mutableIcebergProperties;
  private AbstractCatalog icebergCatalog;

  protected GravitinoIcebergCatalog(
      String catalogName,
      String defaultDatabase,
      SchemaAndTablePropertiesConverter schemaAndTablePropertiesConverter,
      PartitionConverter partitionConverter,
      Map<String, String> catalogOptions,
      Map<String, String> icebergCatalogProperties) {
    super(
        catalogName,
        catalogOptions,
        defaultDatabase,
        schemaAndTablePropertiesConverter,
        partitionConverter);
    this.icebergCatalogName = catalogName;
    this.mutableIcebergProperties = new HashMap<>(icebergCatalogProperties);
  }

  protected GravitinoIcebergCatalog(
      String catalogName,
      String defaultDatabase,
      SchemaAndTablePropertiesConverter schemaAndTablePropertiesConverter,
      PartitionConverter partitionConverter,
      Map<String, String> catalogOptions,
      AbstractCatalog icebergCatalog) {
    super(
        catalogName,
        catalogOptions,
        defaultDatabase,
        schemaAndTablePropertiesConverter,
        partitionConverter);
    this.icebergCatalogName = catalogName;
    this.mutableIcebergProperties = null;
    this.icebergCatalog = icebergCatalog;
  }

  @Override
  public void open() throws CatalogException {
    if (icebergCatalog == null) {
      try {
        Catalog gravitinoCatalog = catalog();
        List<String> tokenProviders =
            checkCredentialProviders(
                catalogName(), mutableIcebergProperties, gravitinoCatalog.properties());
        if (!tokenProviders.isEmpty()) {
          LOG.warn(
              "Catalog '{}' uses credential providers {}, which vend no credential at catalog"
                  + " level. The Flink connector injects no storage credential for them, so"
                  + " Iceberg FileIO uses the credentials available on the Flink nodes.",
              catalogName(),
              tokenProviders);
        }
        CredentialPropertyUtils.applyIcebergCredentials(
            CredentialPropertyUtils.getCredentials(gravitinoCatalog), mutableIcebergProperties);
      } catch (NoSuchCatalogException e) {
        LOG.warn(
            "Catalog '{}' not found in Gravitino during open(); credential injection skipped."
                + " This is expected during CREATE CATALOG.",
            catalogName(),
            e);
      }
      this.icebergCatalog =
          asAbstractCatalog(
              createInnerIcebergCatalog(icebergCatalogName, mutableIcebergProperties));
    }
    super.open();
  }

  /**
   * Creates the inner Iceberg {@code FlinkCatalogFactory}-backed catalog. Subclasses for different
   * Flink versions implement this because {@code FlinkCatalogFactory.createCatalog}'s signature has
   * changed across Flink major versions (e.g. Flink 2.x removed the public 2-arg {@code
   * createCatalog(String, Map)} overload used by Flink 1.x and made the 3-arg {@code
   * createCatalog(String, Map, Configuration)} overload protected, leaving only the public {@code
   * createCatalog(Context)} overload usable from outside the iceberg-flink package).
   *
   * @param catalogName the Iceberg catalog name
   * @param properties the Iceberg catalog properties
   * @return the created inner catalog
   */
  protected abstract Object createInnerIcebergCatalog(
      String catalogName, Map<String, String> properties);

  @Override
  public Optional<Factory> getFactory() {
    Preconditions.checkState(icebergCatalog != null, "Catalog '%s' has not been opened", getName());
    return icebergCatalog.getFactory();
  }

  @Override
  protected AbstractCatalog realCatalog() {
    Preconditions.checkState(icebergCatalog != null, "Catalog '%s' has not been opened", getName());
    return icebergCatalog;
  }

  protected static AbstractCatalog asAbstractCatalog(Object catalog) {
    Preconditions.checkState(
        catalog instanceof AbstractCatalog,
        "Expected AbstractCatalog from FlinkCatalogFactory but got %s.",
        catalog.getClass().getName());
    return (AbstractCatalog) catalog;
  }

  // Without per-table vended credentials, the inner catalog only receives the catalog-level
  // credentials fetched once in open(). A REST backend with vended credentials enabled is exempt
  // because the inner REST catalog obtains per-table, refreshable credentials itself. aws-irsa is
  // rejected: its credential is injected but never refreshed, so it overrides the node's own
  // credentials and then expires. Path-scoped token providers vend nothing at catalog level, which
  // leaves the node's own credentials in effect, so they are only returned for the caller to warn
  // about. The REST check uses the effective inner catalog-type, since an explicit catalog-type may
  // differ from the persisted catalog-backend. When no provider list is set, the server infers
  // gcs-token from gcs-service-account-file, which is not reflected in the catalog properties seen
  // by the client, so the same inference is applied here.
  @VisibleForTesting
  static List<String> checkCredentialProviders(
      String catalogName,
      Map<String, String> icebergCatalogProperties,
      @Nullable Map<String, String> gravitinoCatalogProperties) {
    boolean restWithVendedCredentials =
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_REST.equalsIgnoreCase(
                icebergCatalogProperties.get(IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE))
            && IcebergPropertiesConstants.ICEBERG_DATA_ACCESS_VENDED_CREDENTIALS.equalsIgnoreCase(
                icebergCatalogProperties.get(IcebergPropertiesConstants.ICEBERG_ACCESS_DELEGATION));
    if (restWithVendedCredentials || gravitinoCatalogProperties == null) {
      return Collections.emptyList();
    }
    String providers = gravitinoCatalogProperties.get(CredentialConstants.CREDENTIAL_PROVIDERS);
    if (StringUtils.isBlank(providers)) {
      return StringUtils.isNotBlank(
              gravitinoCatalogProperties.get(GCSProperties.GRAVITINO_GCS_SERVICE_ACCOUNT_FILE))
          ? Collections.singletonList(GCSTokenCredential.GCS_TOKEN_CREDENTIAL_TYPE)
          : Collections.emptyList();
    }
    List<String> providerList =
        Splitter.on(',').trimResults().omitEmptyStrings().splitToList(providers);
    if (providerList.stream()
        .anyMatch(AwsIrsaCredential.AWS_IRSA_CREDENTIAL_TYPE::equalsIgnoreCase)) {
      throw new CatalogException(
          String.format(
              "Catalog '%s' uses credential provider %s, which the Flink connector does not"
                  + " support for Iceberg catalogs with a non-REST backend: its credential is"
                  + " never refreshed and expires. Use a static provider such as %s, or use a"
                  + " REST backend with %s=%s.",
              catalogName,
              AwsIrsaCredential.AWS_IRSA_CREDENTIAL_TYPE,
              S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE,
              IcebergPropertiesConstants.GRAVITINO_ICEBERG_DATA_ACCESS,
              IcebergPropertiesConstants.ICEBERG_DATA_ACCESS_VENDED_CREDENTIALS));
    }
    return providerList.stream()
        .filter(provider -> PATH_SCOPED_TOKEN_PROVIDERS.contains(provider.toLowerCase(Locale.ROOT)))
        .collect(Collectors.toList());
  }
}
