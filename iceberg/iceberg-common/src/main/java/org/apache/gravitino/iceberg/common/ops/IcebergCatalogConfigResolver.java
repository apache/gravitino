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
package org.apache.gravitino.iceberg.common.ops;

import static org.apache.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialInfos;
import org.apache.gravitino.credential.SupportsCredentials;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.exceptions.RESTException;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.secret.SupportsSecrets;
import org.apache.gravitino.utils.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves lakehouse-iceberg catalog properties into an {@link IcebergConfig} suitable for {@link
 * IcebergCatalogWrapper#create(IcebergConfig)}.
 *
 * <p>Shared by {@link MetalakeCatalogWrapperManager} and the Iceberg REST dynamic config provider.
 */
public final class IcebergCatalogConfigResolver {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogConfigResolver.class);

  private IcebergCatalogConfigResolver() {}

  /**
   * Resolves plaintext catalog properties from a live {@link Catalog} (including secrets /
   * credentials when available).
   *
   * @param catalog catalog; must be {@code lakehouse-iceberg}
   * @return property map including optional {@link IcebergConstants#CATALOG_UUID}
   */
  public static Map<String, String> resolveProps(Catalog catalog) {
    Preconditions.checkArgument(
        "lakehouse-iceberg".equals(catalog.provider()),
        String.format("Catalog %s is not an Iceberg catalog", catalog.name()));

    // Auxiliary: BaseCatalog + SecretManager plaintext. Standalone: properties + getSecrets for
    // non-credential secrets, then static (expireTimeInMs == 0) getCredentials().credentialInfo()
    // for cloud/JDBC fields (expiring tokens are skipped; this config has no refresh path).
    if (catalog instanceof BaseCatalog) {
      BaseCatalog<?> baseCatalog = (BaseCatalog<?>) catalog;
      Map<String, String> props =
          new HashMap<>(
              GravitinoEnv.getInstance()
                  .secretManager()
                  .toPlaintextProperties(baseCatalog.propertiesWithCredentialProviders()));
      props.put(IcebergConstants.CATALOG_UUID, baseCatalog.entity().id().toString());
      return props;
    }
    Map<String, String> props =
        new HashMap<>(catalog.properties() == null ? Map.of() : catalog.properties());
    try {
      SupportsSecrets supportsSecrets = catalog.supportsSecrets();
      if (supportsSecrets != null) {
        Map<String, String> secrets = supportsSecrets.getSecrets();
        if (secrets != null) {
          props.putAll(secrets);
        }
      }
    } catch (UnsupportedOperationException | NotFoundException e) {
      LOG.debug(
          "Skipping getSecrets while resolving Iceberg catalog {}: {}",
          catalog.name(),
          e.toString());
    }
    try {
      SupportsCredentials supportsCredentials = catalog.supportsCredentials();
      if (supportsCredentials != null) {
        Credential[] credentials = supportsCredentials.getCredentials();
        props.putAll(CredentialInfos.nonExpiringCredentialInfo(credentials));
      }
    } catch (UnsupportedOperationException | NotFoundException e) {
      LOG.debug(
          "Skipping getCredentials while resolving Iceberg catalog {}: {}",
          catalog.name(),
          e.toString());
    } catch (RESTException e) {
      LOG.warn(
          "Failed to resolve getCredentials for Iceberg catalog {}; continuing without static"
              + " credential info: {}",
          catalog.name(),
          e.toString());
    }
    return props;
  }

  /**
   * Builds an {@link IcebergConfig} from catalog properties, promoting {@code gravitino.bypass.*}
   * keys into the Iceberg catalog property map.
   *
   * @param catalogProperties resolved catalog properties
   * @return Iceberg configuration
   */
  public static IcebergConfig toIcebergConfig(Map<String, String> catalogProperties) {
    Map<String, String> properties = new HashMap<>();
    properties.putAll(MapUtils.getPrefixMap(catalogProperties, CATALOG_BYPASS_PREFIX));
    properties.putAll(catalogProperties);
    return new IcebergConfig(properties);
  }

  /**
   * Resolves {@link IcebergConfig} from a live catalog.
   *
   * @param catalog catalog; must be {@code lakehouse-iceberg}
   * @return Iceberg configuration
   */
  public static IcebergConfig resolve(Catalog catalog) {
    return toIcebergConfig(resolveProps(catalog));
  }
}
