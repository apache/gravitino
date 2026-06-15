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

package org.apache.gravitino.spark.connector.glue;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.glue.GlueConstants;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialPropertyUtils;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.catalog.BaseCatalog;
import org.apache.gravitino.spark.connector.hive.SparkHiveTable;
import org.apache.gravitino.spark.connector.hive.SparkHiveTypeConverter;
import org.apache.gravitino.spark.connector.iceberg.SparkIcebergTable;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.kyuubi.spark.connector.hive.HiveTable;
import org.apache.kyuubi.spark.connector.hive.HiveTableCatalog;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gravitino Glue catalog implementation for Apache Spark.
 *
 * <p>This catalog handles mixed table types stored in AWS Glue Data Catalog:
 *
 * <ul>
 *   <li>Non-Iceberg tables (Hive, Delta, Parquet): routed to {@link HiveTableCatalog} backed by the
 *       AWS Glue Data Catalog Hive client ({@code AWSGlueDataCatalogHiveClientFactory})
 *   <li>Iceberg tables: routed to Iceberg's {@link SparkCatalog} (GlueCatalog) for I/O
 * </ul>
 *
 * <p>Table routing is based on the {@code table-format} property in Glue table parameters. Tables
 * with {@code table-format=ICEBERG} are delegated to the Iceberg backend.
 *
 * <p>{@link HiveTableCatalog} is configured with {@code hive.metastore.client.factory.class} set to
 * {@code AWSGlueDataCatalogHiveClientFactory}, which replaces the embedded Derby metastore with a
 * direct AWS Glue API connection. This means no local Derby sync is required and the catalog works
 * correctly across multiple concurrent Spark applications sharing the same Glue catalog.
 */
public class GravitinoGlueCatalog extends BaseCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoGlueCatalog.class);

  // Lazily initialized Iceberg GlueCatalog for Iceberg tables
  volatile SparkCatalog icebergGlueCatalog;

  // Store original config for Iceberg catalog initialization
  private String catalogName;
  private Map<String, String> catalogProperties;

  /**
   * AWS credentials obtained via Gravitino credential vending at init time. Keyed by Gravitino
   * catalog property names (e.g. "aws-access-key-id") so they can be merged directly into catalog
   * properties before being passed to {@link GluePropertiesConverter#toIcebergCatalogProperties}.
   * Null when credential vending returns no S3 credentials.
   */
  private Map<String, String> vendedAwsCredentials;

  /** Creates a new GravitinoGlueCatalog. */
  public GravitinoGlueCatalog() {}

  /**
   * Creates a new HiveTableCatalog instance. Override in tests to inject mock instances.
   *
   * @return a new HiveTableCatalog
   */
  protected HiveTableCatalog createHiveTableCatalog() {
    return new HiveTableCatalog();
  }

  @Override
  protected TableCatalog createAndInitSparkCatalog(
      String name, CaseInsensitiveStringMap options, Map<String, String> properties) {
    this.catalogName = name;
    this.catalogProperties = properties;

    TableCatalog hiveCatalog = createHiveTableCatalog();
    Map<String, String> all =
        getPropertiesConverter().toSparkCatalogProperties(options, properties);
    this.vendedAwsCredentials = applyS3Credential(gravitinoCatalogClient, all);
    hiveCatalog.initialize(name, new CaseInsensitiveStringMap(all));
    return hiveCatalog;
  }

  /**
   * Obtains S3 credentials via Gravitino credential vending and injects them into {@code props} as
   * {@code hadoop.fs.s3a.*} for the non-Iceberg (Hive) path's S3 data access. Returns the vended
   * credentials keyed by Gravitino catalog property names for reuse in the Iceberg path. Returns an
   * empty map if credential vending is unavailable.
   *
   * @return map of vended AWS credentials (Gravitino key names), or empty map if none vended
   */
  static Map<String, String> applyS3Credential(
      org.apache.gravitino.Catalog catalog, Map<String, String> props) {
    Map<String, String> vended = new HashMap<>();
    Credential[] credentials;
    try {
      credentials = CredentialPropertyUtils.getCredentials(catalog);
    } catch (RuntimeException e) {
      LOG.debug(
          "Failed to obtain credentials from Glue catalog, S3 credential injection skipped", e);
      return vended;
    }
    for (Credential credential : credentials) {
      if (credential instanceof S3SecretKeyCredential) {
        S3SecretKeyCredential s3 = (S3SecretKeyCredential) credential;
        props.put("hadoop.fs.s3a.access.key", s3.accessKeyId());
        props.put("hadoop.fs.s3a.secret.key", s3.secretAccessKey());
        vended.put(GluePropertiesConverter.AWS_ACCESS_KEY_ID, s3.accessKeyId());
        vended.put(GluePropertiesConverter.AWS_SECRET_ACCESS_KEY, s3.secretAccessKey());
      } else {
        LOG.warn(
            "Received unrecognized credential type '{}' for Glue catalog, skipping",
            credential.getClass().getName());
      }
    }
    return vended;
  }

  /**
   * Routes Spark table loading to the correct backend.
   *
   * <p>Iceberg tables are loaded from the Iceberg GlueCatalog. Non-Iceberg tables are loaded
   * directly from {@link HiveTableCatalog}, which reads from Glue via the factory client.
   */
  @Override
  protected Table loadSparkTable(Identifier ident) {
    try {
      org.apache.gravitino.rel.Table gravitinoTable = loadGravitinoTable(ident);
      if (isIcebergTable(gravitinoTable)) {
        return getOrCreateIcebergGlueCatalog().loadTable(ident);
      }
      return sparkCatalog.loadTable(ident);
    } catch (NoSuchTableException e) {
      throw new RuntimeException(
          String.format("Failed to load spark table: %s.%s", getDatabase(ident), ident.name()), e);
    }
  }

  /**
   * Routes table creation to the appropriate Spark wrapper based on the Gravitino table type.
   * Iceberg tables are wrapped in {@link SparkIcebergTable}; all others in {@link SparkHiveTable}.
   */
  @Override
  protected Table createSparkTable(
      Identifier identifier,
      org.apache.gravitino.rel.Table gravitinoTable,
      Table sparkTable,
      TableCatalog sparkHiveCatalog,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter,
      SparkTypeConverter sparkTypeConverter) {

    if (isIcebergTable(gravitinoTable)) {
      Preconditions.checkArgument(
          sparkTable instanceof SparkTable,
          "Iceberg table %s expected SparkTable from Iceberg backend, got %s",
          identifier,
          sparkTable.getClass().getName());
      return new SparkIcebergTable(
          identifier,
          gravitinoTable,
          (SparkTable) sparkTable,
          getOrCreateIcebergGlueCatalog(),
          propertiesConverter,
          sparkTransformConverter,
          sparkTypeConverter);
    }

    return new SparkHiveTable(
        identifier,
        gravitinoTable,
        (HiveTable) sparkTable,
        (HiveTableCatalog) sparkHiveCatalog,
        propertiesConverter,
        sparkTransformConverter,
        sparkTypeConverter);
  }

  /** {@inheritDoc} Returns the Glue-specific properties converter singleton. */
  @Override
  protected PropertiesConverter getPropertiesConverter() {
    return GluePropertiesConverter.getInstance();
  }

  /** {@inheritDoc} Returns a transform converter with identity partition support disabled. */
  @Override
  protected SparkTransformConverter getSparkTransformConverter() {
    return new SparkTransformConverter(false);
  }

  /** {@inheritDoc} Returns the Hive-compatible type converter used for Glue tables. */
  @Override
  protected SparkTypeConverter getSparkTypeConverter() {
    return new SparkHiveTypeConverter();
  }

  /**
   * Delegates function lookup to the Iceberg {@link SparkCatalog}, which registers Iceberg built-in
   * partition functions ({@code days}, {@code hours}, {@code months}, {@code years}, etc.). Falls
   * back to the Gravitino function registry for non-Iceberg functions.
   *
   * <p>Without this override, {@link org.apache.gravitino.spark.connector.catalog.BaseCatalog}
   * would only query the Gravitino server's function registry, which does not include Iceberg
   * built-ins. This causes {@code INSERT} into Iceberg tables partitioned by time-based transforms
   * to fail with {@code AnalysisException: days(col) is not currently supported}.
   */
  @Override
  public UnboundFunction loadFunction(Identifier ident) throws NoSuchFunctionException {
    try {
      return getOrCreateIcebergGlueCatalog().loadFunction(ident);
    } catch (NoSuchFunctionException e) {
      return super.loadFunction(ident);
    }
  }

  /**
   * Invalidates both the Hive backend and the Iceberg backend caches for the given table.
   *
   * <p>{@link BaseCatalog} only calls {@code sparkCatalog.invalidateTable}, which clears the {@link
   * HiveTableCatalog} cache. The Iceberg {@link SparkCatalog} maintains its own {@code
   * CachingCatalog} that must be invalidated separately after any table mutation (ALTER, DROP,
   * PURGE, RENAME) to avoid stale schema errors on subsequent reads.
   */
  @Override
  public void invalidateTable(Identifier ident) {
    super.invalidateTable(ident);
    if (icebergGlueCatalog != null) {
      icebergGlueCatalog.invalidateTable(ident);
    }
  }

  /**
   * Returns true if the Gravitino table is an Iceberg-format table based on its properties.
   *
   * @param gravitinoTable the Gravitino table to inspect
   * @return true for Iceberg tables, false otherwise
   */
  static boolean isIcebergTable(org.apache.gravitino.rel.Table gravitinoTable) {
    Map<String, String> properties = gravitinoTable.properties();
    if (properties == null) {
      return false;
    }
    // Gravitino convention: table-format=ICEBERG
    String tableFormat = properties.get(GlueConstants.TABLE_FORMAT);
    if (GlueConstants.TABLE_FORMAT_ICEBERG.equalsIgnoreCase(tableFormat)) {
      return true;
    }
    // Iceberg Glue catalog convention: table_type=ICEBERG stored in Glue table parameters
    return GlueConstants.ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(
        properties.get(GlueConstants.TABLE_TYPE_PARAM));
  }

  /**
   * Gets or creates the Iceberg GlueCatalog using double-checked locking.
   *
   * @return the Iceberg SparkCatalog
   */
  private SparkCatalog getOrCreateIcebergGlueCatalog() {
    if (icebergGlueCatalog == null) {
      synchronized (this) {
        if (icebergGlueCatalog == null) {
          Preconditions.checkArgument(
              catalogName != null && catalogProperties != null,
              "Catalog name and properties must be set before accessing Iceberg catalog");
          try {
            icebergGlueCatalog = createIcebergGlueCatalog();
          } catch (Exception e) {
            throw new RuntimeException(
                String.format(
                    "Failed to initialize Iceberg GlueCatalog for catalog '%s'. "
                        + "Check aws-region, aws-access-key-id, and aws-secret-access-key properties.",
                    catalogName),
                e);
          }
        }
      }
    }
    return icebergGlueCatalog;
  }

  /**
   * Creates a new Iceberg GlueCatalog with appropriate configuration.
   *
   * @return the configured Iceberg SparkCatalog
   */
  private SparkCatalog createIcebergGlueCatalog() {
    GluePropertiesConverter converter = GluePropertiesConverter.getInstance();
    Map<String, String> effectiveProperties = new HashMap<>(catalogProperties);
    // Vended credentials take precedence over static catalog properties so that
    // GluePropertiesConverter sets up GravitinoGlueCredentialsProvider with the vended AK/SK.
    if (vendedAwsCredentials != null && !vendedAwsCredentials.isEmpty()) {
      effectiveProperties.putAll(vendedAwsCredentials);
    }
    Map<String, String> icebergProperties =
        converter.toIcebergCatalogProperties(effectiveProperties);
    SparkCatalog catalog = new SparkCatalog();
    catalog.initialize(catalogName + "_iceberg", new CaseInsensitiveStringMap(icebergProperties));
    return catalog;
  }
}
