package org.apache.gravitino.catalog.lakehouse.paimon.integration.test;

import static org.apache.gravitino.catalog.lakehouse.paimon.authentication.AuthenticationConfig.AUTH_TYPE_KEY;
import static org.apache.gravitino.catalog.lakehouse.paimon.authentication.AuthenticationConfig.IMPERSONATION_ENABLE_KEY;
import static org.apache.gravitino.catalog.lakehouse.paimon.authentication.kerberos.KerberosConfig.KEY_TAB_URI_KEY;
import static org.apache.gravitino.catalog.lakehouse.paimon.authentication.kerberos.KerberosConfig.PRINCIPAL_KEY;
import static org.apache.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalogPropertiesMetadata;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.client.KerberosTokenProvider;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CatalogPaimonKerberosHiveIT extends CatalogPaimonKerberosBaseIT {

  @Override
  protected void initCatalogProperties() {
    catalogProperties = new HashMap<>();
    String type = "filesystem";
    String warehouse =
        String.format(
            "hdfs://%s:%d/user/hive/paimon_catalog_warehouse/",
            kerberosHiveContainer.getContainerIpAddress(), HiveContainer.HDFS_DEFAULTFS_PORT);
    String uri =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);
    catalogProperties.put(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND, type);
    catalogProperties.put(PaimonCatalogPropertiesMetadata.WAREHOUSE, warehouse);
    catalogProperties.put(PaimonCatalogPropertiesMetadata.URI, uri);
  }

  @Test
  void testIcebergWithKerberosAndUserImpersonation() throws IOException {
    KerberosTokenProvider provider =
        KerberosTokenProvider.builder()
            .withClientPrincipal(GRAVITINO_CLIENT_PRINCIPAL)
            .withKeyTabFile(new File(TMP_DIR + GRAVITINO_CLIENT_KEYTAB))
            .build();
    adminClient = GravitinoAdminClient.builder(serverUri).withKerberosAuth(provider).build();

    GravitinoMetalake gravitinoMetalake =
        adminClient.createMetalake(METALAKE_NAME, null, ImmutableMap.of());

    // Create a catalog
    Map<String, String> properties = Maps.newHashMap();
    properties.put(IMPERSONATION_ENABLE_KEY, "true");
    properties.put(AUTH_TYPE_KEY, "kerberos");

    properties.put(KEY_TAB_URI_KEY, TMP_DIR + HIVE_METASTORE_CLIENT_KEYTAB);
    properties.put(PRINCIPAL_KEY, HIVE_METASTORE_CLIENT_PRINCIPAL);
    properties.put(
        CATALOG_BYPASS_PREFIX + "hive.metastore.kerberos.principal",
        "hive/_HOST@HADOOPKRB"
            .replace("_HOST", containerSuite.getKerberosHiveContainer().getHostName()));
    properties.put(CATALOG_BYPASS_PREFIX + "hive.metastore.sasl.enabled", "true");
    properties.putAll(catalogProperties);
    properties.put("location", "hdfs://localhost:9000/user/hive/warehouse-catalog-iceberg");

    Catalog catalog =
        gravitinoMetalake.createCatalog(
            CATALOG_NAME, Catalog.Type.RELATIONAL, "lakehouse-iceberg", "comment", properties);

    // Test create schema
    Exception exception =
        Assertions.assertThrows(
            Exception.class,
            () -> catalog.asSchemas().createSchema(SCHEMA_NAME, "comment", ImmutableMap.of()));
    String exceptionMessage = Throwables.getStackTraceAsString(exception);

    // Make sure the real user is 'gravitino_client'
    Assertions.assertTrue(
        exceptionMessage.contains("Permission denied: user=gravitino_client, access=WRITE"));

    // Now try to permit the user to create the schema again
    kerberosHiveContainer.executeInContainer(
        "hadoop", "fs", "-mkdir", "/user/hive/warehouse-catalog-iceberg");
    kerberosHiveContainer.executeInContainer(
        "hadoop", "fs", "-chmod", "-R", "777", "/user/hive/warehouse-catalog-iceberg");
    Assertions.assertDoesNotThrow(
        () -> catalog.asSchemas().createSchema(SCHEMA_NAME, "comment", ImmutableMap.of()));

    // Create table
    NameIdentifier tableNameIdentifier = NameIdentifier.of(SCHEMA_NAME, TABLE_NAME);
    catalog
        .asTableCatalog()
        .createTable(
            tableNameIdentifier,
            createColumns(),
            "",
            ImmutableMap.of(),
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            SortOrders.NONE);

    // Now try to alter the table
    catalog.asTableCatalog().alterTable(tableNameIdentifier, TableChange.rename("new_table"));
    NameIdentifier newTableIdentifier = NameIdentifier.of(SCHEMA_NAME, "new_table");

    // Old table name should not exist
    Assertions.assertFalse(catalog.asTableCatalog().tableExists(tableNameIdentifier));
    Assertions.assertTrue(catalog.asTableCatalog().tableExists(newTableIdentifier));

    // Drop table
    catalog.asTableCatalog().dropTable(newTableIdentifier);
    Assertions.assertFalse(catalog.asTableCatalog().tableExists(newTableIdentifier));

    // Drop schema
    catalog.asSchemas().dropSchema(SCHEMA_NAME, false);
    Assertions.assertFalse(catalog.asSchemas().schemaExists(SCHEMA_NAME));

    // Drop catalog
    Assertions.assertTrue(gravitinoMetalake.dropCatalog(CATALOG_NAME));
  }
}
