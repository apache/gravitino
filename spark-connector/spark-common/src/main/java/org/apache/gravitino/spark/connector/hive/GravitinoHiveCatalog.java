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

package org.apache.gravitino.spark.connector.hive;

import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.credential.AzureAccountKeyCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialPropertyUtils;
import org.apache.gravitino.credential.OSSSecretKeyCredential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.gravitino.spark.connector.catalog.BaseCatalog;
import org.apache.kyuubi.spark.connector.hive.HiveTable;
import org.apache.kyuubi.spark.connector.hive.HiveTableCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GravitinoHiveCatalog extends BaseCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(GravitinoHiveCatalog.class);

  @Override
  protected TableCatalog createAndInitSparkCatalog(
      String name, CaseInsensitiveStringMap options, Map<String, String> properties) {
    TableCatalog hiveCatalog = new HiveTableCatalog();
    Map<String, String> all =
        getPropertiesConverter().toSparkCatalogProperties(options, properties);
    applyS3Credential(gravitinoCatalogClient, all);
    hiveCatalog.initialize(name, new CaseInsensitiveStringMap(all));
    return hiveCatalog;
  }

  static void applyS3Credential(Catalog catalog, Map<String, String> props) {
    for (Credential credential : CredentialPropertyUtils.getCredentials(catalog)) {
      if (credential instanceof S3SecretKeyCredential) {
        S3SecretKeyCredential s3 = (S3SecretKeyCredential) credential;
        props.put("hadoop.fs.s3a.access.key", s3.accessKeyId());
        props.put("hadoop.fs.s3a.secret.key", s3.secretAccessKey());
      } else if (credential instanceof OSSSecretKeyCredential) {
        OSSSecretKeyCredential oss = (OSSSecretKeyCredential) credential;
        props.put("hadoop.fs.oss.accessKeyId", oss.accessKeyId());
        props.put("hadoop.fs.oss.accessKeySecret", oss.secretAccessKey());
      } else if (credential instanceof AzureAccountKeyCredential) {
        AzureAccountKeyCredential azure = (AzureAccountKeyCredential) credential;
        props.put(
            String.format(
                "hadoop.fs.azure.account.key.%s.dfs.core.windows.net", azure.accountName()),
            azure.accountKey());
      } else {
        LOG.warn(
            "Received unrecognized credential type '{}' for Hive catalog, skipping",
            credential.getClass().getName());
      }
    }
  }

  @Override
  protected org.apache.spark.sql.connector.catalog.Table createSparkTable(
      Identifier identifier,
      Table gravitinoTable,
      org.apache.spark.sql.connector.catalog.Table sparkTable,
      TableCatalog sparkHiveCatalog,
      PropertiesConverter propertiesConverter,
      SparkTransformConverter sparkTransformConverter,
      SparkTypeConverter sparkTypeConverter) {
    return new SparkHiveTable(
        identifier,
        gravitinoTable,
        (HiveTable) sparkTable,
        (HiveTableCatalog) sparkHiveCatalog,
        propertiesConverter,
        sparkTransformConverter,
        sparkTypeConverter);
  }

  @Override
  protected PropertiesConverter getPropertiesConverter() {
    return HivePropertiesConverter.getInstance();
  }

  @Override
  protected SparkTransformConverter getSparkTransformConverter() {
    return new SparkTransformConverter(false);
  }

  @Override
  protected SparkTypeConverter getSparkTypeConverter() {
    return new SparkHiveTypeConverter();
  }

  @Override
  protected org.apache.spark.sql.connector.catalog.Table createSparkView(
      Identifier ident,
      View gravitinoView,
      org.apache.spark.sql.connector.catalog.Table sparkTable) {
    return new SparkHiveView(
        gravitinoView,
        (HiveTable) sparkTable,
        (HiveTableCatalog) sparkCatalog,
        getSparkTypeConverter());
  }
}
