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
package org.apache.gravitino.hive.converter;

import static org.apache.gravitino.catalog.hive.HiveConstants.LOCATION;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.hive.HiveSchema;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalType;

public class HiveDatabaseConverter {
  public static HiveSchema fromHiveDB(Database db) {
    Preconditions.checkArgument(db != null, "Database cannot be null");

    Map<String, String> properties = buildSchemaProperties(db);

    // Get audit info from Hive's Database object. Because Hive's database doesn't store create
    // time, last modifier and last modified time, we only get creator from Hive's database.
    AuditInfo.Builder auditInfoBuilder = AuditInfo.builder();
    Optional.ofNullable(db.getOwnerName()).ifPresent(auditInfoBuilder::withCreator);

    String catalogName = null;
    try {
      java.lang.reflect.Method getCatalogNameMethod = db.getClass().getMethod("getCatalogName");
      catalogName = (String) getCatalogNameMethod.invoke(db);
    } catch (Exception e) {
      // Hive2 doesn't have getCatalogName method, catalogName will be null
    }

    HiveSchema hiveSchema =
        HiveSchema.builder()
            .withName(db.getName())
            .withComment(db.getDescription())
            .withProperties(properties)
            .withAuditInfo(auditInfoBuilder.build())
            .withCatalogName(catalogName)
            .build();
    return hiveSchema;
  }
  /**
   * Add a comment on lines L57 to L65Add diff commentMarkdown input: edit mode
   * selected.WritePreviewHeadingBoldItalicQuoteCodeLinkUnordered listNumbered listTask
   * listMentionReferenceSaved repliesAdd FilesPaste, drop, or click to add filesCancelCommentStart
   * a reviewReturn to code Build schema properties from a Hive Database object.
   *
   * @param database The Hive Database object.
   * @return A map of schema properties.
   */
  public static Map<String, String> buildSchemaProperties(Database database) {
    Map<String, String> properties = new HashMap<>(database.getParameters());
    properties.put(LOCATION, database.getLocationUri());
    return properties;
  }

  public static Database toHiveDb(HiveSchema hiveSchema) {
    Preconditions.checkArgument(hiveSchema != null, "HiveSchema cannot be null");
    Database hiveDb = new Database();

    hiveDb.setName(hiveSchema.name());
    Optional.ofNullable(hiveSchema.properties().get(LOCATION)).ifPresent(hiveDb::setLocationUri);
    Optional.ofNullable(hiveSchema.comment()).ifPresent(hiveDb::setDescription);

    // TODO: Add more privilege info to Hive's Database object after Gravitino supports privilege.
    hiveDb.setOwnerName(hiveSchema.auditInfo().creator());
    hiveDb.setOwnerType(PrincipalType.USER);

    Map<String, String> parameters = new HashMap<>(hiveSchema.properties());
    parameters.remove(LOCATION);
    hiveDb.setParameters(parameters);

    return hiveDb;
  }
}
