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

import avro.shaded.com.google.common.collect.Maps;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Schema;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.SchemaDTO;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalType;

public class HiveDatabaseConverter {
  public static Schema fromHiveDB(Database db) {
    Map<String, String> properties = buildSchemaProperties(db);

    // Get audit info from Hive's Database object. Because Hive's database doesn't store create
    // time, last modifier and last modified time, we only get creator from Hive's database.
    AuditDTO.Builder auditDtoBuilder = AuditDTO.builder();
    Optional.ofNullable(db.getOwnerName()).ifPresent(auditDtoBuilder::withCreator);

    return SchemaDTO.builder()
        .withName(db.getName())
        .withComment(db.getDescription())
        .withProperties(properties)
        .withAudit(auditDtoBuilder.build())
        .build();
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
    Map<String, String> properties = Maps.newHashMap(database.getParameters());
    properties.put(LOCATION, database.getLocationUri());
    return properties;
  }

  public static Database toHiveDb(Schema db) {
    Database hiveDb = new Database();

    hiveDb.setName(db.name());
    Optional.ofNullable(db.properties().get(LOCATION)).ifPresent(hiveDb::setLocationUri);
    Optional.ofNullable(db.comment()).ifPresent(hiveDb::setDescription);

    // TODO: Add more privilege info to Hive's Database object after Gravitino supports privilege.
    hiveDb.setOwnerName(db.auditInfo().creator());
    hiveDb.setOwnerType(PrincipalType.USER);

    Map<String, String> parameters = Maps.newHashMap(db.properties());
    parameters.remove(LOCATION);
    hiveDb.setParameters(parameters);

    return hiveDb;
  }
}
