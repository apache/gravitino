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
package org.apache.gravitino.catalog.hive;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Optional;
import lombok.ToString;
import org.apache.gravitino.connector.BaseSchema;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalType;

/** Represents an Apache Hive Schema (Database) entity in the Hive Metastore catalog. */
@ToString
public class HiveSchema extends BaseSchema {
  private HiveSchema() {}

  /**
   * Creates a new HiveSchema instance from a Database and a Builder.
   *
   * @param db The Database representing the HiveSchema.
   * @return A new HiveSchema instance.
   */
  public static HiveSchema fromHiveDB(Database db) {
    Map<String, String> properties = buildSchemaProperties(db);

    // Get audit info from Hive's Database object. Because Hive's database doesn't store create
    // time, last modifier and last modified time, we only get creator from Hive's database.
    AuditInfo.Builder auditInfoBuilder = AuditInfo.builder();
    Optional.ofNullable(db.getOwnerName()).ifPresent(auditInfoBuilder::withCreator);

    return HiveSchema.builder()
        .withName(db.getName())
        .withComment(db.getDescription())
        .withProperties(properties)
        .withAuditInfo(auditInfoBuilder.build())
        .build();
  }

  /**
   * Build schema properties from a Hive Database object.
   *
   * @param database The Hive Database object.
   * @return A map of schema properties.
   */
  public static Map<String, String> buildSchemaProperties(Database database) {
    Map<String, String> properties = Maps.newHashMap(database.getParameters());
    properties.put(HiveSchemaPropertiesMetadata.LOCATION, database.getLocationUri());
    return properties;
  }

  /**
   * Converts this HiveSchema to its corresponding Database.
   *
   * @return The converted Database object.
   */
  public Database toHiveDB() {
    Database hiveDb = new Database();

    hiveDb.setName(name());
    Optional.ofNullable(properties().get(HiveSchemaPropertiesMetadata.LOCATION))
        .ifPresent(hiveDb::setLocationUri);
    Optional.ofNullable(comment()).ifPresent(hiveDb::setDescription);

    // TODO: Add more privilege info to Hive's Database object after Gravitino supports privilege.
    hiveDb.setOwnerName(auditInfo().creator());
    hiveDb.setOwnerType(PrincipalType.USER);

    Map<String, String> parameters = Maps.newHashMap(properties());
    parameters.remove(HiveSchemaPropertiesMetadata.LOCATION);
    hiveDb.setParameters(parameters);

    return hiveDb;
  }

  /** A builder class for constructing HiveSchema instances. */
  public static class Builder extends BaseSchemaBuilder<Builder, HiveSchema> {

    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    /**
     * Internal method to build a HiveSchema instance using the provided values.
     *
     * @return A new HiveSchema instance with the configured values.
     */
    @Override
    protected HiveSchema internalBuild() {
      HiveSchema hiveSchema = new HiveSchema();
      hiveSchema.name = name;
      hiveSchema.comment = comment;
      hiveSchema.properties = properties;
      hiveSchema.auditInfo = auditInfo;

      return hiveSchema;
    }
  }
  /**
   * Creates a new instance of {@link Builder}.
   *
   * @return The new instance.
   */
  public static Builder builder() {
    return new Builder();
  }
}
