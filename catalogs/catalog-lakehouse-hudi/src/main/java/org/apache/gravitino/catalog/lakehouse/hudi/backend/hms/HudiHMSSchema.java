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
package org.apache.gravitino.catalog.lakehouse.hudi.backend.hms;

import static org.apache.gravitino.catalog.lakehouse.hudi.HudiSchemaPropertiesMetadata.LOCATION;

import com.google.common.collect.Maps;
import java.util.Optional;
import org.apache.gravitino.catalog.lakehouse.hudi.HudiSchema;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.hadoop.hive.metastore.api.Database;

public class HudiHMSSchema extends HudiSchema<Database> {

  public static Builder builder() {
    return new Builder();
  }

  private HudiHMSSchema() {
    super();
  }

  @Override
  public Database fromHudiSchema() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public static class Builder extends HudiSchema.Builder<Database> {

    @Override
    protected HudiHMSSchema simpleBuild() {
      HudiHMSSchema schema = new HudiHMSSchema();
      schema.name = name;
      schema.comment = comment;
      schema.properties = properties;
      schema.auditInfo = auditInfo;
      return schema;
    }

    @Override
    protected HudiHMSSchema buildFromSchema(Database database) {
      name = database.getName();
      comment = database.getDescription();

      properties = Maps.newHashMap(database.getParameters());
      properties.put(LOCATION, database.getLocationUri());

      AuditInfo.Builder auditInfoBuilder = AuditInfo.builder();
      Optional.ofNullable(database.getOwnerName()).ifPresent(auditInfoBuilder::withCreator);
      auditInfo = auditInfoBuilder.build();

      return simpleBuild();
    }
  }
}
