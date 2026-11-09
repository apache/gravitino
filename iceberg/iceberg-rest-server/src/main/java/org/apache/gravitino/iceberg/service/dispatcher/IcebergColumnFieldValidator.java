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
package org.apache.gravitino.iceberg.service.dispatcher;

import static org.apache.gravitino.Entity.EntityType.COLUMN;

import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.rel.types.Types;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.NestedField;

final class IcebergColumnFieldValidator {

  static void validateSchema(Schema schema) {
    schema
        .columns()
        .forEach(
            column -> {
              ColumnEntity.NAME.validate(column.name(), COLUMN);
              ColumnEntity.COMMENT.validate(column.doc(), COLUMN);
            });
    TypeUtil.indexById(schema.asStruct())
        .values()
        .forEach(IcebergColumnFieldValidator::validateType);
  }

  /**
   * Refuses a column type that Iceberg accepts but Gravitino cannot represent, before the table is
   * created. Otherwise the table is created and then fails to import into Gravitino.
   */
  private static void validateType(NestedField field) {
    if (field.type() instanceof DecimalType) {
      DecimalType decimal = (DecimalType) field.type();
      try {
        Types.DecimalType.of(decimal.precision(), decimal.scale());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid type %s for column %s: %s", decimal, field.name(), e.getMessage()),
            e);
      }
    }
  }

  static void validateUpdate(UpdateTableRequest request) {
    request.updates().stream()
        .filter(MetadataUpdate.AddSchema.class::isInstance)
        .map(MetadataUpdate.AddSchema.class::cast)
        .map(MetadataUpdate.AddSchema::schema)
        .forEach(IcebergColumnFieldValidator::validateSchema);
  }

  private IcebergColumnFieldValidator() {}
}
