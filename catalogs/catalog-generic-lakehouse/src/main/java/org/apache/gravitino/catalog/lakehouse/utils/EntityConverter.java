/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.catalog.lakehouse.utils;

import java.util.List;
import org.apache.gravitino.connector.GenericLakehouseColumn;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.rel.Column;

public class EntityConverter {
  public static Column[] toColumns(List<ColumnEntity> columnEntities) {
    return columnEntities.stream().map(EntityConverter::toColumn).toArray(Column[]::new);
  }

  private static Column toColumn(ColumnEntity columnEntity) {
    return GenericLakehouseColumn.builder()
        .withName(columnEntity.name())
        .withComment(columnEntity.comment())
        .withAutoIncrement(columnEntity.autoIncrement())
        .withNullable(columnEntity.nullable())
        .withType(columnEntity.dataType())
        .withDefaultValue(columnEntity.defaultValue())
        .build();
  }
}
