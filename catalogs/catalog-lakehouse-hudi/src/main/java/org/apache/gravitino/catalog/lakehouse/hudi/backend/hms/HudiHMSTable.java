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

import org.apache.gravitino.catalog.lakehouse.hudi.HudiColumn;
import org.apache.gravitino.catalog.lakehouse.hudi.HudiTable;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;

public class HudiHMSTable extends HudiTable<Table> {
  public static Builder builder() {
    return new Builder();
  }

  private HudiHMSTable() {
    super();
  }

  @Override
  public Table fromHudiTable() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public static class Builder extends HudiTable.Builder<Table> {
    @Override
    protected HudiHMSTable simpleBuild() {
      HudiHMSTable table = new HudiHMSTable();
      table.name = name;
      table.comment = comment;
      table.columns = columns;
      table.indexes = indexes;
      table.partitioning = partitioning;
      table.sortOrders = sortOrders;
      table.distribution = distribution;
      table.properties = properties;
      table.auditInfo = auditInfo;
      return table;
    }

    @Override
    protected HudiHMSTable buildFromTable(Table hmsTable) {
      name = hmsTable.name();
      comment = hmsTable.comment();
      Column[] backendColumns = hmsTable.columns();
      HudiColumn[] hudiColumns = new HudiColumn[backendColumns.length];
      for (int i = 0; i < backendColumns.length; i++) {
        Column c = backendColumns[i];
        hudiColumns[i] =
            HudiColumn.builder()
                .withName(c.name())
                .withComment(c.comment())
                .withType(c.dataType())
                .withNullable(c.nullable())
                .withAutoIncrement(c.autoIncrement())
                .withDefaultValue(c.defaultValue())
                .build();
      }
      columns = hudiColumns;
      partitioning = hmsTable.partitioning();

      sortOrders = hmsTable.sortOrder();

      distribution = hmsTable.distribution();
      auditInfo = AuditInfo.builder().withCreator(hmsTable.auditInfo().creator()).build();

      properties = hmsTable.properties();
      return simpleBuild();
    }
  }
}
