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

import static org.apache.gravitino.catalog.lakehouse.hudi.HudiTablePropertiesMetadata.COMMENT;
import static org.apache.gravitino.catalog.lakehouse.hudi.HudiTablePropertiesMetadata.INPUT_FORMAT;
import static org.apache.gravitino.catalog.lakehouse.hudi.HudiTablePropertiesMetadata.LOCATION;
import static org.apache.gravitino.catalog.lakehouse.hudi.HudiTablePropertiesMetadata.OUTPUT_FORMAT;

import org.apache.gravitino.catalog.lakehouse.hudi.HudiColumn;
import org.apache.gravitino.catalog.lakehouse.hudi.HudiTable;
import org.apache.gravitino.hive.converter.HiveTableConverter;
import org.apache.hadoop.hive.metastore.api.Table;

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
      name = hmsTable.getTableName();
      comment = hmsTable.getParameters().get(COMMENT);
      columns = HiveTableConverter.getColumns(hmsTable, HudiColumn.builder());
      partitioning = HiveTableConverter.getPartitioning(hmsTable);

      // Should always be SortOrders.NONE since Hudi using clustering to sort data (see
      // https://hudi.apache.org/docs/next/clustering/)
      // but is run as a background table service
      sortOrders = HiveTableConverter.getSortOrders(hmsTable);

      // Should always be Distributions.NONE since Hudi doesn't support distribution
      distribution = HiveTableConverter.getDistribution(hmsTable);
      auditInfo = HiveTableConverter.getAuditInfo(hmsTable);

      properties = hmsTable.getParameters();
      properties.put(LOCATION, hmsTable.getSd().getLocation());
      properties.put(INPUT_FORMAT, hmsTable.getSd().getInputFormat());
      properties.put(OUTPUT_FORMAT, hmsTable.getSd().getOutputFormat());

      return simpleBuild();
    }
  }
}
