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

package org.apache.gravitino.connector;

import org.apache.gravitino.rel.GenericTable;

public class GenericLakehouseTable extends BaseTable implements GenericTable {
  @SuppressWarnings("unused")
  private String schemaName;

  private String format;

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String format() {
    return format;
  }

  @Override
  public String location() {
    return properties.get("location");
  }

  @Override
  public boolean external() {
    return properties.get("external") != null && Boolean.parseBoolean(properties.get("external"));
  }

  @Override
  protected TableOperations newOps() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public static class Builder extends BaseTableBuilder<Builder, GenericLakehouseTable> {

    private String schemaName;
    private String format;

    public Builder withSchemaName(String schemaName) {
      this.schemaName = schemaName;
      return this;
    }

    public Builder withFormat(String format) {
      this.format = format;
      return this;
    }

    @Override
    protected GenericLakehouseTable internalBuild() {
      GenericLakehouseTable genericLakehouseTable = new GenericLakehouseTable();
      genericLakehouseTable.schemaName = this.schemaName;
      genericLakehouseTable.format = this.format;
      genericLakehouseTable.columns = this.columns;
      genericLakehouseTable.comment = this.comment;
      genericLakehouseTable.properties = this.properties;
      genericLakehouseTable.auditInfo = this.auditInfo;
      genericLakehouseTable.distribution = this.distribution;
      genericLakehouseTable.indexes = this.indexes;
      genericLakehouseTable.name = this.name;
      genericLakehouseTable.partitioning = this.partitioning;
      genericLakehouseTable.sortOrders = this.sortOrders;
      return genericLakehouseTable;
    }
  }
}
