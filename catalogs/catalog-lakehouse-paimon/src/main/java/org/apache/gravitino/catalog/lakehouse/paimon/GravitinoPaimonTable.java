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
package org.apache.gravitino.catalog.lakehouse.paimon;

import static org.apache.gravitino.meta.AuditInfo.EMPTY;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.ToString;
import org.apache.gravitino.connector.BaseTable;
import org.apache.gravitino.connector.TableOperations;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;

/**
 * Implementation of {@link Table} that represents an Apache Paimon Table entity in the Paimon
 * table.
 */
@ToString
@Getter
public class GravitinoPaimonTable extends BaseTable {

  private GravitinoPaimonTable() {}

  @Override
  protected TableOperations newOps() {
    // TODO: Implement this interface when we have the Paimon table operations.
    throw new UnsupportedOperationException("PaimonTable does not support TableOperations.");
  }

  /**
   * Converts {@link GravitinoPaimonTable} instance to Paimon table.
   *
   * @return The converted Paimon table.
   */
  public Schema toPaimonTableSchema() {
    Schema.Builder builder = Schema.newBuilder().comment(comment).options(properties);
    if (partitioning != null) {
      builder.partitionKeys(
          Arrays.stream(partitioning)
              .map(
                  partition -> {
                    NamedReference[] references = partition.references();
                    Preconditions.checkArgument(
                        references.length == 1,
                        "Partitioning column must be single-column, like 'a'.");
                    return references[0].toString();
                  })
              .collect(Collectors.toList()));
    }
    for (int index = 0; index < columns.length; index++) {
      DataField dataField = GravitinoPaimonColumn.toPaimonColumn(index, columns[index]);
      builder.column(dataField.name(), dataField.type(), dataField.description());
    }
    return builder.build();
  }

  /**
   * Creates a new {@link GravitinoPaimonTable} instance from Paimon table.
   *
   * @param table The {@link Table} instance of Paimon table.
   * @return A new {@link GravitinoPaimonTable} instance.
   */
  public static GravitinoPaimonTable fromPaimonTable(Table table) {
    return builder()
        .withName(table.name())
        .withColumns(
            GravitinoPaimonColumn.fromPaimonRowType(table.rowType())
                .toArray(new GravitinoPaimonColumn[0]))
        .withPartitioning(toGravitinoPartitioning(table.partitionKeys()))
        .withComment(table.comment().orElse(null))
        .withProperties(table.options())
        .withAuditInfo(EMPTY)
        .build();
  }

  public static Transform[] toGravitinoPartitioning(List<String> partitionKeys) {
    return partitionKeys.stream().map(Transforms::identity).toArray(Transform[]::new);
  }

  /** A builder class for constructing {@link GravitinoPaimonTable} instance. */
  public static class Builder extends BaseTableBuilder<Builder, GravitinoPaimonTable> {

    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    /**
     * Internal method to build a {@link GravitinoPaimonTable} instance using the provided values.
     *
     * @return A new {@link GravitinoPaimonTable} instance with the configured values.
     */
    @Override
    protected GravitinoPaimonTable internalBuild() {
      GravitinoPaimonTable paimonTable = new GravitinoPaimonTable();
      paimonTable.name = name;
      paimonTable.comment = comment;
      paimonTable.columns = columns;
      paimonTable.partitioning = partitioning;
      paimonTable.properties = properties == null ? Maps.newHashMap() : Maps.newHashMap(properties);
      paimonTable.auditInfo = auditInfo;
      return paimonTable;
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
