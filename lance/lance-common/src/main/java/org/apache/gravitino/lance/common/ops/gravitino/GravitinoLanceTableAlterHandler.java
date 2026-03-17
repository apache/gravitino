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

package org.apache.gravitino.lance.common.ops.gravitino;

import com.google.common.annotations.VisibleForTesting;
import com.lancedb.lance.namespace.model.AlterTableAlterColumnsRequest;
import com.lancedb.lance.namespace.model.AlterTableAlterColumnsResponse;
import com.lancedb.lance.namespace.model.AlterTableDropColumnsRequest;
import com.lancedb.lance.namespace.model.AlterTableDropColumnsResponse;
import com.lancedb.lance.namespace.model.ColumnAlteration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.lance.common.utils.LanceConstants;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;

/**
 * Handler for altering a Gravitino table. It builds the Gravitino table changes based on the
 * request and applies the changes to the table.
 *
 * @param <REQUEST> the type of the request to alter the table, such as add column, drop column,
 *     etc. the request will be converted to Gravitino TableChange and applied to the table
 * @param <RESPONSE> the type of the response after handling the alter table request.
 */
public interface GravitinoLanceTableAlterHandler<REQUEST, RESPONSE> {

  /**
   * Build the Gravitino table changes based on the alter table request.
   *
   * @param request the request to alter the table
   * @return the array of Gravitino TableChange to be applied to the table
   */
  TableChange[] buildGravitinoTableChange(REQUEST request);

  /**
   * Apply the Gravitino table changes to the table and return the response.
   *
   * @param gravitinoTable the Gravitino table to be altered
   * @param request the request to alter the table, it can be used to generate the response after
   *     applying the changes
   * @return the response after handling the alter table request, it can contain the details of the
   *     altered
   */
  RESPONSE handle(Table gravitinoTable, REQUEST request);

  @VisibleForTesting
  class DropColumns
      implements GravitinoLanceTableAlterHandler<
          AlterTableDropColumnsRequest, AlterTableDropColumnsResponse> {

    @Override
    public TableChange[] buildGravitinoTableChange(AlterTableDropColumnsRequest request) {
      return request.getColumns().stream()
          .map(colName -> TableChange.deleteColumn(new String[] {colName}, false))
          .toArray(TableChange[]::new);
    }

    @Override
    public AlterTableDropColumnsResponse handle(
        Table gravitinoTable, AlterTableDropColumnsRequest request) {
      AlterTableDropColumnsResponse response = new AlterTableDropColumnsResponse();
      Long version = extractTableVersion(gravitinoTable);
      if (version != null) {
        response.setVersion(version);
      }
      return response;
    }
  }

  private static Long extractTableVersion(Table gravitinoTable) {
    return Optional.ofNullable(gravitinoTable.properties().get(LanceConstants.LANCE_TABLE_VERSION))
        .map(Long::valueOf)
        .orElse(null);
  }

  @VisibleForTesting
  class AlterColumnsGravitinoLance
      implements GravitinoLanceTableAlterHandler<
          AlterTableAlterColumnsRequest, AlterTableAlterColumnsResponse> {

    @Override
    public TableChange[] buildGravitinoTableChange(AlterTableAlterColumnsRequest request) {
      return buildAlterColumnChanges(request);
    }

    @Override
    public AlterTableAlterColumnsResponse handle(
        Table gravitinoTable, AlterTableAlterColumnsRequest request) {
      AlterTableAlterColumnsResponse response = new AlterTableAlterColumnsResponse();
      Long version = extractTableVersion(gravitinoTable);
      if (version != null) {
        response.setVersion(version);
      }

      return response;
    }

    private TableChange[] buildAlterColumnChanges(AlterTableAlterColumnsRequest request) {
      List<ColumnAlteration> columns = request.getAlterations();

      List<TableChange> changes = new ArrayList<>();
      for (ColumnAlteration column : columns) {
        // Column name will not be null according to LanceDB spec.
        String columnName = column.getColumn();
        String newName = column.getRename();
        if (StringUtils.isNotBlank(newName)) {
          changes.add(TableChange.renameColumn(new String[] {columnName}, newName));
        }

        // The format of ColumnAlteration#castTo is unclear, so we will skip it now
        // for more, please refer to: https://shorturl.at/bYI0Z (short url for
        // github.com/lance-format/lance-namespace)
        if (StringUtils.isNotBlank(column.getCastTo())) {
          throw new UnsupportedOperationException(
              "Altering column data type is not supported yet.");
        }
      }
      return changes.stream().toArray(TableChange[]::new);
    }
  }
}
