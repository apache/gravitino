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

package org.apache.gravitino.storage.relational.mapper.provider.postgresql;

import static org.apache.gravitino.storage.relational.mapper.TableVersionMapper.TABLE_NAME;

import org.apache.gravitino.storage.relational.mapper.provider.base.TableVersionBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.ibatis.annotations.Param;

public class TableVersionPostgreSQLProvider extends TableVersionBaseSQLProvider {

  public String insertTableVersionOnDuplicateKeyUpdate(@Param("tablePO") TablePO tablePO) {
    return "INSERT INTO "
        + TABLE_NAME
        + " (table_id, format, properties, partitioning,"
        + " distribution, sort_orders, indexes, comment,"
        + " version, deleted_at)"
        + " VALUES ("
        + " #{tablePO.tableId},"
        + " #{tablePO.format},"
        + " #{tablePO.properties},"
        + " #{tablePO.partitions},"
        + " #{tablePO.distribution},"
        + " #{tablePO.sortOrders},"
        + " #{tablePO.indexes},"
        + " #{tablePO.comment},"
        + " #{tablePO.currentVersion},"
        + " #{tablePO.deletedAt}"
        + " )"
        + " ON CONFLICT (table_id, version, deleted_at) DO UPDATE SET"
        + " format = #{tablePO.format},"
        + " properties = #{tablePO.properties},"
        + " partitioning = #{tablePO.partitions},"
        + " distribution = #{tablePO.distribution},"
        + " sort_orders = #{tablePO.sortOrders},"
        + " indexes = #{tablePO.indexes},"
        + " comment = #{tablePO.comment},"
        + " version = #{tablePO.currentVersion},"
        + " deleted_at = #{tablePO.deletedAt}";
  }

  @Override
  public String softDeleteTableVersionByTableIdAndVersion(Long tableId, Long version) {
    return "UPDATE "
        + TABLE_NAME
        + " SET deleted_at = round(extract(epoch from(current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')) * 1000)"
        + " WHERE table_id = #{tableId} AND version = #{version}";
  }
}
