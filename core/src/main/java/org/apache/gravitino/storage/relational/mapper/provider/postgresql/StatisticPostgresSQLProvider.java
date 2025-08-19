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
package org.apache.gravitino.storage.relational.mapper.provider.postgresql;

import static org.apache.gravitino.storage.relational.mapper.StatisticMetaMapper.STATISTIC_META_TABLE_NAME;

import java.util.List;
import org.apache.gravitino.storage.relational.mapper.provider.base.StatisticBaseSQLProvider;
import org.apache.gravitino.storage.relational.po.StatisticPO;

public class StatisticPostgresSQLProvider extends StatisticBaseSQLProvider {
  @Override
  protected String softDeleteSQL() {
    return " SET deleted_at = floor(extract(epoch from((current_timestamp -"
        + " timestamp '1970-01-01 00:00:00')*1000))) ";
  }

  @Override
  public String batchInsertStatisticPOsOnDuplicateKeyUpdate(List<StatisticPO> statisticPOs) {
    return "<script>"
        + "INSERT INTO "
        + STATISTIC_META_TABLE_NAME
        + " (statistic_id, statistic_name, statistic_value, metalake_id, metadata_object_id,"
        + " metadata_object_type, audit_info, current_version, last_version, deleted_at) VALUES "
        + "<foreach collection='statisticPOs' item='item' separator=','>"
        + "(#{item.statisticId}, "
        + "#{item.statisticName}, "
        + "#{item.statisticValue}, "
        + "#{item.metalakeId}, "
        + "#{item.metadataObjectId}, "
        + "#{item.metadataObjectType}, "
        + "#{item.auditInfo}, "
        + "#{item.currentVersion}, "
        + "#{item.lastVersion}, "
        + "#{item.deletedAt})"
        + "</foreach>"
        + " ON CONFLICT (statistic_name, metadata_object_id, deleted_at)"
        + " DO UPDATE SET "
        + "  statistic_value = EXCLUDED.statistic_value,"
        + "  audit_info = EXCLUDED.audit_info,"
        + "  current_version = EXCLUDED.current_version,"
        + "  last_version = EXCLUDED.last_version,"
        + "  deleted_at = EXCLUDED.deleted_at"
        + "</script>";
  }
}
