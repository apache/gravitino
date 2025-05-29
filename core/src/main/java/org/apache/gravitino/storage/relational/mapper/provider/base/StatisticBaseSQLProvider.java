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
package org.apache.gravitino.storage.relational.mapper.provider.base;

import org.apache.gravitino.storage.relational.po.StatisticPO;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public class StatisticBaseSQLProvider {

    public String batchInsertStatistics(@Param("statistics") List<StatisticPO> statistics) {
        return "<script>"
                + "INSERT INTO statistics_meta (statistic_id, statistic_name, object_id, object_type, audit_info, current_version, last_version, deleted_at) VALUES "
                + "<foreach collection='statistics' item='item' separator=','>"
                + "(#{item.statisticId}, "
                + "#{item.statisticName}, "
                + "#{item.objectId}, "
                + "#{item.objectType}, "
                + "#{item.auditInfo}, "
                + "#{item.currentVersion}, "
                + "#{item.lastVersion}, "
                + "#{item.deletedAt})"
                + "</foreach>"
                + "</script>";
    }

    public String batchUpdateStatistics() {
        return "";
    }

    public String batchDeleteStatistics() {
        return "";
    }

    public String softDeleteStatisticsByObjectId(@Param("objectId") Long objectId) {
        return "UPDATE statistics_meta "
                + "SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
                + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
                + " WHERE object_id = #{objectId} AND deleted_at = 0";
    }

    public String listStatisticsByObjectId(@Param("objectId") Long objectId) {
        return "SELECT statistic_id as statisticId, statistic_name as statisticName, object_id as objectId, object_type as objectType, audit_info as auditInfo, current_version as currentVersion, last_version as lastVersion, deleted_at as deletedAt FROM statistics_meta "
                + "WHERE object_id = #{objectId} AND deleted_at = 0";
    }
}
