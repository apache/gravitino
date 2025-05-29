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

package org.apache.gravitino.storage.relational.mapper;

import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.storage.relational.JDBCBackend;
import org.apache.gravitino.storage.relational.mapper.provider.base.StatisticBaseSQLProvider;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;

import java.util.Map;

public class StatisticSQLProviderFactory {

    static class StatisticMySQLProvider extends StatisticBaseSQLProvider {};
    static class StatisticH2Provider extends StatisticBaseSQLProvider {};

    private static final Map<JDBCBackend.JDBCBackendType, StatisticBaseSQLProvider> STATISTIC_SQL_PROVIDERS =
            ImmutableMap.of(JDBCBackend.JDBCBackendType.H2, new StatisticH2Provider(), JDBCBackend.JDBCBackendType.MYSQL, new StatisticMySQLProvider());

    public static StatisticBaseSQLProvider getProvider() {
        String databaseId =
                SqlSessionFactoryHelper.getInstance()
                        .getSqlSessionFactory()
                        .getConfiguration()
                        .getDatabaseId();
        JDBCBackend.JDBCBackendType jdbcBackendType =
                JDBCBackend.JDBCBackendType.fromString(databaseId);
        return STATISTIC_SQL_PROVIDERS.get(jdbcBackendType);
    }
}
