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
import java.util.Map;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.mapper.provider.base.DomainMemberBaseSQLProvider;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;

/** Selects shared Domain membership statements for the configured database. */
public class DomainMemberSQLProviderFactory {
  private static final DomainMemberBaseSQLProvider BASE = new DomainMemberBaseSQLProvider();
  private static final Map<JDBCBackendType, DomainMemberBaseSQLProvider> PROVIDERS =
      ImmutableMap.of(
          JDBCBackendType.MYSQL, BASE, JDBCBackendType.H2, BASE, JDBCBackendType.POSTGRESQL, BASE);

  /** Delegates {@link DomainMemberBaseSQLProvider#hasMembers}. */
  public static String hasMembers() {
    return getProvider().hasMembers();
  }

  /** Delegates {@link DomainMemberBaseSQLProvider#domains}. */
  public static String domains() {
    return getProvider().domains();
  }

  /** Delegates {@link DomainMemberBaseSQLProvider#list}. */
  public static String list() {
    return getProvider().list();
  }

  /** Delegates {@link DomainMemberBaseSQLProvider#members}. */
  public static String members() {
    return getProvider().members();
  }

  /** Delegates {@link DomainMemberBaseSQLProvider#liveMembers}. */
  public static String liveMembers() {
    return getProvider().liveMembers();
  }

  /** Delegates {@link DomainMemberBaseSQLProvider#add}. */
  public static String add() {
    return getProvider().add();
  }

  /** Delegates {@link DomainMemberBaseSQLProvider#lastRetirement}. */
  public static String lastRetirement() {
    return getProvider().lastRetirement();
  }

  /** Delegates {@link DomainMemberBaseSQLProvider#retire}. */
  public static String retire() {
    return getProvider().retire();
  }

  /** Delegates {@link DomainMemberBaseSQLProvider#lastMetalakeRetirement}. */
  public static String lastMetalakeRetirement() {
    return getProvider().lastMetalakeRetirement();
  }

  /** Delegates {@link DomainMemberBaseSQLProvider#retireMember}. */
  public static String retireMember() {
    return getProvider().retireMember();
  }

  /** Delegates {@link DomainMemberBaseSQLProvider#retireMetalake}. */
  public static String retireMetalake() {
    return getProvider().retireMetalake();
  }

  /** Delegates {@link DomainMemberBaseSQLProvider#purge}. */
  public static String purge() {
    return getProvider().purge();
  }

  private static DomainMemberBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();
    return PROVIDERS.get(JDBCBackendType.fromString(databaseId));
  }
}
