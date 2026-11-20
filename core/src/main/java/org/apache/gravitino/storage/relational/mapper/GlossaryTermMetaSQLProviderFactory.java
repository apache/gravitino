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
import org.apache.gravitino.storage.relational.mapper.provider.base.GlossaryTermMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;

/** Selects SQL for glossary definitions and synonyms on the configured backend. */
public class GlossaryTermMetaSQLProviderFactory {
  private static final Map<JDBCBackendType, GlossaryTermMetaBaseSQLProvider> PROVIDERS =
      ImmutableMap.of(
          JDBCBackendType.MYSQL, new GlossaryTermMetaMySQLProvider(),
          JDBCBackendType.H2, new GlossaryTermMetaBaseSQLProvider(),
          JDBCBackendType.POSTGRESQL, new GlossaryTermMetaBaseSQLProvider());

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#getForUpdate}. */
  public static String getForUpdate() {
    return getProvider().getForUpdate();
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#getIds}. */
  public static String getIds() {
    return getProvider().getIds();
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#insert}. */
  public static String insert() {
    return getProvider().insert();
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#update}. */
  public static String update() {
    return getProvider().update();
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#synonyms}. */
  public static String synonyms() {
    return getProvider().synonyms();
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#addSynonym}. */
  public static String addSynonym() {
    return getProvider().addSynonym();
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#clearSynonyms}. */
  public static String clearSynonyms() {
    return getProvider().clearSynonyms();
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#delete}. */
  public static String delete() {
    return getProvider().delete();
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#deleteSynonyms}. */
  public static String deleteSynonyms() {
    return getProvider().deleteSynonyms();
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#dropMetalake}. */
  public static String dropMetalake() {
    return getProvider().dropMetalake();
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#dropMetalakeSynonyms}. */
  public static String dropMetalakeSynonyms() {
    return getProvider().dropMetalakeSynonyms();
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#expired}. */
  public static String expired() {
    return getProvider().expired();
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#purgeSynonyms}. */
  public static String purgeSynonyms() {
    return getProvider().purgeSynonyms();
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#purge}. */
  public static String purge() {
    return getProvider().purge();
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#snapshot}. */
  public static String snapshot(Map<String, Object> params) {
    return getProvider().snapshot(params);
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#conflictingName}. */
  public static String conflictingName() {
    return getProvider().conflictingName();
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#conflictingSynonym}. */
  public static String conflictingSynonym() {
    return getProvider().conflictingSynonym();
  }

  /** Delegates {@link GlossaryTermMetaBaseSQLProvider#idsByKeys}. */
  public static String idsByKeys() {
    return getProvider().idsByKeys();
  }

  private static GlossaryTermMetaBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();
    return PROVIDERS.get(JDBCBackendType.fromString(databaseId));
  }

  static class GlossaryTermMetaMySQLProvider extends GlossaryTermMetaBaseSQLProvider {
    @Override
    public String getForUpdate() {
      // Relation writers lock the primary key before updating the name index. Resolve the ID
      // without locking the name index, then verify the live name on the locked row.
      return selectForUpdate(
          " FORCE INDEX (PRIMARY) WHERE term_id=(SELECT term_id FROM glossary_term_meta WHERE "
              + LIVE_NAME
              + ") AND "
              + LIVE_NAME);
    }
  }
}
