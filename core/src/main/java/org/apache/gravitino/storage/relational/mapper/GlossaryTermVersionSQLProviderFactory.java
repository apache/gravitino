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
import org.apache.gravitino.storage.relational.mapper.provider.base.GlossaryTermVersionBaseSQLProvider;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;

/** Selects glossary term version statements for the configured database. */
public class GlossaryTermVersionSQLProviderFactory {
  private static final GlossaryTermVersionBaseSQLProvider BASE =
      new GlossaryTermVersionBaseSQLProvider();
  private static final Map<JDBCBackendType, GlossaryTermVersionBaseSQLProvider> PROVIDERS =
      ImmutableMap.of(
          JDBCBackendType.MYSQL, BASE, JDBCBackendType.H2, BASE, JDBCBackendType.POSTGRESQL, BASE);

  /** Delegates {@link GlossaryTermVersionBaseSQLProvider#getVersionHeadByMetalakeAndTermId}. */
  public static String getVersionHeadByMetalakeAndTermId() {
    return getProvider().getVersionHeadByMetalakeAndTermId();
  }

  /** Delegates {@link GlossaryTermVersionBaseSQLProvider#findTermIdByReservedName}. */
  public static String findTermIdByReservedName() {
    return getProvider().findTermIdByReservedName();
  }

  /** Delegates {@link GlossaryTermVersionBaseSQLProvider#insertVersionHead}. */
  public static String insertVersionHead() {
    return getProvider().insertVersionHead();
  }

  /** Delegates {@link GlossaryTermVersionBaseSQLProvider#updateVersionHead}. */
  public static String updateVersionHead() {
    return getProvider().updateVersionHead();
  }

  /** Delegates {@link GlossaryTermVersionBaseSQLProvider#insertTermVersion}. */
  public static String insertTermVersion() {
    return getProvider().insertTermVersion();
  }

  /** Delegates {@link GlossaryTermVersionBaseSQLProvider#getVersionByTermIdAndVersionId}. */
  public static String getVersionByTermIdAndVersionId() {
    return getProvider().getVersionByTermIdAndVersionId();
  }

  /** Delegates {@link GlossaryTermVersionBaseSQLProvider#listVersionsByTermId}. */
  public static String listVersionsByTermId() {
    return getProvider().listVersionsByTermId();
  }

  /** Delegates {@link GlossaryTermVersionBaseSQLProvider#updateVersionLifecycle}. */
  public static String updateVersionLifecycle() {
    return getProvider().updateVersionLifecycle();
  }

  /** Delegates {@link GlossaryTermVersionBaseSQLProvider#deleteVersionsByTermId}. */
  public static String deleteVersionsByTermId() {
    return getProvider().deleteVersionsByTermId();
  }

  /** Delegates {@link GlossaryTermVersionBaseSQLProvider#deleteVersionHeadByTermId}. */
  public static String deleteVersionHeadByTermId() {
    return getProvider().deleteVersionHeadByTermId();
  }

  /** Delegates {@link GlossaryTermVersionBaseSQLProvider#deleteVersionsByMetalakeId}. */
  public static String deleteVersionsByMetalakeId() {
    return getProvider().deleteVersionsByMetalakeId();
  }

  /** Delegates {@link GlossaryTermVersionBaseSQLProvider#deleteVersionHeadsByMetalakeId}. */
  public static String deleteVersionHeadsByMetalakeId() {
    return getProvider().deleteVersionHeadsByMetalakeId();
  }

  private static GlossaryTermVersionBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();
    return PROVIDERS.get(JDBCBackendType.fromString(databaseId));
  }
}
