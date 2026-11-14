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

package org.apache.gravitino.iceberg.service.idempotency.mapper;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.iceberg.service.idempotency.mapper.provider.base.IcebergIdempotencyKeyBaseSQLProvider;
import org.apache.gravitino.iceberg.service.idempotency.mapper.provider.postgresql.IcebergIdempotencyKeyPostgreSQLProvider;
import org.apache.gravitino.iceberg.service.idempotency.po.IcebergIdempotencyKeyPO;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

/** Selects SQL for the shared entity store's database dialect. */
public class IcebergIdempotencyKeySQLProviderFactory {

  private static final IcebergIdempotencyKeyBaseSQLProvider BASE_PROVIDER =
      new IcebergIdempotencyKeyBaseSQLProvider();

  private static final Map<JDBCBackendType, IcebergIdempotencyKeyBaseSQLProvider> PROVIDERS =
      ImmutableMap.of(
          JDBCBackendType.MYSQL, BASE_PROVIDER,
          JDBCBackendType.H2, BASE_PROVIDER,
          JDBCBackendType.POSTGRESQL, new IcebergIdempotencyKeyPostgreSQLProvider());

  /**
   * Returns the statement for the configured entity-store database.
   *
   * @param po reservation to persist
   * @return SQL statement
   */
  public static String insert(@Param("po") IcebergIdempotencyKeyPO po) {
    return getProvider().insert(po);
  }

  /**
   * Returns the statement for the configured entity-store database.
   *
   * @param po reservation to persist
   * @return SQL statement
   */
  public static String replaceExpired(@Param("po") IcebergIdempotencyKeyPO po) {
    return getProvider().replaceExpired(po);
  }

  /**
   * Returns the statement for the configured entity-store database.
   *
   * @param idempotencyKey canonical UUIDv7 key
   * @param nowMs current time in unix epoch milliseconds
   * @return SQL statement
   */
  public static String selectRecord(
      @Param("idempotencyKey") String idempotencyKey, @Param("nowMs") long nowMs) {
    return getProvider().selectRecord(idempotencyKey, nowMs);
  }

  /**
   * Returns the statement for the configured entity-store database.
   *
   * @param idempotencyKey canonical UUIDv7 key
   * @param claim reservation fencing token
   * @param httpStatus response status
   * @param responseSummary serialized response body, or null for an empty body
   * @param nowMs current time in unix epoch milliseconds
   * @return SQL statement
   */
  public static String finalizeRecord(
      @Param("idempotencyKey") String idempotencyKey,
      @Param("claim") long claim,
      @Param("httpStatus") int httpStatus,
      @Param("responseSummary") String responseSummary,
      @Param("nowMs") long nowMs) {
    return getProvider().finalizeRecord(idempotencyKey, claim, httpStatus, responseSummary, nowMs);
  }

  /**
   * Returns the statement for the configured entity-store database.
   *
   * @param idempotencyKey canonical UUIDv7 key
   * @param claim reservation fencing token
   * @return SQL statement
   */
  public static String deleteReservation(
      @Param("idempotencyKey") String idempotencyKey, @Param("claim") long claim) {
    return getProvider().deleteReservation(idempotencyKey, claim);
  }

  /**
   * Returns the statement for the configured entity-store database.
   *
   * @param beforeMs inclusive expiry cutoff in unix epoch milliseconds
   * @param batchSize maximum number of rows to delete
   * @return SQL statement
   */
  public static String deleteExpiredBatch(
      @Param("beforeMs") long beforeMs, @Param("batchSize") int batchSize) {
    return getProvider().deleteExpiredBatch(beforeMs, batchSize);
  }

  private static IcebergIdempotencyKeyBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();
    return PROVIDERS.get(JDBCBackendType.fromString(databaseId));
  }
}
