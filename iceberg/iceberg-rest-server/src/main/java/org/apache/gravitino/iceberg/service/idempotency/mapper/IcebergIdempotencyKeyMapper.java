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

import javax.annotation.Nullable;
import org.apache.gravitino.iceberg.service.idempotency.po.IcebergIdempotencyKeyPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/**
 * MyBatis mapper for the {@code iceberg_idempotency_key_meta} table. SQL is supplied per backend by
 * {@link IcebergIdempotencyKeySQLProviderFactory} and executed through the Gravitino entity store's
 * shared {@code SqlSessionFactory}, so the durable idempotency store reuses the relational
 * backend's connection pool and multi-backend handling instead of opening its own JDBC connections.
 */
public interface IcebergIdempotencyKeyMapper {

  /** Table containing reservations and replayable responses. */
  String TABLE_NAME = "iceberg_idempotency_key_meta";

  /**
   * Inserts a reservation; a duplicate key is handled by the store.
   *
   * @param po reservation to persist
   * @return number of affected rows
   */
  @InsertProvider(type = IcebergIdempotencyKeySQLProviderFactory.class, method = "insert")
  int insert(@Param("po") IcebergIdempotencyKeyPO po);

  /**
   * Replaces an expired row only if it has not been reclaimed by a peer.
   *
   * @param po reservation to persist
   * @return number of affected rows
   */
  @UpdateProvider(type = IcebergIdempotencyKeySQLProviderFactory.class, method = "replaceExpired")
  int replaceExpired(@Param("po") IcebergIdempotencyKeyPO po);

  /**
   * Loads a finalized response within its reuse window.
   *
   * @param idempotencyKey canonical UUIDv7 key
   * @param nowMs current time in unix epoch milliseconds
   * @return matching response, or null
   */
  @SelectProvider(type = IcebergIdempotencyKeySQLProviderFactory.class, method = "selectRecord")
  @Nullable
  IcebergIdempotencyKeyPO selectRecord(
      @Param("idempotencyKey") String idempotencyKey, @Param("nowMs") long nowMs);

  /**
   * Finalizes a live reservation owned by the given claim.
   *
   * @param idempotencyKey canonical UUIDv7 key
   * @param claim reservation fencing token
   * @param httpStatus response status
   * @param responseSummary serialized response body, or null for an empty body
   * @param nowMs current time in unix epoch milliseconds
   * @return number of affected rows
   */
  @UpdateProvider(type = IcebergIdempotencyKeySQLProviderFactory.class, method = "finalizeRecord")
  int finalizeRecord(
      @Param("idempotencyKey") String idempotencyKey,
      @Param("claim") long claim,
      @Param("httpStatus") int httpStatus,
      @Param("responseSummary") @Nullable String responseSummary,
      @Param("nowMs") long nowMs);

  /**
   * Deletes only rows matching the statement's ownership or expiry guard.
   *
   * @param idempotencyKey canonical UUIDv7 key
   * @param claim reservation fencing token
   * @return number of affected rows
   */
  @DeleteProvider(
      type = IcebergIdempotencyKeySQLProviderFactory.class,
      method = "deleteReservation")
  int deleteReservation(@Param("idempotencyKey") String idempotencyKey, @Param("claim") long claim);

  /**
   * Deletes only rows matching the statement's ownership or expiry guard.
   *
   * @param beforeMs inclusive expiry cutoff in unix epoch milliseconds
   * @param batchSize maximum number of rows to delete
   * @return number of affected rows
   */
  @DeleteProvider(
      type = IcebergIdempotencyKeySQLProviderFactory.class,
      method = "deleteExpiredBatch")
  int deleteExpiredBatch(@Param("beforeMs") long beforeMs, @Param("batchSize") int batchSize);
}
