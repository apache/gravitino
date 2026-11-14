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

package org.apache.gravitino.iceberg.service.idempotency.mapper.provider.base;

import static org.apache.gravitino.iceberg.service.idempotency.mapper.IcebergIdempotencyKeyMapper.TABLE_NAME;

import org.apache.gravitino.iceberg.service.idempotency.po.IcebergIdempotencyKeyPO;
import org.apache.ibatis.annotations.Param;

/** SQL statements shared by the MySQL and H2 entity stores. */
public class IcebergIdempotencyKeyBaseSQLProvider {
  /**
   * Returns the reservation INSERT.
   *
   * @param po reservation to persist
   * @return SQL statement
   */
  public String insert(@Param("po") IcebergIdempotencyKeyPO po) {
    return "INSERT INTO "
        + TABLE_NAME
        + " (idempotency_key, operation_binding, claim, audit_info, created_at, expires_at)"
        + " VALUES (#{po.idempotencyKey}, #{po.operationBinding}, #{po.claim}, #{po.auditInfo},"
        + " #{po.createdAt}, #{po.expiresAt})";
  }

  /**
   * Returns an atomic replacement of an expired reservation or response.
   *
   * @param po reservation to persist
   * @return SQL statement
   */
  public String replaceExpired(@Param("po") IcebergIdempotencyKeyPO po) {
    return "UPDATE "
        + TABLE_NAME
        + " SET operation_binding = #{po.operationBinding}, claim = #{po.claim},"
        + " http_status = NULL, response_summary = NULL, audit_info = #{po.auditInfo},"
        + " created_at = #{po.createdAt}, expires_at = #{po.expiresAt}"
        + " WHERE idempotency_key = #{po.idempotencyKey} AND expires_at <= #{po.createdAt}";
  }

  /**
   * Returns only a finalized, unexpired response.
   *
   * @param idempotencyKey canonical UUIDv7 key
   * @param nowMs current time in unix epoch milliseconds
   * @return SQL statement
   */
  public String selectRecord(
      @Param("idempotencyKey") String idempotencyKey, @Param("nowMs") long nowMs) {
    return "SELECT idempotency_key AS idempotencyKey, operation_binding AS operationBinding,"
        + " claim, http_status AS httpStatus, response_summary AS responseSummary,"
        + " audit_info AS auditInfo, created_at AS createdAt, expires_at AS expiresAt FROM "
        + TABLE_NAME
        + " WHERE idempotency_key = #{idempotencyKey}"
        + " AND http_status IS NOT NULL AND expires_at > #{nowMs}";
  }

  /**
   * Returns a finalization guarded by the reservation's claim and expiry.
   *
   * @param idempotencyKey canonical UUIDv7 key
   * @param claim reservation fencing token
   * @param httpStatus response status
   * @param responseSummary serialized response body, or null for an empty body
   * @param nowMs current time in unix epoch milliseconds
   * @return SQL statement
   */
  public String finalizeRecord(
      @Param("idempotencyKey") String idempotencyKey,
      @Param("claim") long claim,
      @Param("httpStatus") int httpStatus,
      @Param("responseSummary") String responseSummary,
      @Param("nowMs") long nowMs) {
    return "UPDATE "
        + TABLE_NAME
        + " SET http_status = #{httpStatus}, response_summary = #{responseSummary}"
        + " WHERE idempotency_key = #{idempotencyKey} AND claim = #{claim}"
        + " AND http_status IS NULL AND expires_at > #{nowMs}";
  }

  /**
   * Returns a release guarded by the reservation's claim. Finalized responses are retained.
   *
   * @param idempotencyKey canonical UUIDv7 key
   * @param claim reservation fencing token
   * @return SQL statement
   */
  public String deleteReservation(
      @Param("idempotencyKey") String idempotencyKey, @Param("claim") long claim) {
    return "DELETE FROM "
        + TABLE_NAME
        + " WHERE idempotency_key = #{idempotencyKey} AND claim = #{claim} AND http_status IS NULL";
  }

  /**
   * Returns an indexed, bounded deletion including the exact expiry cutoff.
   *
   * @param beforeMs inclusive expiry cutoff in unix epoch milliseconds
   * @param batchSize maximum number of rows to delete
   * @return SQL statement
   */
  public String deleteExpiredBatch(
      @Param("beforeMs") long beforeMs, @Param("batchSize") int batchSize) {
    return "DELETE FROM " + TABLE_NAME + " WHERE expires_at <= #{beforeMs} LIMIT #{batchSize}";
  }
}
