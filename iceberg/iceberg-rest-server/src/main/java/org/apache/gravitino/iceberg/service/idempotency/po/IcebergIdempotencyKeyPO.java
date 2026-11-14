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

package org.apache.gravitino.iceberg.service.idempotency.po;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.UncheckedIOException;
import java.time.Instant;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.gravitino.iceberg.common.idempotency.IdempotencyRecord;
import org.apache.gravitino.iceberg.service.idempotency.mapper.IcebergIdempotencyKeyMapper;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * Persistence object for one {@code iceberg_idempotency_key_meta} row. Mirrors the table columns
 * and is mapped by {@link IcebergIdempotencyKeyMapper}, and converts to the {@link
 * IdempotencyRecord} the store SPI hands back. Instances are created through the generated {@code
 * builder()} (or populated by MyBatis via field reflection through the private no-arg constructor)
 * and expose no setters, so they are effectively immutable once built.
 */
@Getter
@Builder(setterPrefix = "with")
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class IcebergIdempotencyKeyPO {

  private String idempotencyKey;
  private String operationBinding;
  private Long claim;
  @Nullable private Integer httpStatus;
  @Nullable private String responseSummary;
  private String auditInfo;
  private Long createdAt;
  private Long expiresAt;

  /**
   * Builds a new reserved (not yet finalized) row.
   *
   * @param idempotencyKey the client-provided UUIDv7 key
   * @param operationBinding request identity, for example {@code POST
   *     /v1/cat1/namespaces/ns1/tables}
   * @param claim the reserving caller's fencing token
   * @param createdAtMs reservation time in unix epoch millis
   * @param expiresAtMs time after which the record may be purged, in unix epoch millis
   * @return the persistence object
   */
  public static IcebergIdempotencyKeyPO reserved(
      String idempotencyKey,
      String operationBinding,
      long claim,
      long createdAtMs,
      long expiresAtMs) {
    return builder()
        .withIdempotencyKey(idempotencyKey)
        .withOperationBinding(operationBinding)
        .withClaim(claim)
        .withHttpStatus(null)
        .withResponseSummary(null)
        .withAuditInfo(auditInfoJson(createdAtMs))
        .withCreatedAt(createdAtMs)
        .withExpiresAt(expiresAtMs)
        .build();
  }

  /**
   * Converts this row to the domain record.
   *
   * @return the domain record, finalized iff the row carries an HTTP status
   */
  public IdempotencyRecord toIdempotencyRecord() {
    IdempotencyRecord record =
        IdempotencyRecord.reserved(idempotencyKey, operationBinding, claim, createdAt, expiresAt);
    return httpStatus == null ? record : record.withResponse(httpStatus, responseSummary);
  }

  // Record the reserving principal before the catalog mutation begins.
  private static String auditInfoJson(long createdAtMs) {
    AuditInfo auditInfo =
        AuditInfo.builder()
            .withCreator(PrincipalUtils.getCurrentUserName())
            .withCreateTime(Instant.ofEpochMilli(createdAtMs))
            .build();
    try {
      return JsonUtils.anyFieldMapper().writeValueAsString(auditInfo);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException("Failed to serialize the idempotency record audit info", e);
    }
  }
}
