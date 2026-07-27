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

package org.apache.gravitino.iceberg.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.requests.PlanTableScanRequestParser;
import org.apache.iceberg.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code plan-task} token handed out by {@code POST .../tables/{table}/plan} and redeemed at
 * {@code POST .../tables/{table}/tasks}.
 *
 * <p>The token is self-describing: it carries the table, the scan request it was planned from (with
 * the snapshot pinned at planning time) and the slice of the planned file scan tasks it stands for.
 * Nothing about the token is stored server side, so it stays valid across server restarts and can
 * be redeemed by any Gravitino instance that serves the same catalog.
 *
 * <p>The token is opaque to clients but is not a capability: it grants no access on its own. {@code
 * POST .../tasks} authorizes the table in the request path and rejects a token minted for a
 * different table, so a forged token can at most express a scan the caller could already submit
 * through {@code POST .../plan}.
 */
class PlanTaskToken {

  private static final Logger LOG = LoggerFactory.getLogger(PlanTaskToken.class);

  private static final int CURRENT_VERSION = 1;

  private static final String VERSION = "version";
  private static final String TABLE = "table";
  private static final String OFFSET = "offset";
  private static final String LIMIT = "limit";
  private static final String SCAN = "scan";

  private final String table;
  private final PlanTableScanRequest scanRequest;
  private final int offset;
  private final int limit;

  private PlanTaskToken(String table, PlanTableScanRequest scanRequest, int offset, int limit) {
    this.table = table;
    this.scanRequest = scanRequest;
    this.offset = offset;
    this.limit = limit;
  }

  /**
   * Encodes a token covering {@code limit} file scan tasks starting at {@code offset} of the plan
   * produced by {@code scanRequest}.
   *
   * @param tableIdentifier the table the plan belongs to.
   * @param scanRequest the scan request the plan was produced from, with the snapshot pinned.
   * @param offset index of the first file scan task the token covers.
   * @param limit maximum number of file scan tasks the token covers.
   * @return the encoded token.
   */
  static String encode(
      TableIdentifier tableIdentifier, PlanTableScanRequest scanRequest, int offset, int limit) {
    ObjectNode node = JsonUtil.mapper().createObjectNode();
    node.put(VERSION, CURRENT_VERSION);
    node.put(TABLE, tableIdentifier.toString());
    node.put(OFFSET, offset);
    node.put(LIMIT, limit);
    try {
      node.set(SCAN, JsonUtil.mapper().readTree(PlanTableScanRequestParser.toJson(scanRequest)));
      return Base64.getUrlEncoder()
          .withoutPadding()
          .encodeToString(
              JsonUtil.mapper().writeValueAsString(node).getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to encode plan task for table " + tableIdentifier, e);
    }
  }

  /**
   * Decodes a token previously produced by {@link #encode}.
   *
   * @param planTask the token presented by the client.
   * @return the decoded token, or empty if the token was not issued by this server.
   */
  static Optional<PlanTaskToken> decode(String planTask) {
    if (planTask == null || planTask.isEmpty()) {
      return Optional.empty();
    }

    try {
      byte[] decoded = Base64.getUrlDecoder().decode(planTask);
      JsonNode node = JsonUtil.mapper().readTree(new String(decoded, StandardCharsets.UTF_8));
      if (!node.isObject() || JsonUtil.getInt(VERSION, node) != CURRENT_VERSION) {
        return Optional.empty();
      }

      int offset = JsonUtil.getInt(OFFSET, node);
      int limit = JsonUtil.getInt(LIMIT, node);
      if (offset < 0 || limit <= 0) {
        return Optional.empty();
      }

      return Optional.of(
          new PlanTaskToken(
              JsonUtil.getString(TABLE, node),
              PlanTableScanRequestParser.fromJson(JsonUtil.get(SCAN, node)),
              offset,
              limit));
    } catch (Exception e) {
      // Any malformed token is simply a token this server did not issue.
      LOG.debug("Ignoring plan task that could not be decoded: {}", planTask, e);
      return Optional.empty();
    }
  }

  /**
   * Returns whether this token was minted for {@code tableIdentifier}.
   *
   * @param tableIdentifier the table from the request path.
   * @return true if the token belongs to that table.
   */
  boolean matchesTable(TableIdentifier tableIdentifier) {
    return table.equals(tableIdentifier.toString());
  }

  PlanTableScanRequest scanRequest() {
    return scanRequest;
  }

  int offset() {
    return offset;
  }

  int limit() {
    return limit;
  }

  @Override
  public String toString() {
    return String.format(
        "PlanTaskToken{table=%s, offset=%s, limit=%s, scan=%s}", table, offset, limit, scanRequest);
  }
}
