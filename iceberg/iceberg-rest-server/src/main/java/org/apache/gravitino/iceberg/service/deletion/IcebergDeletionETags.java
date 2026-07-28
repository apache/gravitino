/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.iceberg.service.deletion;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.gravitino.storage.relational.po.EntityDeletionPO;
import org.apache.gravitino.storage.relational.po.TablePO;

/** Strong validators for safe, non-secret deletion-action representations. */
public final class IcebergDeletionETags {

  private static final char[] HEX = "0123456789abcdef".toCharArray();

  private IcebergDeletionETags() {}

  /**
   * Computes a strong validator token without HTTP quotes.
   *
   * <p>Only fields in the public deletion representation participate. Worker-only progress and
   * errors therefore do not invalidate a client's lifecycle precondition.
   *
   * @param retained retained table root and deletion action
   * @param serverNow authoritative request time used by the public recoverable field
   * @return strong validator token
   */
  public static String strongTag(IcebergRetainedTableDeletion retained, long serverNow) {
    EntityDeletionPO deletion = retained.getDeletion();
    TablePO table = retained.getTable();
    String canonical =
        String.join(
            "\n",
            deletion.getDeletionId(),
            String.valueOf(table.getTableId()),
            String.valueOf(table.getCurrentVersion()),
            String.valueOf(table.getSchemaId()),
            String.valueOf(table.getTableName()),
            String.valueOf(deletion.getState()),
            String.valueOf(table.getDeletedAt()),
            String.valueOf(deletion.getRetentionExpiresAt()),
            String.valueOf(deletion.getPurgeJobId()),
            String.valueOf(IcebergTableDeletionLifecycle.isRecoverable(deletion, serverNow)));
    return "iceberg-deletion-"
        + deletion.getDeletionId()
        + "-"
        + sha256(canonical).substring(0, 16);
  }

  private static String sha256(String value) {
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
      StringBuilder result = new StringBuilder(digest.length * 2);
      for (byte part : digest) {
        result.append(HEX[(part >>> 4) & 0x0f]);
        result.append(HEX[part & 0x0f]);
      }
      return result.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }
}
