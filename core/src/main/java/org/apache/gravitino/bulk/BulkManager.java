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
package org.apache.gravitino.bulk;

import java.util.List;
import java.util.Optional;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.AccessControlDispatcher;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.dto.responses.BulkError;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.exceptions.AlreadyExistsException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.exceptions.NotInUseException;
import org.apache.gravitino.metalake.MetalakeManager;

/** Manages best-effort bulk operations. */
public class BulkManager {

  private final AccessControlDispatcher accessControlDispatcher;
  private final OwnerDispatcher ownerDispatcher;
  private final int bulkMaxItems;

  /**
   * Creates a new {@link BulkManager}.
   *
   * @param config The Gravitino configuration.
   * @param accessControlDispatcher The access-control dispatcher.
   * @param ownerDispatcher The owner dispatcher.
   */
  public BulkManager(
      Config config,
      AccessControlDispatcher accessControlDispatcher,
      OwnerDispatcher ownerDispatcher) {
    this.accessControlDispatcher = accessControlDispatcher;
    this.ownerDispatcher = ownerDispatcher;
    this.bulkMaxItems =
        config == null
            ? Configs.BULK_MAX_ITEMS.getDefaultValue()
            : config.get(Configs.BULK_MAX_ITEMS);
  }

  /**
   * Adds users in bulk.
   *
   * @param metalake The metalake name.
   * @param users The users to add.
   * @return The item-level results.
   */
  public List<BulkItemResult<User>> addUsers(String metalake, List<UserAdd> users) {
    checkBulkSize("users", users.size());
    MetalakeManager.checkMetalakeInUse(metalake);
    return accessControlDispatcher.addUsers(metalake, users);
  }

  /**
   * Removes users in bulk.
   *
   * @param metalake The metalake name.
   * @param users The user names to remove.
   * @return The item-level results.
   */
  public List<BulkItemResult<String>> removeUsers(String metalake, List<String> users) {
    checkBulkSize("names", users.size());
    MetalakeManager.checkMetalakeInUse(metalake);
    Optional<Owner> metalakeOwner =
        ownerDispatcher.getOwner(
            metalake, MetadataObjects.of(null, metalake, MetadataObject.Type.METALAKE));
    return accessControlDispatcher.removeUsers(metalake, users, metalakeOwner);
  }

  /**
   * Converts an item-level exception to a bulk error.
   *
   * @param result The failed item result.
   * @return The bulk error.
   */
  public BulkError toBulkError(BulkItemResult<?> result) {
    Exception error =
        result
            .error()
            .orElseThrow(() -> new IllegalArgumentException("Bulk item result has no error"));
    return new BulkError(
        result.index(),
        result.name(),
        errorCode(error),
        error.getClass().getSimpleName(),
        error.getMessage());
  }

  private void checkBulkSize(String fieldName, int size) {
    if (size > bulkMaxItems) {
      throw new IllegalArgumentException(
          String.format(
              "\"%s\" size %d exceeds the maximum allowed bulk items %d",
              fieldName, size, bulkMaxItems));
    }
  }

  private int errorCode(Exception e) {
    if (e instanceof IllegalArgumentException) {
      return ErrorConstants.ILLEGAL_ARGUMENTS_CODE;
    } else if (e instanceof NotFoundException) {
      return ErrorConstants.NOT_FOUND_CODE;
    } else if (e instanceof AlreadyExistsException) {
      return ErrorConstants.ALREADY_EXISTS_CODE;
    } else if (e instanceof ForbiddenException) {
      return ErrorConstants.FORBIDDEN_CODE;
    } else if (e instanceof NotInUseException) {
      return ErrorConstants.NOT_IN_USE_CODE;
    }
    return ErrorConstants.INTERNAL_ERROR_CODE;
  }
}
