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

package org.apache.gravitino.idp.basic;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

/** Validates built-in IdP username and password credentials. */
public final class IdpCredentialValidator {

  private static final int MIN_PASSWORD_LENGTH = 12;

  private static final int MAX_PASSWORD_LENGTH = 64;

  /** Matches {@code idp_user_meta.user_name} and {@code idp_group_meta.group_name} column size. */
  private static final int MAX_NAME_LENGTH = 128;

  private IdpCredentialValidator() {}

  public static void validateUsername(String username) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(username), "\"user\" field is required and cannot be empty");
    Preconditions.checkArgument(!username.contains(":"), "User name cannot contain a colon (:)");
    Preconditions.checkArgument(
        username.length() <= MAX_NAME_LENGTH,
        "Username must not exceed %s characters",
        MAX_NAME_LENGTH);
  }

  public static void validateGroupName(String groupName) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(groupName), "\"group\" field is required and cannot be empty");
    Preconditions.checkArgument(
        groupName.length() <= MAX_NAME_LENGTH,
        "Group name must not exceed %s characters",
        MAX_NAME_LENGTH);
  }

  public static void validatePassword(String password) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(password), "\"password\" field is required and cannot be empty");
    Preconditions.checkArgument(
        password.length() >= MIN_PASSWORD_LENGTH && password.length() <= MAX_PASSWORD_LENGTH,
        "Password must be at least %s characters long and at most %s characters long",
        MIN_PASSWORD_LENGTH,
        MAX_PASSWORD_LENGTH);
  }
}
