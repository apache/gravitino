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
package org.apache.gravitino.encryption.kms;

import com.google.common.base.Preconditions;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.DeveloperApi;

/**
 * Validation helpers for KMS API identifiers.
 *
 * <p>Identifiers are matched exactly. They must be lowercase kebab-case with no surrounding
 * whitespace (for example {@code aws-kms}). Values are never normalized.
 */
@DeveloperApi
public final class KmsApiIdentifiers {

  private static final Pattern LOWERCASE_KEBAB_CASE = Pattern.compile("^[a-z0-9]+(-[a-z0-9]+)*$");

  private KmsApiIdentifiers() {}

  /**
   * Validates a KMS API identifier.
   *
   * @param api the identifier to validate
   * @return the same identifier when valid
   * @throws IllegalArgumentException if {@code api} is null, blank, padded, or not lowercase
   *     kebab-case
   */
  public static String requireValid(String api) {
    Preconditions.checkArgument(StringUtils.isNotBlank(api), "KMS API cannot be blank");
    Preconditions.checkArgument(
        api.equals(api.trim()), "KMS API cannot have leading or trailing whitespace");
    Preconditions.checkArgument(
        LOWERCASE_KEBAB_CASE.matcher(api).matches(),
        "KMS API must be lowercase kebab-case: '%s'",
        api);
    return api;
  }
}
