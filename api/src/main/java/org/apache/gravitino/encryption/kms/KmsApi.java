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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.gravitino.annotation.DeveloperApi;

/** The KMS product API that defines key identifier and operation semantics. */
@DeveloperApi
public enum KmsApi {
  /** Amazon Web Services Key Management Service. */
  AWS_KMS("aws-kms"),

  /** Google Cloud Key Management Service. */
  GOOGLE_CLOUD_KMS("google-cloud-kms"),

  /** Microsoft Azure Key Vault. */
  AZURE_KEY_VAULT("azure-key-vault"),

  /** The OpenBao Transit secrets engine API. */
  OPENBAO_TRANSIT("openbao-transit"),

  /** The HashiCorp Vault Transit secrets engine API. */
  VAULT_TRANSIT("vault-transit");

  private final String wireValue;

  KmsApi(String wireValue) {
    this.wireValue = wireValue;
  }

  /**
   * Returns the stable configuration value for this API.
   *
   * @return the configuration value
   */
  @JsonValue
  public String wireValue() {
    return wireValue;
  }

  /**
   * Parses a configured API value.
   *
   * @param value configured API value
   * @return the matching API
   * @throws IllegalArgumentException if the value is null, blank, or unsupported
   */
  @JsonCreator
  public static KmsApi fromWireValue(String value) {
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException("KMS API cannot be blank");
    }

    String normalized = value.trim().toLowerCase(Locale.ROOT);
    return Arrays.stream(values())
        .filter(api -> api.wireValue.equals(normalized))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    String.format(
                        "Unsupported KMS API '%s'; supported values are %s",
                        value,
                        Arrays.stream(values())
                            .map(KmsApi::wireValue)
                            .collect(Collectors.joining(", ")))));
  }
}
