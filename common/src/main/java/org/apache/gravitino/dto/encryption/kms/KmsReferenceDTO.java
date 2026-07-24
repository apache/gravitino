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
package org.apache.gravitino.dto.encryption.kms;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.gravitino.encryption.kms.KmsApi;
import org.apache.gravitino.encryption.kms.KmsReference;

/** Data transfer object for a KMS key reference. */
@Getter
@EqualsAndHashCode
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor
@Builder(setterPrefix = "with")
public class KmsReferenceDTO {

  @JsonProperty("api")
  private String api;

  @JsonProperty("source")
  private String source;

  @JsonProperty("keyId")
  private String keyId;

  /**
   * Converts this DTO to a {@link KmsReference}.
   *
   * @return the KMS key reference
   */
  public KmsReference toKmsReference() {
    return new KmsReference(KmsApi.fromWireValue(api), source, keyId);
  }

  /**
   * Creates a DTO from a {@link KmsReference}.
   *
   * @param reference the KMS key reference
   * @return the KMS key reference DTO
   */
  public static KmsReferenceDTO fromKmsReference(KmsReference reference) {
    return new KmsReferenceDTO(reference.api().wireValue(), reference.source(), reference.keyId());
  }
}
