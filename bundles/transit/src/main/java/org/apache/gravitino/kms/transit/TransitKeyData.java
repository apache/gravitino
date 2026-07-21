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
package org.apache.gravitino.kms.transit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
final class TransitKeyData {

  private final Boolean supportsEncryption;
  private final Boolean supportsDecryption;
  private final Boolean softDeleted;

  @JsonCreator
  TransitKeyData(
      @JsonProperty("supports_encryption") Boolean supportsEncryption,
      @JsonProperty("supports_decryption") Boolean supportsDecryption,
      @JsonProperty("soft_deleted") Boolean softDeleted) {
    this.supportsEncryption = supportsEncryption;
    this.supportsDecryption = supportsDecryption;
    this.softDeleted = softDeleted;
  }

  Boolean supportsEncryption() {
    return supportsEncryption;
  }

  Boolean supportsDecryption() {
    return supportsDecryption;
  }

  Boolean softDeleted() {
    return softDeleted;
  }
}
