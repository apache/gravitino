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

package org.apache.gravitino.storage.kv;

/**
 * The status of a value. The value can be normal or deleted. The deleted value is not visible to
 * the user and can be garbage collected.
 *
 * <p>In the future, we may add more status, such as tombstone and so on.
 */
public enum ValueStatusEnum {
  // The value is normal.
  NORMAL((byte) 0),

  // The value has been deleted.
  DELETED((byte) 1);

  private final byte code;

  ValueStatusEnum(byte code) {
    this.code = code;
  }

  public byte getCode() {
    return code;
  }

  public static ValueStatusEnum fromCode(byte code) {
    for (ValueStatusEnum valueStatusEnum : ValueStatusEnum.values()) {
      if (valueStatusEnum.getCode() == code) {
        return valueStatusEnum;
      }
    }
    throw new IllegalArgumentException("Invalid code: " + code);
  }
}
