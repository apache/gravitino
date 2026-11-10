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
package org.apache.gravitino.storage.relational.po.cache;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Operate type emitted into {@code entity_change_log.operate_type}.
 *
 * <p>The numeric {@code code} is what is persisted in the database. Codes are stable forever and
 * must never be reused: removing a value from this enum is allowed, but the same numeric code
 * cannot subsequently be assigned to a different meaning, otherwise old log rows would be
 * misinterpreted by readers.
 */
public enum OperateType {
  ALTER(1),
  DROP(2);

  private static final Map<Integer, OperateType> BY_CODE =
      Arrays.stream(values()).collect(Collectors.toMap(OperateType::getCode, Function.identity()));

  private final int code;

  OperateType(int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }

  public static OperateType fromCode(int code) {
    OperateType type = BY_CODE.get(code);
    if (type == null) {
      throw new IllegalArgumentException("Unknown OperateType code: " + code);
    }
    return type;
  }
}
