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

package org.apache.gravitino.function;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;

/** Function type supported by Gravitino. */
@Evolving
public enum FunctionType {
  /** Scalar function. */
  SCALAR,

  /** Aggregate function. */
  AGGREGATE,

  /** Table-valued function. */
  TABLE;

  private static final Map<String, FunctionType> NAME_TO_TYPE;

  static {
    Map<String, FunctionType> map = new HashMap<>();
    for (FunctionType value : values()) {
      map.put(value.typeName().toLowerCase(Locale.ROOT), value);
    }
    map.put("agg", AGGREGATE);
    NAME_TO_TYPE = Collections.unmodifiableMap(map);
  }

  /**
   * Parse the function type from a string value.
   *
   * @param type the string to parse.
   * @return the parsed {@link FunctionType}.
   * @throws IllegalArgumentException if the value cannot be parsed.
   */
  public static FunctionType fromString(String type) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(type) && NAME_TO_TYPE.containsKey(type.toLowerCase(Locale.ROOT)),
        "Invalid function type: " + type);
    return NAME_TO_TYPE.get(type.toLowerCase(Locale.ROOT));
  }

  /**
   * @return the canonical string representation used by APIs.
   */
  public String typeName() {
    return name().toLowerCase(Locale.ROOT);
  }
}
