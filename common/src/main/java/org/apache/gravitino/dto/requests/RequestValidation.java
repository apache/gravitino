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
package org.apache.gravitino.dto.requests;

import com.google.common.base.Preconditions;
import java.util.Map;

final class RequestValidation {
  private RequestValidation() {}

  static void properties(Map<String, String> properties) {
    if (properties != null) {
      properties.forEach(
          (key, value) -> {
            Preconditions.checkArgument(key != null, "properties must not contain null keys");
            Preconditions.checkArgument(value != null, "properties[%s] must not be null", key);
          });
    }
  }

  static void elements(Object[] elements, String field) {
    if (elements != null) {
      for (int i = 0; i < elements.length; i++) {
        Preconditions.checkArgument(elements[i] != null, "%s[%s] must not be null", field, i);
      }
    }
  }
}
