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
package org.apache.gravitino.lance.common.ops;

import java.util.Arrays;
import org.apache.gravitino.lance.common.ops.gravitino.GravitinoLanceNamespaceWrapper;

public enum LanceNamespaceBackend {
  GRAVITINO("gravitino", GravitinoLanceNamespaceWrapper.class);

  private final String type;
  private final Class<? extends NamespaceWrapper> wrapperClass;

  public static LanceNamespaceBackend fromType(String type) {
    for (LanceNamespaceBackend backend : values()) {
      if (backend.type.equalsIgnoreCase(type)) {
        return backend;
      }
    }
    throw new IllegalArgumentException(
        String.format(
            "Unknown backend type %s, available types: %s",
            type, Arrays.toString(LanceNamespaceBackend.values())));
  }

  LanceNamespaceBackend(String type, Class<? extends NamespaceWrapper> wrapperClass) {
    this.type = type;
    this.wrapperClass = wrapperClass;
  }

  public String getType() {
    return type;
  }

  public Class<? extends NamespaceWrapper> getWrapperClass() {
    return wrapperClass;
  }
}
