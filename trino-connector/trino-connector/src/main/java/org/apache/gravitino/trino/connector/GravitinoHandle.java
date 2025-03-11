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
package org.apache.gravitino.trino.connector;

import static org.apache.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT;

import io.trino.spi.TrinoException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.gravitino.trino.connector.util.json.JsonCodec;

/** this interface for GravitinoXXXHande to communicate with Trino */
interface GravitinoHandle<T> {
  String HANDLE_STRING = "handleString";

  /**
   * Serialize handle to json string
   *
   * @return the json string
   */
  public String getHandleString();

  /**
   * Retrieve the internal handle
   *
   * @return the internal handle
   */
  public T getInternalHandle();

  public static <T> T unWrap(T handle) {
    return ((GravitinoHandle<T>) handle).getInternalHandle();
  }

  public static <T> List<T> unWrap(List<T> handles) {
    return handles.stream()
        .map(handle -> (((GravitinoHandle<T>) handle).getInternalHandle()))
        .collect(Collectors.toList());
  }

  public static <T> Map<String, T> unWrap(Map<String, T> handleMap) {
    return handleMap.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, e -> ((GravitinoHandle<T>) e.getValue()).getInternalHandle()));
  }

  class HandleWrapper<T> {
    private final T handle;
    private String valueString;
    private final Class<T> clazz;

    public HandleWrapper(Class<T> clazz) {
      this.handle = null;
      this.clazz = clazz;
    }

    public HandleWrapper(T handle) {
      this.handle = Objects.requireNonNull(handle, "handle is not null");
      clazz = (Class<T>) handle.getClass();
    }

    public HandleWrapper<T> fromJson(String valueString) {
      try {
        T newHandle =
            JsonCodec.getMapper(clazz.getClassLoader()).readerFor(clazz).readValue(valueString);
        return new HandleWrapper<>(newHandle);
      } catch (Exception e) {
        throw new TrinoException(GRAVITINO_ILLEGAL_ARGUMENT, "Can not deserialize from json", e);
      }
    }

    public String toJson() {
      if (valueString == null) {
        try {
          valueString =
              JsonCodec.getMapper(clazz.getClassLoader())
                  .writerFor(clazz)
                  .writeValueAsString(this.handle);
        } catch (Exception e) {
          throw new TrinoException(GRAVITINO_ILLEGAL_ARGUMENT, "Can not serialize to json", e);
        }
      }
      return valueString;
    }

    public T getHandle() {
      return handle;
    }
  }
}
