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
  /** The key used for handle string in JSON serialization */
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

  /**
   * Unwrap a handle
   *
   * @param handle the handle
   * @return the unwrapped handle
   */
  public static <T> T unWrap(T handle) {
    return ((GravitinoHandle<T>) handle).getInternalHandle();
  }

  /**
   * Unwrap a list of handles
   *
   * @param handles the list of handles
   * @return the unwrapped list of handles
   */
  public static <T> List<T> unWrap(List<T> handles) {
    return handles.stream()
        .map(handle -> (((GravitinoHandle<T>) handle).getInternalHandle()))
        .collect(Collectors.toList());
  }

  /**
   * Unwrap a map of handles
   *
   * @param handleMap the map of handles
   * @return the unwrapped map of handles
   */
  public static <T> Map<String, T> unWrap(Map<String, T> handleMap) {
    return handleMap.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, e -> ((GravitinoHandle<T>) e.getValue()).getInternalHandle()));
  }

  /**
   * A wrapper class for handling serialization and deserialization of Gravitino handles.
   *
   * @param <T> The type of handle being wrapped
   */
  class HandleWrapper<T> {
    /** The wrapped handle instance */
    private final T handle;

    /** Cached JSON string representation of the handle */
    private String valueString;

    /** The class type of the handle */
    private final Class<T> clazz;

    /**
     * Constructs a new HandleWrapper with a null handle.
     *
     * @param clazz The class type of the handle
     */
    public HandleWrapper(Class<T> clazz) {
      this.handle = null;
      this.clazz = clazz;
    }

    /**
     * Constructs a new HandleWrapper with the specified handle.
     *
     * @param handle The handle to wrap
     * @throws NullPointerException if handle is null
     */
    public HandleWrapper(T handle) {
      this.handle = Objects.requireNonNull(handle, "handle is not null");
      clazz = (Class<T>) handle.getClass();
    }

    /**
     * Creates a new HandleWrapper by deserializing from a JSON string.
     *
     * @param valueString The JSON string to deserialize
     * @return A new HandleWrapper instance
     * @throws TrinoException if deserialization fails
     */
    public HandleWrapper<T> fromJson(String valueString) {
      try {
        T newHandle =
            JsonCodec.getMapper(clazz.getClassLoader()).readerFor(clazz).readValue(valueString);
        return new HandleWrapper<>(newHandle);
      } catch (Exception e) {
        throw new TrinoException(GRAVITINO_ILLEGAL_ARGUMENT, "Can not deserialize from json", e);
      }
    }

    /**
     * Serializes the wrapped handle to a JSON string.
     *
     * @return The JSON string representation of the handle
     * @throws TrinoException if serialization fails
     */
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

    /**
     * Gets the wrapped handle instance.
     *
     * @return The wrapped handle
     */
    public T getHandle() {
      return handle;
    }
  }
}
