/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT;

import com.datastrato.gravitino.trino.connector.util.JsonCodec;
import io.trino.spi.TrinoException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

interface GravitinoHandle<T> {
  String HANDLE_STRING = "handleString";

  public String getHandleString();

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
            JsonCodec.instance(clazz.getClassLoader())
                .getMapper()
                .readerFor(clazz)
                .readValue(valueString);
        return new HandleWrapper<>(newHandle);
      } catch (Exception e) {
        throw new TrinoException(
            GRAVITINO_ILLEGAL_ARGUMENT, "Invalid handle string: " + valueString);
      }
    }

    public String toJson() {
      if (valueString == null) {
        try {
          valueString =
              JsonCodec.instance(clazz.getClassLoader())
                  .getMapper()
                  .writerFor(clazz)
                  .writeValueAsString(this.handle);
        } catch (Exception e) {
          throw new TrinoException(
              GRAVITINO_ILLEGAL_ARGUMENT, "Invalid handle string: " + valueString);
        }
      }
      return valueString;
    }

    public T getHandle() {
      return handle;
    }
  }
}
