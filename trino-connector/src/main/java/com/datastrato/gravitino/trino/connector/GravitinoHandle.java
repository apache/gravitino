/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector;

import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT;

import com.datastrato.gravitino.trino.connector.util.JsonCodec;
import io.trino.spi.TrinoException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

interface GravitinoHandle<T> {
  String HANDLE_STRING = "handleString";

  public String getHandleString();

  public T getInternalHandle();

  public static <S, T> T wrap(S handle) {
    return (T) ((GravitinoHandle) handle).getInternalHandle();
  }

  public static <S, T> T unWrap(S handle) {
    return (T) ((GravitinoHandle) handle).getInternalHandle();
  }

  public static <S, T> List<T> unWrap(List<S> handles) {
    return handles.stream()
        .map(handle -> (T) (((GravitinoHandle) handle).getInternalHandle()))
        .collect(Collectors.toList());
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
