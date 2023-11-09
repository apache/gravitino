/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.kv;

public enum ValueStatusEnum {
  // The value is normal.
  NORMAL(0),

  // The value has been deleted.
  DELETED(1);

  private final int code;

  ValueStatusEnum(int code) {
    this.code = code;
  }

  public int getCode() {
    return code;
  }

  public static ValueStatusEnum fromCode(int code) {
    for (ValueStatusEnum valueStatusEnum : ValueStatusEnum.values()) {
      if (valueStatusEnum.getCode() == code) {
        return valueStatusEnum;
      }
    }
    throw new IllegalArgumentException("Invalid code: " + code);
  }
}
