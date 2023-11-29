/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.kv;

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
