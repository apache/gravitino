/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational.po;

import com.google.common.base.Objects;

public class FilesetMaxVersionPO {
  private Long filesetId;
  private Long version;

  public Long getFilesetId() {
    return filesetId;
  }

  public Long getVersion() {
    return version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof FilesetMaxVersionPO)) return false;
    FilesetMaxVersionPO that = (FilesetMaxVersionPO) o;
    return Objects.equal(getFilesetId(), that.getFilesetId())
        && Objects.equal(getVersion(), that.getVersion());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getFilesetId(), getVersion());
  }
}
