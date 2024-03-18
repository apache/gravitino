/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.connector.BaseFileset;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
public class TestFileset extends BaseFileset {

  public static class Builder extends BaseFilesetBuilder<Builder, TestFileset> {
    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    @Override
    protected TestFileset internalBuild() {
      TestFileset fileset = new TestFileset();
      fileset.name = name;
      fileset.comment = comment;
      fileset.properties = properties;
      fileset.auditInfo = auditInfo;
      fileset.type = type;
      fileset.storageLocation = storageLocation;
      return fileset;
    }
  }

  /**
   * Creates a new instance of {@link Builder}.
   *
   * @return The new instance.
   */
  public static Builder builder() {
    return new Builder();
  }
}
