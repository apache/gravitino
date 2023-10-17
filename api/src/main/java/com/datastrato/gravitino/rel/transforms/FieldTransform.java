/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.transforms;

/** A FieldTransform is a transform that references a Field. */
public abstract class FieldTransform implements RefTransform<String[]> {

  private static final String FIELD_REFERENCE = "field_reference";

  @Override
  public String name() {
    return FIELD_REFERENCE;
  }
}
