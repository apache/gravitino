/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.meta.rel.transforms;

import com.datastrato.graviton.rel.Transform;

/**
 * A reference transform is a transform that references a field or a {@link
 * io.substrait.expression.Expression.Literal}.
 *
 * @param <T> the representation of the reference.
 */
public interface RefTransform<T> extends Transform {
  Transform[] EMPTY_ARGS = new Transform[0];

  T value();

  default Transform[] arguments() {
    return EMPTY_ARGS;
  }
}
