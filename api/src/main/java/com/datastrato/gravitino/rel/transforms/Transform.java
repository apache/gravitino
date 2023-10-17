/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel.transforms;

/**
 * Used for partitioning.
 *
 * <p>For example, the transform toYYYYMM(dt) is used to derive a date value from a Date type field.
 * The transform name is "toYYYYMM" and its argument is a reference to the "dt" field.
 */
public interface Transform {

  String name();

  Transform[] arguments();
}
