/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * APIs that are still evolving towards becoming stable APIs, and can change from one feature
 * release to another (0.5.0 to 0.6.0).
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({
  ElementType.PACKAGE,
  ElementType.CONSTRUCTOR,
  ElementType.METHOD,
  ElementType.TYPE,
  ElementType.FIELD
})
public @interface Evolving {}
