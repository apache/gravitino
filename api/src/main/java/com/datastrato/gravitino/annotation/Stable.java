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
 * APIs that can evolve while retaining compatibility for feature releases (0.5.0 to 0.6.0). The
 * compatibility can be broken only in major releases (1.x to 2.x).
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
public @interface Stable {

  /** @return The version when the API was first marked stable. */
  String since();
}
