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
 * Unstable APIs, with no guarantee on stability and compatibility across any releases. Classes that
 * are unannotated are considered Unstable.
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
public @interface Unstable {}
