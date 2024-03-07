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
 * A {@link DeveloperApi} is the developer-facing API defined in Gravitino. These APIs are intended
 * for developers, and may be changed or removed in minor versions of Gravitino.
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
public @interface DeveloperApi {}
