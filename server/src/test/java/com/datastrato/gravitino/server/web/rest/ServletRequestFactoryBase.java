/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import java.util.function.Supplier;
import javax.servlet.http.HttpServletRequest;
import org.glassfish.hk2.api.Factory;

abstract class ServletRequestFactoryBase
    implements Factory<HttpServletRequest>, Supplier<HttpServletRequest> {

  @Override
  public HttpServletRequest provide() {
    return get();
  }

  @Override
  public void dispose(HttpServletRequest instance) {}
}
