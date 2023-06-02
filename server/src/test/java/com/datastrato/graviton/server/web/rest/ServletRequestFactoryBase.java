package com.datastrato.graviton.server.web.rest;

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
