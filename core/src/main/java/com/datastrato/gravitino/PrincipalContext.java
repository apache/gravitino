/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino;

import com.datastrato.gravitino.auth.AuthConstants;
import org.apache.commons.lang3.StringUtils;

public class PrincipalContext implements AutoCloseable {

  private String currentUser;

  private static final ThreadLocal<PrincipalContext> context = new ThreadLocal<>();

  public static PrincipalContext get() {
    return context.get();
  }

  public String getCurrentUser() {
    if (StringUtils.isBlank(currentUser)) {
      return AuthConstants.ANONYMOUS_USER;
    }
    return currentUser;
  }

  public static PrincipalContext createPrincipalContext(String currentUser) {
    PrincipalContext principalContext = new PrincipalContext();
    principalContext.currentUser = currentUser;
    context.set(principalContext);
    return principalContext;
  }

  @Override
  public void close() throws Exception {
    context.remove();
  }
}
