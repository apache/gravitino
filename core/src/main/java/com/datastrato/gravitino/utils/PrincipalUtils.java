/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.utils;

import com.datastrato.gravitino.UserPrincipal;
import com.datastrato.gravitino.auth.AuthConstants;
import com.google.common.base.Throwables;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import javax.security.auth.Subject;

@SuppressWarnings("removal")
public class PrincipalUtils {
  private PrincipalUtils() {}

  public static <T> T doAs(Principal principal, PrivilegedExceptionAction<T> action)
      throws Exception {
    try {
      Subject subject = new Subject();
      subject.getPrincipals().add(principal);
      return Subject.doAs(subject, action);
    } catch (PrivilegedActionException pae) {
      Throwable cause = pae.getCause();
      Throwables.propagateIfPossible(cause, Exception.class);
      throw new RuntimeException("doAs method occurs an unexpected exception", pae);
    }
  }

  public static Principal getCurrentPrincipal() {
    java.security.AccessControlContext context = java.security.AccessController.getContext();
    Subject subject = Subject.getSubject(context);
    if (subject == null || subject.getPrincipals(UserPrincipal.class).isEmpty()) {
      return new UserPrincipal(AuthConstants.ANONYMOUS_USER);
    }
    return subject.getPrincipals(UserPrincipal.class).iterator().next();
  }
}
