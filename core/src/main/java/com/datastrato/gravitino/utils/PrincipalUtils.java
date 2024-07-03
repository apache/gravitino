/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
    if (subject == null) {
      return new UserPrincipal(AuthConstants.ANONYMOUS_USER);
    }

    if (!subject.getPrincipals(UserPrincipal.class).isEmpty()) {
      return subject.getPrincipals(UserPrincipal.class).iterator().next();
    }

    if (!subject.getPrincipals().isEmpty()) {
      return new UserPrincipal(subject.getPrincipals().iterator().next().getName());
    }

    return new UserPrincipal(AuthConstants.ANONYMOUS_USER);
  }

  public static String getCurrentUserName() {
    return getCurrentPrincipal().getName();
  }
}
