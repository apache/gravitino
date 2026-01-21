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

package org.apache.gravitino.utils;

import com.google.common.base.Throwables;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.security.auth.Subject;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.AuthConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("removal")
public class PrincipalUtils {

  private static final Logger LOG = LoggerFactory.getLogger(PrincipalUtils.class);
  private static final Map<String, Pattern> PATTERN_CACHE = new ConcurrentHashMap<>();

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
    } catch (Error t) {
      LOG.warn("doAs method occurs an unexpected error", t);
      throw new RuntimeException("doAs method occurs an unexpected exception", t);
    }
  }

  // This method can't be used in nested `Subject#doAs` block.
  public static Principal getCurrentPrincipal() {
    java.security.AccessControlContext context = java.security.AccessController.getContext();
    Subject subject = Subject.getSubject(context);
    if (subject == null || subject.getPrincipals(UserPrincipal.class).isEmpty()) {
      return new UserPrincipal(AuthConstants.ANONYMOUS_USER);
    }

    return subject.getPrincipals(UserPrincipal.class).iterator().next();
  }

  public static String getCurrentUserName() {
    return getCurrentPrincipal().getName();
  }

  /**
   * Applies a user mapping pattern to extract the username from the principal. Similar to Trino's
   * http-server.authentication.oauth2.user-mapping.pattern.
   *
   * <p>The regex pattern is matched against the principal. If matched and the pattern contains at
   * least one capturing group, the username is replaced with the first regex group. If the pattern
   * matches but has no capturing groups, the original principal is returned. If the pattern doesn't
   * match at all, the original principal is returned.
   *
   * @param principal the principal name to apply the pattern to
   * @param patternStr the regex pattern string. If null or empty, returns the original principal.
   *     The default pattern "(.*)" allows any username.
   * @return the extracted username from the first capturing group, or the original principal if no
   *     match or no capturing group
   * @throws IllegalArgumentException if the pattern string is invalid regex syntax
   */
  public static String applyUserMappingPattern(String principal, String patternStr) {
    if (principal == null) {
      return null;
    }

    if (patternStr == null || patternStr.isEmpty()) {
      return principal;
    }

    try {
      Pattern pattern = PATTERN_CACHE.computeIfAbsent(patternStr, Pattern::compile);
      Matcher matcher = pattern.matcher(principal);

      // If pattern matches and has at least one capturing group, return the first group
      if (matcher.find() && matcher.groupCount() >= 1) {
        String extracted = matcher.group(1);
        // Return extracted value if it's not null and not empty, otherwise original principal
        return (extracted != null && !extracted.isEmpty()) ? extracted : principal;
      }

      // If pattern doesn't match or has no capturing groups, return original principal
      return principal;
    } catch (Exception e) {
      LOG.error("Invalid user mapping pattern: {}", patternStr, e);
      throw new IllegalArgumentException("Invalid user mapping pattern: " + patternStr, e);
    }
  }
}
