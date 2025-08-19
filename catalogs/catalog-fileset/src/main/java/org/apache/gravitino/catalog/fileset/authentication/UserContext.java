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

package org.apache.gravitino.catalog.fileset.authentication;

import static org.apache.gravitino.catalog.fileset.SecureFilesetCatalogOperations.GRAVITINO_KEYTAB_FORMAT;
import static org.apache.gravitino.catalog.fileset.authentication.AuthenticationConfig.AUTH_TYPE_ENTRY;
import static org.apache.gravitino.catalog.fileset.authentication.AuthenticationConfig.ENABLE_IMPERSONATION_ENTRY;
import static org.apache.gravitino.catalog.fileset.authentication.AuthenticationConfig.IMPERSONATION_ENABLE_KEY;

import com.google.common.collect.Maps;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.fileset.authentication.AuthenticationConfig.AuthenticationType;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

public abstract class UserContext implements Closeable {

  private static final Map<NameIdentifier, UserContext> userContextMap = Maps.newConcurrentMap();

  private static void addUserContext(NameIdentifier nameIdentifier, UserContext userContext) {
    userContextMap.put(nameIdentifier, userContext);
  }

  public static void clearUserContext(NameIdentifier nameIdentifier) {
    UserContext userContext = userContextMap.remove(nameIdentifier);
    if (userContext != null) {
      try {
        userContext.close();
      } catch (IOException e) {
        throw new RuntimeException("Failed to close user context", e);
      }
    }
  }

  public static void cleanAllUserContext() {
    userContextMap.keySet().forEach(UserContext::clearUserContext);
    userContextMap.clear();
  }

  public static UserContext getUserContext(
      NameIdentifier nameIdentifier,
      Map<String, String> properties,
      Configuration configuration,
      CatalogInfo catalogInfo) {
    // Try to get the parent user context.
    NameIdentifier currentNameIdentifier = NameIdentifier.of(nameIdentifier.namespace().levels());
    UserContext parentContext = null;
    while (!currentNameIdentifier.namespace().isEmpty()) {
      if (userContextMap.containsKey(currentNameIdentifier)) {
        parentContext = userContextMap.get(currentNameIdentifier);
        break;
      }
      currentNameIdentifier = NameIdentifier.of(currentNameIdentifier.namespace().levels());
    }

    if (configuration == null) {
      configuration = new Configuration();
    }
    AuthenticationConfig authenticationConfig = new AuthenticationConfig(properties);

    // If we do not set the impersonation, we will use the parent context;
    boolean enableUserImpersonation = ENABLE_IMPERSONATION_ENTRY.getDefaultValue();
    if (properties.containsKey(IMPERSONATION_ENABLE_KEY)) {
      enableUserImpersonation = authenticationConfig.isImpersonationEnabled();
    } else if (parentContext != null) {
      enableUserImpersonation = parentContext.enableUserImpersonation();
    }

    AuthenticationType authenticationType =
        AuthenticationType.fromString(AUTH_TYPE_ENTRY.getDefaultValue());
    // If we do not set the authentication type explicitly, we will use the parent context. If the
    // parent is null, then we will use the default value.
    if (properties.containsKey(AuthenticationConfig.AUTH_TYPE_KEY)) {
      authenticationType =
          authenticationConfig.isSimpleAuth()
              ? AuthenticationType.SIMPLE
              : AuthenticationType.KERBEROS;

    } else if (parentContext != null) {
      authenticationType =
          parentContext instanceof SimpleUserContext
              ? AuthenticationType.SIMPLE
              : AuthenticationType.KERBEROS;
    }

    UserGroupInformation currentUser;
    try {
      currentUser = UserGroupInformation.getCurrentUser();
    } catch (IOException ioException) {
      throw new RuntimeException("Failed to get current user", ioException);
    }

    if (authenticationType == AuthenticationType.SIMPLE) {
      UserGroupInformation userGroupInformation =
          parentContext != null ? parentContext.getUser() : currentUser;
      SimpleUserContext simpleUserContext =
          new SimpleUserContext(userGroupInformation, enableUserImpersonation);
      addUserContext(nameIdentifier, simpleUserContext);
      return simpleUserContext;
    } else if (authenticationType == AuthenticationType.KERBEROS) {
      // if the kerberos authentication is inherited from the parent context, we will use the
      // parent context's kerberos configuration.
      if (parentContext != null && authenticationConfig.isSimpleAuth()) {
        KerberosUserContext kerberosUserContext = ((KerberosUserContext) parentContext).deepCopy();
        kerberosUserContext.setEnableUserImpersonation(enableUserImpersonation);
        addUserContext(nameIdentifier, kerberosUserContext);
        return kerberosUserContext;
      }

      String keytabPath =
          String.format(
              GRAVITINO_KEYTAB_FORMAT,
              catalogInfo.id() + "-" + nameIdentifier.toString().replace(".", "-"));
      KerberosUserContext kerberosUserContext =
          new KerberosUserContext(enableUserImpersonation, keytabPath);
      kerberosUserContext.initKerberos(properties, configuration, parentContext == null);
      addUserContext(nameIdentifier, kerberosUserContext);
      return kerberosUserContext;
    } else {
      throw new RuntimeException("Unsupported authentication type: " + authenticationType);
    }
  }

  abstract UserGroupInformation getUser();

  abstract boolean enableUserImpersonation();

  abstract UserGroupInformation createProxyUser();

  public <T> T doAs(PrivilegedExceptionAction<T> action, NameIdentifier ident) {
    UserGroupInformation u = getUser();
    if (enableUserImpersonation()) {
      u = createProxyUser();
    }

    try {
      return u.doAs(action);
    } catch (IOException | InterruptedException ioe) {
      throw new RuntimeException("Failed to operation on entity:" + ident, ioe);
    } catch (UndeclaredThrowableException e) {
      Throwable innerException = e.getCause();
      if (innerException instanceof PrivilegedActionException) {
        throw new RuntimeException(innerException.getCause());
      } else if (innerException instanceof InvocationTargetException) {
        throw new RuntimeException(innerException.getCause());
      } else {
        throw new RuntimeException(innerException);
      }
    }
  }
}
