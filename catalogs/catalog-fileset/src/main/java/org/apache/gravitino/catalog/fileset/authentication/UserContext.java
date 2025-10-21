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
import static org.apache.gravitino.catalog.fileset.authentication.AuthenticationConfig.ENABLE_IMPERSONATION_ENTRY;
import static org.apache.gravitino.catalog.fileset.authentication.AuthenticationConfig.IMPERSONATION_ENABLE_KEY;
import static org.apache.gravitino.catalog.hadoop.fs.FileSystemProvider.GRAVITINO_BYPASS;

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
import org.apache.gravitino.catalog.hadoop.fs.FileSystemUtils;
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
      NameIdentifier nameIdentifier, Map<String, String> properties, CatalogInfo catalogInfo) {
    if (userContextMap.containsKey(nameIdentifier)) {
      return userContextMap.get(nameIdentifier);
    }

    Configuration configuration = FileSystemUtils.createConfiguration(GRAVITINO_BYPASS, properties);
    AuthenticationConfig authenticationConfig = new AuthenticationConfig(properties, configuration);

    // If we do not set the impersonation, we will use the parent context;
    boolean enableUserImpersonation = ENABLE_IMPERSONATION_ENTRY.getDefaultValue();
    if (properties.containsKey(IMPERSONATION_ENABLE_KEY)) {
      enableUserImpersonation = authenticationConfig.isImpersonationEnabled();
    }

    AuthenticationType authenticationType =
        authenticationConfig.isSimpleAuth()
            ? AuthenticationType.SIMPLE
            : AuthenticationType.KERBEROS;

    UserGroupInformation currentUser;
    try {
      currentUser = UserGroupInformation.getCurrentUser();
    } catch (IOException ioException) {
      throw new RuntimeException("Failed to get current user", ioException);
    }

    if (authenticationType == AuthenticationType.SIMPLE) {
      SimpleUserContext simpleUserContext =
          new SimpleUserContext(currentUser, enableUserImpersonation);
      addUserContext(nameIdentifier, simpleUserContext);
      return simpleUserContext;
    } else if (authenticationType == AuthenticationType.KERBEROS) {
      String keytabPath =
          String.format(
              GRAVITINO_KEYTAB_FORMAT,
              catalogInfo.id() + "-" + nameIdentifier.toString().replace(".", "-"));
      KerberosUserContext kerberosUserContext =
          new KerberosUserContext(enableUserImpersonation, keytabPath);
      kerberosUserContext.initKerberos(properties, configuration, true);
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
