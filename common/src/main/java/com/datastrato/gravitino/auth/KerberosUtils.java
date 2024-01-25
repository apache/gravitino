/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastrato.gravitino.auth;

import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.Oid;

// Referred from Apache Hadoop KerberosTestUtils.java
// Remove the part methods
// Remove the support the IBM JDK
// Split method `doAs` into two methods
// hadoop-common-project/hadoop-auth/src/test/java/org/apache/hadoop/security/\
// authentication/KerberosTestUtils.java
public class KerberosUtils {

  private KerberosUtils() {}

  public static final Oid GSS_SPNEGO_MECH_OID = getNumericOidInstance("1.3.6.1.5.5.2");
  public static final Oid GSS_KRB5_MECH_OID = getNumericOidInstance("1.2.840.113554.1.2.2");
  public static final Oid NT_GSS_KRB5_PRINCIPAL_OID =
      getNumericOidInstance("1.2.840.113554.1.2.2.1");

  // Numeric oids will never generate a GSSException for a malformed oid.
  // Use to initialize statics.
  private static Oid getNumericOidInstance(String oidName) {
    try {
      return new Oid(oidName);
    } catch (GSSException ex) {
      throw new IllegalArgumentException(ex);
    }
  }

  public static LoginContext login(String principal, String keyTabFile) throws LoginException {
    LoginContext loginContext = null;
    Set<Principal> principals = new HashSet<>();
    principals.add(new KerberosPrincipal(principal));
    Subject subject = new Subject(false, principals, new HashSet<>(), new HashSet<>());
    loginContext =
        new LoginContext("", subject, null, new KerberosConfiguration(principal, keyTabFile));
    loginContext.login();
    return loginContext;
  }

  public static <T> T doAs(Subject subject, final Callable<T> callable) throws Exception {
    try {
      return Subject.doAs(
          subject,
          new PrivilegedExceptionAction<T>() {
            @Override
            public T run() throws Exception {
              return callable.call();
            }
          });
    } catch (PrivilegedActionException ex) {
      throw ex.getException();
    }
  }

  private static class KerberosConfiguration extends Configuration {
    private final String principal;
    private final String keyTabFile;

    public KerberosConfiguration(String principal, String keyTabFile) {
      this.principal = principal;
      this.keyTabFile = keyTabFile;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      Map<String, String> options = new HashMap<String, String>();

      options.put("keyTab", keyTabFile);
      options.put("principal", principal);
      options.put("useKeyTab", "true");
      options.put("storeKey", "true");
      options.put("doNotPrompt", "true");
      options.put("useTicketCache", "true");
      options.put("renewTGT", "true");
      options.put("refreshKrb5Config", "true");
      options.put("isInitiator", "true");
      String ticketCache = System.getenv("KRB5CCNAME");
      if (ticketCache != null) {
        options.put("ticketCache", ticketCache);
      }
      options.put("debug", "true");

      return new AppConfigurationEntry[] {
        new AppConfigurationEntry(
            getKrb5LoginModuleName(),
            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
            options),
      };
    }
  }

  /* Return the Kerberos login module name */
  public static String getKrb5LoginModuleName() {
    return "com.sun.security.auth.module.Krb5LoginModule";
  }
}
