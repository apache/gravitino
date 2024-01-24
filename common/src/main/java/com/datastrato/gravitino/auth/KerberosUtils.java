/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
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
import org.ietf.jgss.GSSException;
import org.ietf.jgss.Oid;

// Referred from Apache Hadoop KerberosTestUtils.java
// hadoop-common-project/hadoop-auth/src/test/java/org/apache/hadoop/security/\
// authentication/KerberosTestUtils.java
public class KerberosUtils {

  private KerberosUtils() {}

  public static final Oid GSS_SPNEGO_MECH_OID = getNumericOidInstance("1.3.6.1.5.5.2");
  public static final Oid GSS_KRB5_MECH_OID = getNumericOidInstance("1.2.840.113554.1.2.2");
  public static final Oid NT_GSS_KRB5_PRINCIPAL_OID =
      getNumericOidInstance("1.2.840.113554.1.2.2.1");

  // numeric oids will never generate a GSSException for a malformed oid.
  // use to initialize statics.
  private static Oid getNumericOidInstance(String oidName) {
    try {
      return new Oid(oidName);
    } catch (GSSException ex) {
      throw new IllegalArgumentException(ex);
    }
  }

  public static <T> T doAs(String principal, String keyTabFile, final Callable<T> callable)
      throws Exception {
    LoginContext loginContext = null;
    try {
      Set<Principal> principals = new HashSet<>();
      principals.add(new KerberosPrincipal(principal));
      Subject subject = new Subject(false, principals, new HashSet<>(), new HashSet<>());
      loginContext =
          new LoginContext("", subject, null, new KerberosConfiguration(principal, keyTabFile));
      loginContext.login();
      subject = loginContext.getSubject();
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
    } finally {
      if (loginContext != null) {
        loginContext.logout();
      }
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
