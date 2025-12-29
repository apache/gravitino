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

package org.apache.gravitino.client;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosKey;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.auth.KerberosUtils;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

/**
 * KerberosTokenProvider will get Kerberos token using GSS context negotiation and then provide the
 * access token for every request.
 */
public final class KerberosTokenProvider implements AuthDataProvider {

  private final GSSManager gssManager = GSSManager.getInstance();
  private String clientPrincipal;
  private String keytabFile;
  private String host = "localhost";
  private SubjectProvider subjectProvider;

  private KerberosTokenProvider() {}

  /**
   * Judge whether AuthDataProvider can provide token data.
   *
   * @return true if the AuthDataProvider can provide token data otherwise false.
   */
  @Override
  public boolean hasTokenData() {
    return true;
  }

  /**
   * Acquire the data of token for authentication. The client will set the token data as HTTP header
   * Authorization directly. So the return value should ensure token data contain the token header
   * (eg: Bearer, Basic) if necessary.
   *
   * @return the token data is used for authentication.
   */
  @Override
  public byte[] getTokenData() {
    try {
      return getTokenInternal();
    } catch (Exception e) {
      throw new IllegalStateException("Fail to get the Kerberos token", e);
    }
  }

  private byte[] getTokenInternal() throws Exception {
    @SuppressWarnings("null")
    List<String> principalComponents = Splitter.on('@').splitToList(clientPrincipal);
    String serverPrincipal = "HTTP/" + host + "@" + principalComponents.get(1);
    Subject currentSubject = subjectProvider.get();

    for (Principal principal : currentSubject.getPrincipals()) {
      System.out.println("Principal: " + principal.getName());
    }
    for (KerberosTicket ticket : currentSubject.getPrivateCredentials(KerberosTicket.class)) {
      System.out.println("Ticket: " + ticket.getServer().getName());
    }
    for (KerberosKey key : currentSubject.getPrivateCredentials(KerberosKey.class)) {
      System.out.println("Key: " + key.getPrincipal().getName());
    }

    return KerberosUtils.doAs(
        currentSubject,
        new Callable<byte[]>() {
          @Override
          public byte[] call() throws Exception {
            GSSContext gssContext = null;
            try {
              Oid oid = KerberosUtils.NT_GSS_KRB5_PRINCIPAL_OID;
              GSSName serviceName = gssManager.createName(serverPrincipal, oid);

              oid = KerberosUtils.GSS_KRB5_MECH_OID;
              gssContext =
                  gssManager.createContext(serviceName, oid, null, GSSContext.DEFAULT_LIFETIME);
              gssContext.requestCredDeleg(true);
              gssContext.requestMutualAuth(true);

              byte[] inToken = new byte[0];
              byte[] outToken = gssContext.initSecContext(inToken, 0, inToken.length);
              return (AuthConstants.AUTHORIZATION_NEGOTIATE_HEADER
                      + Base64.getEncoder().encodeToString(outToken))
                  .getBytes(StandardCharsets.UTF_8);

            } finally {
              if (gssContext != null) {
                gssContext.dispose();
              }
            }
          }
        });
  }

  @Override
  public void close() throws IOException {
    try {
      if (subjectProvider != null) {
        subjectProvider.close();
      }
    } catch (LoginException le) {
      throw new IOException("Fail to close login context", le);
    }
  }

  void setHost(String host) {
    this.host = host;
  }

  private interface SubjectProvider {
    Subject get() throws LoginException;

    void close() throws LoginException;
  }

  private static final class ExistingSubjectProvider implements SubjectProvider {
    private final Subject subject;

    ExistingSubjectProvider(Subject subject) {
      this.subject = subject;
    }

    @Override
    public Subject get() {
      return subject;
    }

    @Override
    public void close() {
      // no-op
    }
  }

  private static final class LoginSubjectProvider implements SubjectProvider {
    private final String principal;
    private final String keytabFile;
    private LoginContext loginContext;

    LoginSubjectProvider(String principal, String keytabFile) {
      this.principal = principal;
      this.keytabFile = keytabFile;
    }

    @Override
    public synchronized Subject get() throws LoginException {
      if (loginContext == null) {
        loginContext = KerberosUtils.login(principal, keytabFile);
      } else if (keytabFile != null && isLoginTicketExpired(loginContext)) {
        loginContext.logout();
        loginContext = KerberosUtils.login(principal, keytabFile);
      }
      return loginContext.getSubject();
    }

    @Override
    public void close() throws LoginException {
      if (loginContext != null) {
        loginContext.logout();
      }
    }

    private boolean isLoginTicketExpired(LoginContext ctx) {
      Set<KerberosTicket> tickets = ctx.getSubject().getPrivateCredentials(KerberosTicket.class);
      if (tickets.isEmpty()) {
        return false;
      }
      return tickets.iterator().next().getEndTime().toInstant().isBefore(Instant.now());
    }
  }

  /**
   * Creates a new instance of the KerberosTokenProvider.Builder
   *
   * @return A new instance of KerberosTokenProvider.Builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder class for configuring and creating instances of KerberosTokenProvider. */
  public static class Builder {
    private String clientPrincipal;
    private File keyTabFile;

    /**
     * Sets the client principal for the HTTP token requests.
     *
     * @param clientPrincipal The client principal for the HTTP token requests.
     * @return This Builder instance for method chaining.
     */
    public Builder withClientPrincipal(String clientPrincipal) {
      this.clientPrincipal = clientPrincipal;
      return this;
    }

    /**
     * Sets the keyTabFile for the HTTP token requests.
     *
     * @param file The keyTabFile for the HTTP token requests.
     * @return This Builder instance for method chaining.
     */
    public Builder withKeyTabFile(File file) {
      this.keyTabFile = file;
      return this;
    }

    /**
     * Builds the instance of the KerberosTokenProvider.
     *
     * @return The built KerberosTokenProvider instance.
     */
    @SuppressWarnings("removal")
    public KerberosTokenProvider build() {
      KerberosTokenProvider provider = new KerberosTokenProvider();

      java.security.AccessControlContext context = java.security.AccessController.getContext();
      Subject subject = Subject.getSubject(context);
      if (subject != null
          && (!subject.getPrivateCredentials(KerberosKey.class).isEmpty()
              || !subject.getPrivateCredentials(KerberosTicket.class).isEmpty())) {
        provider.subjectProvider = new ExistingSubjectProvider(subject);
        provider.clientPrincipal = extractPrincipalFromSubject(subject);
        return provider;
      }

      Preconditions.checkArgument(
          StringUtils.isNotBlank(clientPrincipal),
          "KerberosTokenProvider must set clientPrincipal");
      Preconditions.checkArgument(
          Splitter.on('@').splitToList(clientPrincipal).size() == 2,
          "Principal has the wrong format");
      provider.clientPrincipal = clientPrincipal;

      if (keyTabFile != null) {
        Preconditions.checkArgument(
            keyTabFile.exists(), "KerberosTokenProvider's keytabFile doesn't exist");
        Preconditions.checkArgument(
            keyTabFile.canRead(), "KerberosTokenProvider's keytabFile can't read");
        provider.keytabFile = keyTabFile.getAbsolutePath();
      }

      provider.subjectProvider =
          new LoginSubjectProvider(provider.clientPrincipal, provider.keytabFile);
      return provider;
    }

    private String extractPrincipalFromSubject(Subject subject) {
      return subject.getPrincipals(KerberosPrincipal.class).stream()
          .findFirst()
          .map(Object::toString)
          .orElse(null);
    }
  }
}
