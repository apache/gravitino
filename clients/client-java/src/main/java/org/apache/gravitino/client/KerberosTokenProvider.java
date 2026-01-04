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

  /**
   * Strategy interface for providing Kerberos Subject credentials.
   *
   * <p>There are two strategies:
   *
   * <ul>
   *   <li>{@link ExistingSubjectProvider} - Reuses credentials maintained by the framework (e.g.,
   *       Flink)
   *   <li>{@link LoginSubjectProvider} - Creates and manages new credentials using keytab and
   *       principal
   * </ul>
   */
  private interface SubjectProvider {
    Subject get() throws LoginException;

    void close() throws LoginException;
  }

  /**
   * SubjectProvider that reuses existing Kerberos credentials from the framework.
   *
   * <p>When Flink (or another framework) has already logged in using keytab and principal, this
   * provider reuses those credentials instead of creating new ones. This approach:
   *
   * <ul>
   *   <li>Avoids redundant Kerberos logins
   *   <li>Lets the framework manage credential lifecycle (renewal, expiration)
   *   <li>Reduces complexity - the client doesn't need to maintain credentials
   * </ul>
   *
   * <p>The Subject is obtained from the current AccessControlContext and contains KerberosKey or
   * KerberosTicket credentials managed by the framework.
   */
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
      // no-op: The framework owns the Subject and is responsible for its lifecycle
    }
  }

  /**
   * SubjectProvider that performs Kerberos login using keytab and principal.
   *
   * <p>This provider is used when no existing Kerberos credentials are available from the
   * framework. It:
   *
   * <ul>
   *   <li>Performs initial login using the provided keytab file and principal
   *   <li>Manages the LoginContext lifecycle
   *   <li>Automatically re-authenticates when the TGT (Ticket Granting Ticket) expires
   * </ul>
   *
   * <p>This provider is responsible for credential lifecycle management including renewal and
   * cleanup.
   */
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
      // Perform initial login if not already logged in
      if (loginContext == null) {
        loginContext = KerberosUtils.login(principal, keytabFile);
      } else if (keytabFile != null && isLoginTicketExpired(loginContext)) {
        // If the TGT (Ticket Granting Ticket) has expired, logout and re-authenticate
        // This ensures we always have valid credentials for GSS context negotiation
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
      // For one principal, there should be only one TGT ticket
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
     * <p>This method determines the authentication strategy:
     *
     * <ul>
     *   <li>If Kerberos credentials already exist in the current context (e.g., Flink has logged
     *       in), use {@link ExistingSubjectProvider} to reuse those credentials
     *   <li>Otherwise, use {@link LoginSubjectProvider} to perform a new Kerberos login using the
     *       provided keytab and principal
     * </ul>
     *
     * @return The built KerberosTokenProvider instance.
     */
    @SuppressWarnings("removal")
    public KerberosTokenProvider build() {
      KerberosTokenProvider provider = new KerberosTokenProvider();

      // Check if the framework (e.g., Flink) has already established Kerberos credentials
      java.security.AccessControlContext context = java.security.AccessController.getContext();
      Subject subject = Subject.getSubject(context);

      // If credentials exist (KerberosKey or KerberosTicket), reuse them
      // This avoids redundant logins when Flink has already authenticated with Kerberos
      if (subject != null
          && (!subject.getPrivateCredentials(KerberosKey.class).isEmpty()
              || !subject.getPrivateCredentials(KerberosTicket.class).isEmpty())) {
        // Use ExistingSubjectProvider: framework manages the credentials
        provider.subjectProvider = new ExistingSubjectProvider(subject);

        extractPrincipalFromSubject(subject);
        setProviderClientPrincipal(provider);

        return provider;
      }

      // No existing credentials found - we need to login ourselves
      setProviderClientPrincipal(provider);

      if (keyTabFile != null) {
        Preconditions.checkArgument(
            keyTabFile.exists(), "KerberosTokenProvider's keytabFile doesn't exist");
        Preconditions.checkArgument(
            keyTabFile.canRead(), "KerberosTokenProvider's keytabFile can't read");
        provider.keytabFile = keyTabFile.getAbsolutePath();
      }

      // Use LoginSubjectProvider: client manages the credentials
      // This provider will login using keytab/principal and handle ticket renewal
      provider.subjectProvider =
          new LoginSubjectProvider(provider.clientPrincipal, provider.keytabFile);
      return provider;
    }

    private void setProviderClientPrincipal(KerberosTokenProvider provider) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(clientPrincipal),
          "KerberosTokenProvider must set clientPrincipal");
      Preconditions.checkArgument(
          Splitter.on('@').splitToList(clientPrincipal).size() == 2,
          "Principal has the wrong format");
      provider.clientPrincipal = clientPrincipal;
    }

    private void extractPrincipalFromSubject(Subject subject) {
      clientPrincipal =
          subject.getPrincipals(KerberosPrincipal.class).stream()
              .findFirst()
              .map(Object::toString)
              .orElse(null);
    }
  }
}
