/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.client;

import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.auth.KerberosUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import org.apache.commons.lang3.StringUtils;
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
  private LoginContext loginContext;

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
    // Gravitino server's principal must start with HTTP. This restriction follows
    // the style of Apache Hadoop.
    String serverPrincipal = "HTTP/" + host + "@" + principalComponents.get(1);

    synchronized (this) {
      if (loginContext == null) {
        loginContext = KerberosUtils.login(clientPrincipal, keytabFile);
      } else if (isLoginTicketExpired() && keytabFile != null) {
        // We only support use keytab to re-login context
        loginContext.logout();
        loginContext = KerberosUtils.login(clientPrincipal, keytabFile);
      }
    }

    return KerberosUtils.doAs(
        loginContext.getSubject(),
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

  @SuppressWarnings("JavaUtilDate")
  private boolean isLoginTicketExpired() {
    Set<KerberosTicket> tickets =
        loginContext.getSubject().getPrivateCredentials(KerberosTicket.class);

    if (tickets.isEmpty()) {
      return false;
    }

    return tickets.iterator().next().getEndTime().getTime() < System.currentTimeMillis();
  }

  /** Closes the KerberosTokenProvider and releases any underlying resources. */
  @Override
  public void close() throws IOException {
    try {
      if (loginContext != null) {
        loginContext.logout();
      }
    } catch (LoginException le) {
      throw new IOException("Fail to close login context", le);
    }
  }

  void setHost(String host) {
    this.host = host;
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
    @SuppressWarnings("null")
    public KerberosTokenProvider build() {
      KerberosTokenProvider provider = new KerberosTokenProvider();

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

      return provider;
    }
  }
}
