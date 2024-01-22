/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.client;

import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.auth.KerberosUtils;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import org.apache.commons.lang3.StringUtils;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KerberosTokenProvider will get Kerberos token using GSS context negotiation and then provide the
 * access token for every request.
 */
public final class KerberosTokenProvider implements AuthDataProvider {

  private static final Logger LOG = LoggerFactory.getLogger(KerberosTokenProvider.class);

  private final GSSManager gssManager = GSSManager.getInstance();
  private String clientPrincipal;
  private String keytabFile;
  private String host = "localhost";
  private LoginContext loginContext;
  private long reloginIntervalSec;
  private final ScheduledThreadPoolExecutor refreshScheduledExecutor =
      new ScheduledThreadPoolExecutor(1, getThreadFactory("kerberos-token-provider"));

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

  private synchronized byte[] getTokenInternal() throws Exception {

    String[] principalComponents = clientPrincipal.split("@");
    String serverPrincipal = "HTTP/" + host + "@" + principalComponents[1];

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

  private synchronized void refreshLoginContext() throws LoginException {
    if (loginContext != null) {
      loginContext.logout();
    }
    loginContext = KerberosUtils.login(clientPrincipal, keytabFile);
  }

  private void periodicallyRefreshLoginContext() {
    refreshScheduledExecutor.scheduleAtFixedRate(
        () -> {
          try {
            refreshLoginContext();
          } catch (LoginException e) {
            LOG.warn("Fail to re-login", e);
          }
        },
        reloginIntervalSec,
        reloginIntervalSec,
        TimeUnit.SECONDS);
  }

  /** Closes the KerberosTokenProvider and releases any underlying resources. */
  @Override
  public void close() throws IOException {
    try {
      refreshScheduledExecutor.shutdown();
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

  private static ThreadFactory getThreadFactory(String factoryName) {
    return new ThreadFactoryBuilder().setDaemon(true).setNameFormat(factoryName + "-%d").build();
  }

  /**
   * Creates a new instance of the KerberosTokenProvider.Builder
   *
   * @return A new instance of KerberosTokenProvider.Builder
   */
  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String clientPrincipal;
    private File keyTabFile;
    private long reloginIntervalSec = 5;

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
     * Kerberos need to re-login to keep credential active periodically.
     *
     * <p>Sets the re-login interval for KerberosTokenProvider. Default value is 5s.
     *
     * @param reloginIntervalSec The Kerberos re-login interval.
     * @return This Builder instance for method chaining.
     */
    public Builder withReloginIntervalSec(long reloginIntervalSec) {
      this.reloginIntervalSec = reloginIntervalSec;
      return this;
    }

    /**
     * Builds the instance of the KerberosTokenProvider.
     *
     * @return The built KerberosTokenProvider instance.
     */
    public KerberosTokenProvider build() {
      KerberosTokenProvider provider = new KerberosTokenProvider();

      Preconditions.checkArgument(
          StringUtils.isNotBlank(clientPrincipal),
          "KerberosTokenProvider must set clientPrincipal");
      Preconditions.checkArgument(
          clientPrincipal.split("@").length == 2, "Principal has the wrong format");
      provider.clientPrincipal = clientPrincipal;

      if (keyTabFile != null) {
        Preconditions.checkArgument(
            keyTabFile.exists(), "KerberosTokenProvider's keytabFile doesn't exist");
        Preconditions.checkArgument(
            keyTabFile.canRead(), "KerberosTokenProvider's keytabFile can't read");
        provider.keytabFile = keyTabFile.getAbsolutePath();
      }

      try {
        provider.refreshLoginContext();
      } catch (LoginException le) {
        throw new IllegalStateException("Fail to login", le);
      }

      provider.reloginIntervalSec = reloginIntervalSec;
      provider.periodicallyRefreshLoginContext();

      return provider;
    }
  }
}
