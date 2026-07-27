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

package org.apache.gravitino.catalog.hadoop.auth;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.Nullable;
import org.apache.gravitino.catalog.hadoop.auth.KerberosAuthUtils.LoginMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared keytab-based Kerberos login client for Hadoop-aware Gravitino modules.
 *
 * <p>Handles the common orchestration — validate principal, optionally point the JVM at a
 * krb5.conf, log in from a keytab, and (optionally) schedule a background TGT refresh. Per-module
 * differences (login mode, refresh on/off, thread name, krb5.conf wiring) are supplied through
 * {@link Builder}.
 */
public class KerberosClient implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(KerberosClient.class);

  private final String principal;
  private final Configuration hadoopConf;
  private final LoginMode loginMode;
  private final boolean refreshCredentials;
  private final int checkIntervalSec;
  @Nullable private final String hadoopKrb5ConfKey;
  @Nullable private final String systemKrb5ConfKey;

  private ScheduledExecutorService checkTgtRefreshExecutor;
  private UserGroupInformation loginUser;
  private String realm;

  private KerberosClient(Builder builder) {
    this.principal = builder.principal;
    this.hadoopConf = builder.hadoopConf;
    this.loginMode = builder.loginMode;
    this.refreshCredentials = builder.refreshCredentials;
    this.checkIntervalSec = builder.checkIntervalSec;
    this.hadoopKrb5ConfKey = builder.hadoopKrb5ConfKey;
    this.systemKrb5ConfKey = builder.systemKrb5ConfKey;
  }

  /**
   * Logs in from the given keytab and, when configured, starts a background TGT refresh.
   *
   * @param keytabFilePath local keytab file path
   * @return the resulting login UGI (also available via {@link #getLoginUser()})
   * @throws IOException if login fails
   */
  public UserGroupInformation login(String keytabFilePath) throws IOException {
    this.realm = KerberosAuthUtils.checkPrincipalAndGetRealm(principal);
    if (hadoopKrb5ConfKey != null) {
      KerberosAuthUtils.configureKrb5Conf(hadoopConf, hadoopKrb5ConfKey, systemKrb5ConfKey);
    }
    this.loginUser = KerberosAuthUtils.login(principal, keytabFilePath, hadoopConf, loginMode);
    if (refreshCredentials) {
      shutdownTicketRefresh();
      this.checkTgtRefreshExecutor =
          KerberosAuthUtils.startTicketRefresh(loginUser, checkIntervalSec, LOG);
    }
    return loginUser;
  }

  /** Returns the realm parsed from the principal, available after {@link #login(String)}. */
  public String getRealm() {
    Preconditions.checkState(realm != null, "login() has not been called");
    return realm;
  }

  /** Returns the login UGI, available after {@link #login(String)}. */
  public UserGroupInformation getLoginUser() {
    Preconditions.checkState(loginUser != null, "login() has not been called");
    return loginUser;
  }

  @Override
  public void close() {
    shutdownTicketRefresh();
  }

  private void shutdownTicketRefresh() {
    if (checkTgtRefreshExecutor != null) {
      // Graceful shutdown: let any in-flight relogin finish before the thread is torn down.
      checkTgtRefreshExecutor.shutdown();
      checkTgtRefreshExecutor = null;
    }
  }

  /**
   * Creates a builder for a client logging in as {@code principal}.
   *
   * @param principal Kerberos principal in {@code name@REALM} format
   * @param hadoopConf Hadoop configuration
   * @return a new builder
   */
  public static Builder builder(String principal, Configuration hadoopConf) {
    return new Builder(principal, hadoopConf);
  }

  /** Builder for {@link KerberosClient}. */
  public static final class Builder {
    private final String principal;
    private final Configuration hadoopConf;
    private LoginMode loginMode = LoginMode.CURRENT_USER;
    private boolean refreshCredentials = true;
    private int checkIntervalSec = 60;
    private String hadoopKrb5ConfKey;
    private String systemKrb5ConfKey;

    private Builder(String principal, Configuration hadoopConf) {
      this.principal = principal;
      this.hadoopConf = hadoopConf;
    }

    /** Sets the login mode (defaults to {@link LoginMode#CURRENT_USER}). */
    public Builder loginMode(LoginMode mode) {
      this.loginMode = mode;
      return this;
    }

    /** Enables or disables the background TGT refresh (defaults to enabled). */
    public Builder refreshCredentials(boolean refresh) {
      this.refreshCredentials = refresh;
      return this;
    }

    /** Sets the TGT refresh interval in seconds (used only when refresh is enabled). */
    public Builder checkIntervalSec(int seconds) {
      this.checkIntervalSec = seconds;
      return this;
    }

    /**
     * Points the JVM krb5.conf system property at the path stored under {@code hadoopKrb5ConfKey}
     * in the Hadoop configuration before login. When not set, the JVM krb5.conf is left untouched.
     */
    public Builder krb5Conf(String hadoopKrb5ConfKey, String systemKrb5ConfKey) {
      this.hadoopKrb5ConfKey = hadoopKrb5ConfKey;
      this.systemKrb5ConfKey = systemKrb5ConfKey;
      return this;
    }

    /** Builds the client. */
    public KerberosClient build() {
      Preconditions.checkArgument(
          principal != null && !principal.trim().isEmpty(), "principal can't be blank");
      Preconditions.checkArgument(hadoopConf != null, "hadoopConf can't be null");
      Preconditions.checkArgument(loginMode != null, "loginMode can't be null");
      if (refreshCredentials) {
        Preconditions.checkArgument(checkIntervalSec > 0, "checkIntervalSec must be positive");
      }
      if (hadoopKrb5ConfKey != null || systemKrb5ConfKey != null) {
        Preconditions.checkArgument(
            hadoopKrb5ConfKey != null && !hadoopKrb5ConfKey.trim().isEmpty(),
            "hadoopKrb5ConfKey can't be blank");
        Preconditions.checkArgument(
            systemKrb5ConfKey != null && !systemKrb5ConfKey.trim().isEmpty(),
            "systemKrb5ConfKey can't be blank");
      }
      return new KerberosClient(this);
    }
  }
}
