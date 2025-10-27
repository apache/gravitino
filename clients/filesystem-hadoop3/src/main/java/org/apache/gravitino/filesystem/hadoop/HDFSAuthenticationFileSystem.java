package org.apache.gravitino.filesystem.hadoop;
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

import static org.apache.gravitino.catalog.hadoop.fs.Constants.AUTH_KERBEROS;
import static org.apache.gravitino.catalog.hadoop.fs.Constants.AUTH_SIMPlE;
import static org.apache.gravitino.catalog.hadoop.fs.Constants.FS_DISABLE_CACHE;
import static org.apache.gravitino.catalog.hadoop.fs.Constants.HADOOP_KRB5_CONF;
import static org.apache.gravitino.catalog.hadoop.fs.Constants.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.gravitino.catalog.hadoop.fs.Constants.HADOOP_SECURITY_KEYTAB;
import static org.apache.gravitino.catalog.hadoop.fs.Constants.HADOOP_SECURITY_PRINCIPAL;
import static org.apache.gravitino.catalog.hadoop.fs.Constants.SECURITY_KRB5_ENV;
import static org.apache.gravitino.catalog.hadoop.fs.HDFSFileSystemProvider.IPC_FALLBACK_TO_SIMPLE_AUTH_ALLOWED;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.time.Instant;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A FileSystem wrapper that runs all operations under a specific UGI (UserGroupInformation).
 * Supports both simple and Kerberos authentication, with automatic ticket renewal.
 */
public class HDFSAuthenticationFileSystem extends FileSystem {

  private static final Logger LOG = LoggerFactory.getLogger(HDFSAuthenticationFileSystem.class);

  private static final long DEFAULT_RENEW_INTERVAL_MS = 10 * 60 * 1000L;
  private static final String SYSTEM_USER_NAME = System.getProperty("user.name");
  private static final String SYSTEM_ENV_HADOOP_USER_NAME = "HADOOP_USER_NAME";

  private final UserGroupInformation ugi;
  private final FileSystem fs;
  private Timer kerberosRenewTimer;

  /**
   * Create a HDFSAuthenticationFileSystem with the given path and configuration. Supports both
   * simple and Kerberos authentication, with automatic ticket renewal for Kerberos.
   *
   * @param path the HDFS path
   * @param conf the Hadoop configuration
   */
  public HDFSAuthenticationFileSystem(Path path, Configuration conf) {
    try {
      conf.setBoolean(FS_DISABLE_CACHE, true);
      conf.setBoolean(IPC_FALLBACK_TO_SIMPLE_AUTH_ALLOWED, true);

      String authType = conf.get(HADOOP_SECURITY_AUTHENTICATION, AUTH_SIMPlE);

      if (AUTH_KERBEROS.equalsIgnoreCase(authType)) {
        String krb5Config = conf.get(HADOOP_KRB5_CONF);

        if (krb5Config != null) {
          System.setProperty(SECURITY_KRB5_ENV, krb5Config);
        }
        UserGroupInformation.setConfiguration(conf);
        String principal = conf.get(HADOOP_SECURITY_PRINCIPAL, null);
        String keytab = conf.get(HADOOP_SECURITY_KEYTAB, null);

        if (principal == null || keytab == null) {
          throw new GravitinoRuntimeException(
              "Kerberos principal and keytab must be provided for kerberos authentication");
        }

        this.ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
        startKerberosRenewalTask(principal);
      } else {
        String userName = System.getenv(SYSTEM_ENV_HADOOP_USER_NAME);
        if (StringUtils.isEmpty(userName)) {
          userName = SYSTEM_USER_NAME;
        }
        this.ugi = UserGroupInformation.createRemoteUser(userName);
      }

      this.fs =
          ugi.doAs(
              (PrivilegedExceptionAction<FileSystem>)
                  () -> FileSystem.newInstance(path.toUri(), conf));

    } catch (Exception e) {
      throw new GravitinoRuntimeException(e, "Failed to create HDFS FileSystem with UGI: %s", path);
    }
  }

  /** Schedule periodic Kerberos re-login to refresh TGT before expiry. */
  private void startKerberosRenewalTask(String principal) {
    kerberosRenewTimer = new Timer(true);
    kerberosRenewTimer.scheduleAtFixedRate(
        new TimerTask() {
          @Override
          public void run() {
            try {
              if (ugi.hasKerberosCredentials()) {
                ugi.checkTGTAndReloginFromKeytab();
              }
            } catch (Exception e) {
              LOG.error(
                  Instant.now()
                      + " [Kerberos] Failed to renew TGT for principal "
                      + principal
                      + ": "
                      + e.getMessage());
            }
          }
        },
        DEFAULT_RENEW_INTERVAL_MS,
        DEFAULT_RENEW_INTERVAL_MS);
  }

  /** Run a FileSystem operation under the UGI context, with IO exception wrapping. */
  private <T> T doAsIO(PrivilegedExceptionAction<T> action) throws IOException {
    try {
      return ugi.doAs(action);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public URI getUri() {
    return fs.getUri();
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return doAsIO(() -> fs.open(f, bufferSize));
  }

  @Override
  public FSDataOutputStream create(
      Path f,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress)
      throws IOException {
    return doAsIO(
        () -> fs.create(f, permission, overwrite, bufferSize, replication, blockSize, progress));
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    return doAsIO(() -> fs.append(f, bufferSize, progress));
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return doAsIO(() -> fs.rename(src, dst));
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return doAsIO(() -> fs.delete(f, recursive));
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    return doAsIO(() -> fs.listStatus(f));
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    try {
      doAsIO(
          () -> {
            fs.setWorkingDirectory(newDir);
            return null;
          });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Path getWorkingDirectory() {
    try {
      return doAsIO(fs::getWorkingDirectory);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return doAsIO(() -> fs.mkdirs(f, permission));
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    return doAsIO(() -> fs.getFileStatus(f));
  }

  @Override
  public long getDefaultBlockSize(Path f) {
    try {
      return doAsIO(() -> fs.getDefaultBlockSize(f));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public short getDefaultReplication(Path path) {
    try {
      return doAsIO(() -> fs.getDefaultReplication(path));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      if (kerberosRenewTimer != null) {
        kerberosRenewTimer.cancel();
      }
    } finally {
      doAsIO(
          () -> {
            fs.close();
            return null;
          });
    }
  }
}
