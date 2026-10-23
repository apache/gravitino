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
package org.apache.gravitino.integration.test.util;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;

/**
 * Changes HDFS paths on the Kerberos Hive container from the test JVM, in place of {@code docker
 * exec ... hadoop fs}.
 *
 * <p>Each call logs in as {@code hive/<host>}, the principal the container's own {@code hadoop fs}
 * ran as. The image maps every two-part principal to the HDFS superuser, so calls can change any
 * path. The keytab is the container's {@code /etc/admin.keytab}, which holds that principal. The
 * login returns its own {@link UserGroupInformation} and the file system is not cached, so the
 * login user of a Gravitino server in the same JVM is left alone. {@code java.security.krb5.conf}
 * must already point at the container's KDC.
 */
public final class KerberosHdfs {
  private static final String REALM = "HADOOPKRB";
  private static final String KRB5_CONF = "java.security.krb5.conf";
  static final String KRB5_CONFIG_CLASS = "sun.security.krb5.Config";
  private static final String AUTHENTICATION = "hadoop.security.authentication";
  // The image's own rule: every two-part principal is the HDFS superuser. Set on the client so a
  // login does not depend on the default realm Hadoop has cached from an earlier krb5.conf.
  private static final String AUTH_TO_LOCAL =
      "RULE:[2:$1@$0](.*@" + REALM + ")s/.*/hadoop/\nDEFAULT";

  private final URI defaultFs;
  private final String principal;
  private final String nameNodePrincipal;
  private final String keytabPath;

  /**
   * Creates a client for the NameNode at {@code defaultFs}.
   *
   * @param defaultFs the NameNode address, for example {@code hdfs://172.18.0.2:9000}
   * @param hostName the container's host name, which the service principals carry
   * @param keytabPath a local copy of the container's {@code /etc/admin.keytab}
   */
  public KerberosHdfs(String defaultFs, String hostName, String keytabPath) {
    Preconditions.checkArgument(
        defaultFs != null && defaultFs.startsWith("hdfs://"), "defaultFs must be an hdfs:// URI");
    Preconditions.checkArgument(
        hostName != null && !hostName.isBlank(), "hostName must not be blank");
    Preconditions.checkArgument(
        keytabPath != null && !keytabPath.isBlank(), "keytabPath must not be blank");
    this.defaultFs = URI.create(defaultFs);
    this.principal = String.format("hive/%s@%s", hostName, REALM);
    this.nameNodePrincipal = String.format("hdfs/%s@%s", hostName, REALM);
    this.keytabPath = keytabPath;
  }

  /**
   * Creates a client for the given Kerberos Hive container.
   *
   * @param kerberosHive the running Kerberos Hive container
   * @param keytabPath a local copy of the container's {@code /etc/admin.keytab}
   * @return the client
   */
  public static KerberosHdfs of(HiveContainer kerberosHive, String keytabPath) {
    return new KerberosHdfs(
        String.format(
            "hdfs://%s:%d",
            kerberosHive.getContainerIpAddress(), HiveContainer.HDFS_DEFAULTFS_PORT),
        kerberosHive.getHostName(),
        keytabPath);
  }

  /**
   * Returns the container's {@code /etc/krb5.conf} pointed at a KDC the test JVM can reach. The
   * image names {@code localhost}; tests reach the container by its address.
   *
   * @param krb5Conf the content of the container's {@code /etc/krb5.conf}
   * @param kdcHost the container's address
   * @return the content to use in the test JVM
   */
  public static String reachableKrb5Conf(String krb5Conf, String kdcHost) {
    return krb5Conf
        .replace("kdc = localhost:88", "kdc = " + kdcHost + ":88")
        .replace("admin_server = localhost", "admin_server = " + kdcHost + ":749");
  }

  /**
   * Runs {@code action} with the JVM's Kerberos configuration read from {@code krb5ConfPath}, for
   * callers that run before a test sets up its own, such as a container's readiness check.
   *
   * <p>The configuration is JVM-wide and cached: the JDK parses krb5.conf once into {@code
   * sun.security.krb5.Config} and Hadoop caches the default realm in {@link KerberosName}. Tests
   * that set {@code java.security.krb5.conf} later rely on those caches still being empty or
   * holding their own values, so afterwards both caches are put back as they were (not reloaded),
   * the previous property is restored and, if UGI security was off, UGI is switched back to simple
   * authentication.
   *
   * @param krb5ConfPath a krb5.conf that reaches the container's KDC
   * @param action what to run, such as calls on a {@link KerberosHdfs}
   */
  public static void withKrb5Conf(String krb5ConfPath, Runnable action) {
    String previous = System.getProperty(KRB5_CONF);
    boolean securityWasEnabled = UserGroupInformation.isSecurityEnabled();
    Object previousKrb5Config = readStatic(KRB5_CONFIG_CLASS, "singleton");
    Object previousDefaultRealm = readStatic(KerberosName.class.getName(), "defaultRealm");
    System.setProperty(KRB5_CONF, krb5ConfPath);
    refreshKrb5Config();
    try {
      action.run();
    } finally {
      if (previous == null) {
        System.clearProperty(KRB5_CONF);
      } else {
        System.setProperty(KRB5_CONF, previous);
      }
      if (!securityWasEnabled) {
        Configuration simple = new Configuration();
        simple.set(AUTHENTICATION, "simple");
        UserGroupInformation.setConfiguration(simple);
      }
      writeStatic(KRB5_CONFIG_CLASS, "singleton", previousKrb5Config);
      writeStatic(KerberosName.class.getName(), "defaultRealm", previousDefaultRealm);
    }
  }

  /**
   * Creates a directory and any missing parents, like {@code hadoop fs -mkdir -p}.
   *
   * @param path the directory
   */
  public void mkdirs(String path) {
    run(fs -> fs.mkdirs(new Path(path)));
  }

  /**
   * Sets the permission, like {@code hadoop fs -chmod [-R]}.
   *
   * @param path the file or directory
   * @param octalMode the mode in octal, for example {@code 777}
   * @param recursive whether to also change everything below {@code path}
   */
  public void setPermission(String path, String octalMode, boolean recursive) {
    FsPermission permission = new FsPermission(Short.parseShort(octalMode, 8));
    run(
        fs -> {
          setPermission(fs, new Path(path), permission, recursive);
          return null;
        });
  }

  /**
   * Sets the owner and keeps the group, like {@code hadoop fs -chown [-R] <owner>}.
   *
   * @param path the file or directory
   * @param owner the new owner
   * @param recursive whether to also change everything below {@code path}
   */
  public void setOwner(String path, String owner, boolean recursive) {
    run(
        fs -> {
          setOwner(fs, new Path(path), owner, recursive);
          return null;
        });
  }

  /**
   * Deletes a path, like {@code hadoop fs -rm [-r]} without the trash.
   *
   * @param path the file or directory
   * @param recursive whether a non-empty directory may be deleted
   */
  public void delete(String path, boolean recursive) {
    run(fs -> fs.delete(new Path(path), recursive));
  }

  // HDFS has no recursive chmod or chown; the shell walks the tree the same way.
  static void setPermission(FileSystem fs, Path path, FsPermission permission, boolean recursive)
      throws IOException {
    fs.setPermission(path, permission);
    if (recursive && fs.getFileStatus(path).isDirectory()) {
      for (FileStatus child : fs.listStatus(path)) {
        setPermission(fs, child.getPath(), permission, true);
      }
    }
  }

  static void setOwner(FileSystem fs, Path path, String owner, boolean recursive)
      throws IOException {
    fs.setOwner(path, owner, null);
    if (recursive && fs.getFileStatus(path).isDirectory()) {
      for (FileStatus child : fs.listStatus(path)) {
        setOwner(fs, child.getPath(), owner, true);
      }
    }
  }

  // The JDK reads krb5.conf once; tests refresh it the same way after changing the property.
  private static void refreshKrb5Config() {
    try {
      Class.forName(KRB5_CONFIG_CLASS).getMethod("refresh").invoke(null);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Cannot reload the Kerberos configuration", e);
    }
  }

  // Package-private for TestKerberosHdfs. The build opens sun.security.krb5 to test JVMs.
  static Object readStatic(String className, String fieldName) {
    try {
      Field field = Class.forName(className).getDeclaredField(fieldName);
      field.setAccessible(true);
      return field.get(null);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Cannot read " + className + "." + fieldName, e);
    }
  }

  private static void writeStatic(String className, String fieldName, Object value) {
    try {
      Class<?> type = Class.forName(className);
      Field field = type.getDeclaredField(fieldName);
      field.setAccessible(true);
      // Config.getInstance and KerberosName.getDefaultRealm fill these under the class lock.
      synchronized (type) {
        field.set(null, value);
      }
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Cannot restore " + className + "." + fieldName, e);
    }
  }

  private interface FileSystemAction<T> {
    T apply(FileSystem fs) throws IOException;
  }

  private <T> T run(FileSystemAction<T> action) {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", defaultFs.toString());
    conf.set(AUTHENTICATION, "kerberos");
    conf.set("hadoop.security.auth_to_local", AUTH_TO_LOCAL);
    conf.set("dfs.namenode.kerberos.principal", nameNodePrincipal);
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);
    try {
      // A keytab login is only possible while UGI has security on. The Gravitino server in the
      // same JVM turns it on for its own Kerberos catalogs, so only switch it on when it is off.
      if (!UserGroupInformation.isSecurityEnabled()) {
        UserGroupInformation.setConfiguration(conf);
      }
      UserGroupInformation ugi =
          UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytabPath);
      return ugi.doAs(
          (PrivilegedExceptionAction<T>)
              () -> {
                try (FileSystem fs = FileSystem.get(defaultFs, conf)) {
                  return action.apply(fs);
                }
              });
    } catch (IOException e) {
      throw new UncheckedIOException("HDFS call on " + defaultFs + " failed", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted during an HDFS call on " + defaultFs, e);
    }
  }
}
