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

package org.apache.gravitino.catalog.hadoop;

import static org.apache.gravitino.catalog.hadoop.authentication.AuthenticationConfig.IMPERSONATION_ENABLE_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import javax.security.auth.Subject;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.catalog.hadoop.authentication.AuthenticationConfig;
import org.apache.gravitino.catalog.hadoop.authentication.kerberos.KerberosClient;
import org.apache.gravitino.catalog.hadoop.authentication.kerberos.KerberosConfig;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.FilesetAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchFilesetException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.gravitino.file.FilesetChange;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("removal")
public class SecureHadoopCatalogOperations
    implements CatalogOperations, SupportsSchemas, FilesetCatalog {

  public static final Logger LOG = LoggerFactory.getLogger(SecureHadoopCatalogOperations.class);

  private final HadoopCatalogOperations hadoopCatalogOperations;

  private final List<Closeable> closeables = Lists.newArrayList();

  private final Map<NameIdentifier, UserInfo> userInfoMap = Maps.newConcurrentMap();
  private final Map<NameIdentifier, Boolean> impersonationEnableMap = Maps.newConcurrentMap();

  public static final String GRAVITINO_KEYTAB_FORMAT = "keytabs/gravitino-%s";

  private String kerberosRealm;

  public SecureHadoopCatalogOperations() {
    this.hadoopCatalogOperations = new HadoopCatalogOperations();
  }

  public SecureHadoopCatalogOperations(EntityStore store) {
    this.hadoopCatalogOperations = new HadoopCatalogOperations(store);
  }

  @VisibleForTesting
  public HadoopCatalogOperations getBaseHadoopCatalogOperations() {
    return hadoopCatalogOperations;
  }

  public String getKerberosRealm() {
    return kerberosRealm;
  }

  static class UserInfo {
    UserGroupInformation loginUser;
    String keytabPath;
    String realm;

    static UserInfo of(UserGroupInformation loginUser, String keytabPath, String kerberosRealm) {
      UserInfo userInfo = new UserInfo();
      userInfo.loginUser = loginUser;
      userInfo.keytabPath = keytabPath;
      userInfo.realm = kerberosRealm;
      return userInfo;
    }
  }

  // We have overridden the createFileset, dropFileset, createSchema, dropSchema method to reset
  // the current user based on the name identifier.

  @Override
  public Fileset createFileset(
      NameIdentifier ident,
      String comment,
      Fileset.Type type,
      String storageLocation,
      Map<String, String> properties)
      throws NoSuchSchemaException, FilesetAlreadyExistsException {
    UserGroupInformation currentUser = getUGIByIdent(properties, ident);
    String apiUser = PrincipalUtils.getCurrentUserName();
    return doAs(
        currentUser,
        () -> {
          setUser(apiUser);
          return hadoopCatalogOperations.createFileset(
              ident, comment, type, storageLocation, properties);
        },
        ident);
  }

  @Override
  public boolean dropFileset(NameIdentifier ident) {
    FilesetEntity filesetEntity;
    try {
      filesetEntity =
          hadoopCatalogOperations
              .getStore()
              .get(ident, Entity.EntityType.FILESET, FilesetEntity.class);
    } catch (NoSuchEntityException e) {
      LOG.warn("Fileset {} does not exist", ident);
      return false;
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to delete fileset " + ident, ioe);
    }

    // Reset the current user based on the name identifier.
    UserGroupInformation currentUser = getUGIByIdent(filesetEntity.properties(), ident);

    boolean r = doAs(currentUser, () -> hadoopCatalogOperations.dropFileset(ident), ident);
    cleanUserInfoAndImpersonation(ident);
    return r;
  }

  @Override
  public Schema createSchema(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    // Reset the current user based on the name identifier and properties.
    UserGroupInformation currentUser = getUGIByIdent(properties, ident);
    String apiUser = PrincipalUtils.getCurrentUserName();

    return doAs(
        currentUser,
        () -> {
          setUser(apiUser);
          return hadoopCatalogOperations.createSchema(ident, comment, properties);
        },
        ident);
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    try {
      SchemaEntity schemaEntity =
          hadoopCatalogOperations
              .getStore()
              .get(ident, Entity.EntityType.SCHEMA, SchemaEntity.class);
      Map<String, String> properties =
          Optional.ofNullable(schemaEntity.properties()).orElse(Collections.emptyMap());

      // Reset the current user based on the name identifier.
      UserGroupInformation user = getUGIByIdent(properties, ident);

      boolean r = doAs(user, () -> hadoopCatalogOperations.dropSchema(ident, cascade), ident);
      cleanUserInfoAndImpersonation(ident);
      return r;
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to delete schema " + ident, ioe);
    }
  }

  @Override
  public void initialize(
      Map<String, String> config, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException {
    hadoopCatalogOperations.initialize(config, info, propertiesMetadata);
    initAuthentication(hadoopCatalogOperations.getConf(), hadoopCatalogOperations.getHadoopConf());
  }

  @Override
  public Fileset alterFileset(NameIdentifier ident, FilesetChange... changes)
      throws NoSuchFilesetException, IllegalArgumentException {
    Fileset fileset = hadoopCatalogOperations.alterFileset(ident, changes);

    String finalName = ident.name();
    for (FilesetChange change : changes) {
      if (change instanceof FilesetChange.RenameFileset) {
        finalName = ((FilesetChange.RenameFileset) change).getNewName();
      }
    }
    if (!ident.name().equals(finalName)) {
      UserInfo userInfo = userInfoMap.remove(ident);
      if (userInfo != null) {
        userInfoMap.put(NameIdentifier.of(ident.namespace(), finalName), userInfo);
      }

      Boolean impersonationEnable = impersonationEnableMap.remove(ident);
      if (impersonationEnable != null) {
        impersonationEnableMap.put(
            NameIdentifier.of(ident.namespace(), finalName), impersonationEnable);
      }
    }

    return fileset;
  }

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    return hadoopCatalogOperations.listSchemas(namespace);
  }

  @Override
  public Schema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    return hadoopCatalogOperations.loadSchema(ident);
  }

  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    return hadoopCatalogOperations.alterSchema(ident, changes);
  }

  @Override
  public NameIdentifier[] listFilesets(Namespace namespace) throws NoSuchSchemaException {
    return hadoopCatalogOperations.listFilesets(namespace);
  }

  @Override
  public Fileset loadFileset(NameIdentifier ident) throws NoSuchFilesetException {
    return hadoopCatalogOperations.loadFileset(ident);
  }

  @Override
  public void close() throws IOException {
    hadoopCatalogOperations.close();

    userInfoMap.clear();
    impersonationEnableMap.clear();
    closeables.forEach(
        c -> {
          try {
            c.close();
          } catch (Exception e) {
            LOG.error("Failed to close resource", e);
          }
        });
  }

  @Override
  public void testConnection(
      NameIdentifier catalogIdent,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws Exception {
    hadoopCatalogOperations.testConnection(catalogIdent, type, provider, comment, properties);
  }

  private void initAuthentication(Map<String, String> conf, Configuration hadoopConf) {
    AuthenticationConfig config = new AuthenticationConfig(conf);
    CatalogInfo catalogInfo = hadoopCatalogOperations.getCatalogInfo();
    NameIdentifier ident = NameIdentifier.of(catalogInfo.namespace(), catalogInfo.name());

    impersonationEnableMap.put(ident, config.isImpersonationEnabled());
    if (config.isKerberosAuth()) {
      initKerberos(
          conf, hadoopConf, NameIdentifier.of(catalogInfo.namespace(), catalogInfo.name()), true);
    } else if (config.isSimpleAuth()) {
      try {
        // Use service login user.
        UserGroupInformation u = UserGroupInformation.getCurrentUser();
        userInfoMap.put(ident, UserInfo.of(u, null, null));
      } catch (Exception e) {
        throw new RuntimeException("Can't get service user for Hadoop catalog", e);
      }
    }
  }

  /**
   * Get the UserGroupInformation based on the NameIdentifier and properties.
   *
   * <p>Note: As UserGroupInformation is a static class, to avoid the thread safety issue, we need
   * to use synchronized to ensure the thread safety: Make login and getLoginUser atomic.
   */
  public synchronized String initKerberos(
      Map<String, String> properties,
      Configuration configuration,
      NameIdentifier ident,
      boolean refreshCredentials) {
    // Init schema level kerberos authentication.

    CatalogInfo catalogInfo = hadoopCatalogOperations.getCatalogInfo();
    String keytabPath =
        String.format(
            GRAVITINO_KEYTAB_FORMAT, catalogInfo.id() + "-" + ident.toString().replace(".", "-"));
    KerberosConfig kerberosConfig = new KerberosConfig(properties);
    if (kerberosConfig.isKerberosAuth()) {
      configuration.set(
          HADOOP_SECURITY_AUTHENTICATION,
          AuthenticationMethod.KERBEROS.name().toLowerCase(Locale.ROOT));
      try {
        UserGroupInformation.setConfiguration(configuration);
        KerberosClient kerberosClient =
            new KerberosClient(properties, configuration, refreshCredentials);
        // Add the kerberos client to the closable to close resources.
        closeables.add(kerberosClient);

        File keytabFile = kerberosClient.saveKeyTabFileFromUri(keytabPath);
        String kerberosRealm = kerberosClient.login(keytabFile.getAbsolutePath());
        // Should this kerberosRealm need to be equals to the realm in the principal?
        userInfoMap.put(
            ident, UserInfo.of(UserGroupInformation.getLoginUser(), keytabPath, kerberosRealm));
        return kerberosRealm;
      } catch (IOException e) {
        throw new RuntimeException("Failed to login with Kerberos", e);
      }
    }

    return null;
  }

  private UserGroupInformation getUGIByIdent(Map<String, String> properties, NameIdentifier ident) {
    KerberosConfig kerberosConfig = new KerberosConfig(properties);
    if (properties.containsKey(IMPERSONATION_ENABLE_KEY)) {
      impersonationEnableMap.put(ident, kerberosConfig.isImpersonationEnabled());
    }

    if (kerberosConfig.isKerberosAuth()) {
      // We assume that the realm of catalog is the same as the realm of the schema and table.
      initKerberos(properties, new Configuration(), ident, false);
    }

    // If the kerberos is not enabled (simple mode), we will use the current user
    return getUserBaseOnNameIdentifier(ident);
  }

  private UserGroupInformation getUserBaseOnNameIdentifier(NameIdentifier nameIdentifier) {
    UserInfo userInfo = getNearestUserGroupInformation(nameIdentifier);
    if (userInfo == null) {
      throw new RuntimeException("Failed to get user information for " + nameIdentifier);
    }

    UserGroupInformation ugi = userInfo.loginUser;
    boolean userImpersonation = getNearestImpersonationEnable(nameIdentifier);
    if (userImpersonation) {
      String proxyKerberosPrincipalName = PrincipalUtils.getCurrentUserName();
      if (!proxyKerberosPrincipalName.contains("@")) {
        proxyKerberosPrincipalName =
            String.format("%s@%s", proxyKerberosPrincipalName, userInfo.realm);
      }

      ugi = UserGroupInformation.createProxyUser(proxyKerberosPrincipalName, ugi);
    }

    return ugi;
  }

  private Boolean getNearestImpersonationEnable(NameIdentifier nameIdentifier) {
    NameIdentifier currentNameIdentifier = nameIdentifier;
    while (currentNameIdentifier != null) {
      if (impersonationEnableMap.containsKey(currentNameIdentifier)) {
        return impersonationEnableMap.get(currentNameIdentifier);
      }

      String[] levels = currentNameIdentifier.namespace().levels();
      // The ident is catalog level.
      if (levels.length <= 1) {
        return false;
      }
      currentNameIdentifier = NameIdentifier.of(currentNameIdentifier.namespace().levels());
    }
    return false;
  }

  private UserInfo getNearestUserGroupInformation(NameIdentifier nameIdentifier) {
    NameIdentifier currentNameIdentifier = nameIdentifier;
    while (currentNameIdentifier != null) {
      if (userInfoMap.containsKey(currentNameIdentifier)) {
        return userInfoMap.get(currentNameIdentifier);
      }

      String[] levels = currentNameIdentifier.namespace().levels();
      // The ident is catalog level.
      if (levels.length <= 1) {
        return null;
      }
      currentNameIdentifier = NameIdentifier.of(currentNameIdentifier.namespace().levels());
    }
    return null;
  }

  private void cleanUserInfoAndImpersonation(NameIdentifier identifier) {
    UserInfo userInfo = userInfoMap.remove(identifier);
    if (userInfo != null) {
      removeFile(userInfo.keytabPath);
    }

    impersonationEnableMap.remove(identifier);
  }

  private void removeFile(String filePath) {
    if (filePath == null) {
      return;
    }

    File file = new File(filePath);
    if (file.exists()) {
      file.delete();
    }
  }

  private <T> T doAs(
      UserGroupInformation userGroupInformation,
      PrivilegedExceptionAction<T> action,
      NameIdentifier ident) {
    try {
      return userGroupInformation.doAs(action);
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

  /**
   * Add the user to the subject so that we can get the last user in the subject. Hadoop catalog
   * uses this method to pass api user from the client side, so that we can get the user in the
   * subject. Please do not mix it with UserGroupInformation.getCurrentUser().
   *
   * @param apiUser the username to set.
   */
  private void setUser(String apiUser) {
    java.security.AccessControlContext context = java.security.AccessController.getContext();
    Subject subject = Subject.getSubject(context);
    subject.getPrincipals().add(new UserPrincipal(apiUser));
  }
}
