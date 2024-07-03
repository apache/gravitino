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

package com.datastrato.gravitino.catalog.hadoop;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.Schema;
import com.datastrato.gravitino.SchemaChange;
import com.datastrato.gravitino.catalog.hadoop.HadoopCatalogOperations.UserInfo;
import com.datastrato.gravitino.catalog.hadoop.authentication.kerberos.KerberosConfig;
import com.datastrato.gravitino.connector.CatalogInfo;
import com.datastrato.gravitino.connector.CatalogOperations;
import com.datastrato.gravitino.connector.HasPropertyMetadata;
import com.datastrato.gravitino.connector.SupportsSchemas;
import com.datastrato.gravitino.exceptions.FilesetAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.exceptions.NoSuchFilesetException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.file.FilesetCatalog;
import com.datastrato.gravitino.file.FilesetChange;
import com.datastrato.gravitino.meta.FilesetEntity;
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.utils.PrincipalUtils;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SecureHadoopCatalogOperations is a secure version of HadoopCatalogOperations that can manage
 * Schema and fileset level of user authentication.
 */
public class SecureHadoopCatalogOperations
    implements CatalogOperations, SupportsSchemas, FilesetCatalog {

  public static final Logger LOG = LoggerFactory.getLogger(SecureHadoopCatalogOperations.class);

  private final HadoopCatalogOperations hadoopCatalogOperations;

  public SecureHadoopCatalogOperations() {
    this.hadoopCatalogOperations = new HadoopCatalogOperations();
  }

  public SecureHadoopCatalogOperations(EntityStore store) {
    this.hadoopCatalogOperations = new HadoopCatalogOperations(store);
  }

  public HadoopCatalogOperations getHadoopCatalogOperations() {
    return hadoopCatalogOperations;
  }

  public String getKerberosRealm() {
    return hadoopCatalogOperations.getKerberosRealm();
  }

  public void setProxyPlugin(HadoopProxyPlugin plugin) {
    hadoopCatalogOperations.setProxyPlugin(plugin);
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

    // Why I need to do this? When we call getUGIByIdent, `PrincipalUtils.getCurrentUserName()` is
    // Not the api user, but the Kerberos principal name.
    String apiUser = PrincipalUtils.getCurrentUserName();
    hadoopCatalogOperations.setCurrentUser(apiUser);

    UserGroupInformation currentUser = getUGIByIdent(properties, ident);
    try {
      return currentUser.doAs(
          (PrivilegedExceptionAction<Fileset>)
              () ->
                  hadoopCatalogOperations.createFileset(
                      ident, comment, type, storageLocation, properties));
    } catch (IOException | InterruptedException ioe) {
      throw new RuntimeException("Failed to create fileset " + ident, ioe);
    } catch (UndeclaredThrowableException e) {
      Throwable innerException = e.getCause();
      if (innerException instanceof PrivilegedActionException) {
        throw new RuntimeException(innerException.getCause());
      } else if (innerException instanceof InvocationTargetException) {
        throw new RuntimeException(innerException.getCause());
      } else {
        throw new RuntimeException(innerException);
      }
    } finally {
      hadoopCatalogOperations.setCurrentUser(null);
    }
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

    try {
      boolean r =
          currentUser.doAs(
              (PrivilegedExceptionAction<Boolean>)
                  () -> hadoopCatalogOperations.dropFileset(ident));
      cleanUserInfo(ident);
      return r;
    } catch (IOException | InterruptedException ioe) {
      throw new RuntimeException("Failed to create fileset " + ident, ioe);
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

  @Override
  public Schema createSchema(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {

    try {
      String apiUser = PrincipalUtils.getCurrentUserName();
      hadoopCatalogOperations.setCurrentUser(apiUser);
      // Reset the current user based on the name identifier and properties.
      UserGroupInformation currentUser = getUGIByIdent(properties, ident);

      return currentUser.doAs(
          (PrivilegedExceptionAction<Schema>)
              () -> hadoopCatalogOperations.createSchema(ident, comment, properties));
    } catch (IOException | InterruptedException ioe) {
      throw new RuntimeException("Failed to create fileset " + ident, ioe);
    } catch (UndeclaredThrowableException e) {
      Throwable innerException = e.getCause();
      if (innerException instanceof PrivilegedActionException) {
        throw new RuntimeException(innerException.getCause());
      } else if (innerException instanceof InvocationTargetException) {
        throw new RuntimeException(innerException.getCause());
      } else {
        throw new RuntimeException(innerException);
      }
    } finally {
      hadoopCatalogOperations.setCurrentUser(null);
    }
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

      boolean r =
          user.doAs(
              (PrivilegedExceptionAction<Boolean>)
                  () -> hadoopCatalogOperations.dropSchema(ident, cascade));
      cleanUserInfo(ident);
      return r;

    } catch (IOException | InterruptedException ioe) {
      throw new RuntimeException("Failed to create fileset " + ident, ioe);
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

  @Override
  public void initialize(
      Map<String, String> config, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException {
    hadoopCatalogOperations.initialize(config, info, propertiesMetadata);
  }

  @Override
  public Fileset alterFileset(NameIdentifier ident, FilesetChange... changes)
      throws NoSuchFilesetException, IllegalArgumentException {
    try {
      return hadoopCatalogOperations.alterFileset(ident, changes);
    } finally {
      String finalName = ident.name();
      for (FilesetChange change : changes) {
        if (change instanceof FilesetChange.RenameFileset) {
          finalName = ((FilesetChange.RenameFileset) change).getNewName();
        }
      }
      if (!ident.name().equals(finalName)) {
        UserInfo userInfo = hadoopCatalogOperations.getUserInfoMap().remove(ident);
        if (userInfo != null) {
          hadoopCatalogOperations
              .getUserInfoMap()
              .put(NameIdentifier.of(ident.namespace(), finalName), userInfo);
        }
      }
    }
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
  }

  private UserGroupInformation getUGIByIdent(Map<String, String> properties, NameIdentifier ident) {
    KerberosConfig kerberosConfig = new KerberosConfig(properties);
    if (kerberosConfig.isKerberosAuth()) {
      // We assume that the realm of catalog is the same as the realm of the schema and table.
      hadoopCatalogOperations.initKerberos(properties, new Configuration(), ident);
    }
    // If the kerberos is not enabled (Simple mode), we will use the current user
    return getUserBaseOnNameIdentifier(ident);
  }

  private UserGroupInformation getUserBaseOnNameIdentifier(NameIdentifier nameIdentifier) {
    UserInfo userInfo = getNearestUserGroupInformation(nameIdentifier);
    if (userInfo == null) {

      // TODO(yuqi) comment the following code if the user in the docker hive image is the same as
      //  the user `anonymous` see: https://github.com/datastrato/gravitino/issues/4013
      try {
        return UserGroupInformation.getLoginUser();
      } catch (IOException e) {
        throw new RuntimeException("Failed to get login user", e);
      }
    }

    UserGroupInformation ugi = userInfo.loginUser;
    boolean userImpersonation = userInfo.enableUserImpersonation;
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

  private UserInfo getNearestUserGroupInformation(NameIdentifier nameIdentifier) {
    NameIdentifier currentNameIdentifier = nameIdentifier;
    while (currentNameIdentifier != null) {
      if (hadoopCatalogOperations.getUserInfoMap().containsKey(currentNameIdentifier)) {
        return hadoopCatalogOperations.getUserInfoMap().get(currentNameIdentifier);
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

  private void cleanUserInfo(NameIdentifier identifier) {
    UserInfo userInfo = hadoopCatalogOperations.getUserInfoMap().remove(identifier);
    if (userInfo != null) {
      removeFile(userInfo.keytabPath);
    }
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
}
