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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Collections;
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
import org.apache.gravitino.catalog.hadoop.authentication.UserContext;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("removal")
public class SecureHadoopCatalogOperations
    implements CatalogOperations, SupportsSchemas, FilesetCatalog {

  public static final Logger LOG = LoggerFactory.getLogger(SecureHadoopCatalogOperations.class);

  private final HadoopCatalogOperations hadoopCatalogOperations;

  public static final String GRAVITINO_KEYTAB_FORMAT = "keytabs/gravitino-%s";

  private UserContext catalogUserContext;

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

  @Override
  public Fileset createFileset(
      NameIdentifier ident,
      String comment,
      Fileset.Type type,
      String storageLocation,
      Map<String, String> properties)
      throws NoSuchSchemaException, FilesetAlreadyExistsException {
    String apiUser = PrincipalUtils.getCurrentUserName();

    UserContext userContext =
        UserContext.getUserContext(
            ident, properties, null, hadoopCatalogOperations.getCatalogInfo());
    return userContext.doAs(
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

    UserContext userContext =
        UserContext.getUserContext(
            ident, filesetEntity.properties(), null, hadoopCatalogOperations.getCatalogInfo());
    boolean r = userContext.doAs(() -> hadoopCatalogOperations.dropFileset(ident), ident);
    UserContext.clearUserContext(ident);
    return r;
  }

  @Override
  public Schema createSchema(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    String apiUser = PrincipalUtils.getCurrentUserName();
    UserContext userContext =
        UserContext.getUserContext(
            ident, properties, null, hadoopCatalogOperations.getCatalogInfo());
    return userContext.doAs(
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

      UserContext userContext =
          UserContext.getUserContext(
              ident, properties, null, hadoopCatalogOperations.getCatalogInfo());
      boolean r = userContext.doAs(() -> hadoopCatalogOperations.dropSchema(ident, cascade), ident);
      UserContext.clearUserContext(ident);

      return r;
    } catch (NoSuchEntityException e) {
      LOG.warn("Schema {} does not exist", ident);
      return false;

    } catch (IOException ioe) {
      throw new RuntimeException("Failed to delete schema " + ident, ioe);
    }
  }

  @Override
  public void initialize(
      Map<String, String> config, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException {
    hadoopCatalogOperations.initialize(config, info, propertiesMetadata);
    catalogUserContext =
        UserContext.getUserContext(
            NameIdentifier.of(info.namespace(), info.name()),
            config,
            hadoopCatalogOperations.getHadoopConf(),
            info);
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
      UserContext.clearUserContext(NameIdentifier.of(ident.namespace(), finalName));
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
  public String getFileLocation(NameIdentifier ident, String subPath)
      throws NoSuchFilesetException {
    return hadoopCatalogOperations.getFileLocation(ident, subPath);
  }

  @Override
  public void close() throws IOException {
    hadoopCatalogOperations.close();

    catalogUserContext.close();

    UserContext.cleanAllUserContext();
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
