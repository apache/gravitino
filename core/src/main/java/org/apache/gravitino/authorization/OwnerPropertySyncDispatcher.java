/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.authorization;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps the {@code owner} property of Iceberg namespaces and tables in step with Gravitino
 * ownership. The Iceberg REST catalog writes that property, the conventional Iceberg and Hive owner
 * key that engines display, when it creates a namespace or table. After an ownership transfer this
 * dispatcher rewrites the property to the new owner, so engines and Gravitino give the same answer.
 * Objects without the property, such as those created through the Gravitino API, are left alone.
 */
public class OwnerPropertySyncDispatcher implements OwnerDispatcher {

  /** The Iceberg and Hive property that names an object's owner. */
  @VisibleForTesting static final String OWNER_PROPERTY = "owner";

  private static final Logger LOG = LoggerFactory.getLogger(OwnerPropertySyncDispatcher.class);
  private static final String ICEBERG_PROVIDER = "lakehouse-iceberg";

  private final OwnerDispatcher dispatcher;

  /**
   * Creates a dispatcher that sets owners through {@code dispatcher} and then updates the owner
   * property.
   *
   * @param dispatcher the dispatcher that records ownership
   */
  public OwnerPropertySyncDispatcher(OwnerDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public void setOwner(
      String metalake, MetadataObject metadataObject, String ownerName, Owner.Type ownerType) {
    dispatcher.setOwner(metalake, metadataObject, ownerName, ownerType);
    syncOwnerProperty(metalake, metadataObject, ownerName);
  }

  @Override
  public void setOwners(
      String metalake,
      List<MetadataObject> metadataObjects,
      String ownerName,
      Owner.Type ownerType) {
    dispatcher.setOwners(metalake, metadataObjects, ownerName, ownerType);
    if (metadataObjects != null) {
      metadataObjects.forEach(object -> syncOwnerProperty(metalake, object, ownerName));
    }
  }

  @Override
  public Optional<Owner> getOwner(String metalake, MetadataObject metadataObject) {
    return dispatcher.getOwner(metalake, metadataObject);
  }

  /**
   * Rewrites the owner property of an Iceberg namespace or table that has one. Ownership has
   * already changed when this runs, so a failure is logged rather than failing the transfer.
   */
  @VisibleForTesting
  void syncOwnerProperty(String metalake, MetadataObject metadataObject, String ownerName) {
    MetadataObject.Type type = metadataObject.type();
    if (type != MetadataObject.Type.SCHEMA && type != MetadataObject.Type.TABLE) {
      return;
    }
    NameIdentifier identifier = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
    try {
      if (!isIcebergCatalog(NameIdentifierUtil.getCatalogIdentifier(identifier))) {
        return;
      }
      GravitinoEnv env = GravitinoEnv.getInstance();
      if (type == MetadataObject.Type.SCHEMA) {
        SchemaDispatcher schemas = env.internalSchemaDispatcher();
        Schema schema = schemas.loadSchema(identifier);
        if (needsUpdate(schema.properties(), ownerName)) {
          schemas.alterSchema(identifier, SchemaChange.setProperty(OWNER_PROPERTY, ownerName));
        }
      } else {
        TableDispatcher tables = env.internalTableDispatcher();
        Table table = tables.loadTable(identifier);
        if (needsUpdate(table.properties(), ownerName)) {
          tables.alterTable(identifier, TableChange.setProperty(OWNER_PROPERTY, ownerName));
        }
      }
    } catch (Exception e) {
      LOG.warn(
          "Ownership of {} {} moved to {}, but its {} property could not be updated",
          type,
          identifier,
          ownerName,
          OWNER_PROPERTY,
          e);
    }
  }

  private static boolean isIcebergCatalog(NameIdentifier catalogIdent) {
    Catalog catalog =
        GravitinoEnv.getInstance().internalCatalogDispatcher().loadCatalog(catalogIdent);
    return ICEBERG_PROVIDER.equalsIgnoreCase(catalog.provider());
  }

  @VisibleForTesting
  static boolean needsUpdate(Map<String, String> properties, String ownerName) {
    return properties != null
        && properties.containsKey(OWNER_PROPERTY)
        && !ownerName.equals(properties.get(OWNER_PROPERTY));
  }
}
