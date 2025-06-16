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
package org.apache.gravitino.catalog;

import static org.apache.gravitino.catalog.PropertiesMetadataHelpers.validatePropertyForAlter;
import static org.apache.gravitino.utils.NameIdentifierUtil.getCatalogIdentifier;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.file.FilesetChange;
import org.apache.gravitino.messaging.TopicChange;
import org.apache.gravitino.rel.SupportsPartitions;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.ThrowableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An operation dispatcher that dispatches the operations to the underlying catalog implementation.
 */
public abstract class OperationDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(OperationDispatcher.class);

  private final CatalogManager catalogManager;

  protected final EntityStore store;

  protected final IdGenerator idGenerator;

  /**
   * Creates a new CatalogOperationDispatcher instance.
   *
   * @param catalogManager The CatalogManager instance to be used for catalog operations.
   * @param store The EntityStore instance to be used for catalog operations.
   * @param idGenerator The IdGenerator instance to be used for catalog operations.
   */
  protected OperationDispatcher(
      CatalogManager catalogManager, EntityStore store, IdGenerator idGenerator) {
    this.catalogManager = catalogManager;
    this.store = store;
    this.idGenerator = idGenerator;
  }

  protected <R, E extends Throwable> R doWithTable(
      NameIdentifier tableIdent, ThrowableFunction<SupportsPartitions, R> fn, Class<E> ex)
      throws E {
    try {
      NameIdentifier catalogIdent = getCatalogIdentifier(tableIdent);
      CatalogManager.CatalogWrapper c = catalogManager.loadCatalogAndWrap(catalogIdent);
      return c.doWithPartitionOps(tableIdent, fn);
    } catch (Exception exception) {
      if (ex.isInstance(exception)) {
        throw ex.cast(exception);
      }
      if (RuntimeException.class.isAssignableFrom(exception.getClass())) {
        throw (RuntimeException) exception;
      }
      throw new RuntimeException(exception);
    }
  }

  protected <R, E extends Throwable> R doWithCatalog(
      NameIdentifier ident, ThrowableFunction<CatalogManager.CatalogWrapper, R> fn, Class<E> ex)
      throws E {
    catalogManager.checkCatalogInUse(store, ident);

    try {
      CatalogManager.CatalogWrapper c = catalogManager.loadCatalogAndWrap(ident);
      return fn.apply(c);
    } catch (Exception exception) {
      if (ex.isInstance(exception)) {
        throw ex.cast(exception);
      }
      if (RuntimeException.class.isAssignableFrom(exception.getClass())) {
        throw (RuntimeException) exception;
      }
      throw new RuntimeException(exception);
    }
  }

  protected <R, E1 extends Throwable, E2 extends Throwable> R doWithCatalog(
      NameIdentifier ident,
      ThrowableFunction<CatalogManager.CatalogWrapper, R> fn,
      Class<E1> ex1,
      Class<E2> ex2)
      throws E1, E2 {
    catalogManager.checkCatalogInUse(store, ident);

    try {
      CatalogManager.CatalogWrapper c = catalogManager.loadCatalogAndWrap(ident);
      return fn.apply(c);
    } catch (Exception exception) {
      if (ex1.isInstance(exception)) {
        throw ex1.cast(exception);
      } else if (ex2.isInstance(exception)) {
        throw ex2.cast(exception);
      }
      if (RuntimeException.class.isAssignableFrom(exception.getClass())) {
        throw (RuntimeException) exception;
      }

      throw new RuntimeException(exception);
    }
  }

  protected Set<String> getHiddenPropertyNames(
      NameIdentifier catalogIdent,
      ThrowableFunction<HasPropertyMetadata, PropertiesMetadata> provider,
      Map<String, String> properties) {
    return doWithCatalog(
        catalogIdent,
        c ->
            c.doWithPropertiesMeta(
                p -> {
                  PropertiesMetadata propertiesMetadata = provider.apply(p);
                  return properties.keySet().stream()
                      .filter(propertiesMetadata::isHiddenProperty)
                      .collect(Collectors.toSet());
                }),
        IllegalArgumentException.class);
  }

  protected <T> void validateAlterProperties(
      NameIdentifier ident,
      ThrowableFunction<HasPropertyMetadata, PropertiesMetadata> provider,
      T... changes) {
    doWithCatalog(
        getCatalogIdentifier(ident),
        c ->
            c.doWithPropertiesMeta(
                p -> {
                  Map<String, String> upserts = getPropertiesForSet(changes);
                  Map<String, String> deletes = getPropertiesForDelete(changes);
                  validatePropertyForAlter(provider.apply(p), upserts, deletes);
                  return null;
                }),
        IllegalArgumentException.class);
  }

  private <T> Map<String, String> getPropertiesForDelete(T... t) {
    Map<String, String> properties = Maps.newHashMap();
    for (T item : t) {
      if (item instanceof TableChange.RemoveProperty) {
        TableChange.RemoveProperty removeProperty = (TableChange.RemoveProperty) item;
        properties.put(removeProperty.getProperty(), removeProperty.getProperty());
      } else if (item instanceof SchemaChange.RemoveProperty) {
        SchemaChange.RemoveProperty removeProperty = (SchemaChange.RemoveProperty) item;
        properties.put(removeProperty.getProperty(), removeProperty.getProperty());
      } else if (item instanceof FilesetChange.RemoveProperty) {
        FilesetChange.RemoveProperty removeProperty = (FilesetChange.RemoveProperty) item;
        properties.put(removeProperty.getProperty(), removeProperty.getProperty());
      } else if (item instanceof TopicChange.RemoveProperty) {
        TopicChange.RemoveProperty removeProperty = (TopicChange.RemoveProperty) item;
        properties.put(removeProperty.getProperty(), removeProperty.getProperty());
      }
    }

    return properties;
  }

  protected StringIdentifier getStringIdFromProperties(Map<String, String> properties) {
    try {
      StringIdentifier stringId = StringIdentifier.fromProperties(properties);
      if (stringId == null) {
        LOG.warn(FormattedErrorMessages.STRING_ID_NOT_FOUND);
      }
      return stringId;
    } catch (IllegalArgumentException e) {
      LOG.warn(FormattedErrorMessages.STRING_ID_PARSE_ERROR, e.getMessage());
      return null;
    }
  }

  protected <R extends HasIdentifier> R operateOnEntity(
      NameIdentifier ident, ThrowableFunction<NameIdentifier, R> fn, String opName, long id) {
    R ret = null;
    try {
      ret = fn.apply(ident);
    } catch (NoSuchEntityException e) {
      // Case 2: The table is created by Gravitino, but has no corresponding entity in Gravitino.
      LOG.error(FormattedErrorMessages.ENTITY_NOT_FOUND, ident);
    } catch (Exception e) {
      // Case 3: The table is created by Gravitino, but failed to operate the corresponding entity
      // in Gravitino
      LOG.error(FormattedErrorMessages.STORE_OP_FAILURE, opName, ident, e);
    }

    // Case 4: The table is created by Gravitino, but the uid in the corresponding entity is not
    // matched.
    if (ret != null && ret.id() != id) {
      LOG.error(FormattedErrorMessages.ENTITY_UNMATCHED, ident, ret.id(), id);
      ret = null;
    }

    return ret;
  }

  boolean isManagedEntity(NameIdentifier catalogIdent, Capability.Scope scope) {
    return doWithCatalog(
        catalogIdent,
        c -> c.capabilities().managedStorage(scope).supported(),
        IllegalArgumentException.class);
  }

  protected <E extends Entity & HasIdentifier> E getEntity(
      NameIdentifier ident, Entity.EntityType type, Class<E> entityClass) {
    try {
      return store.get(ident, type, entityClass);
    } catch (Exception e) {
      LOG.warn(FormattedErrorMessages.STORE_OP_FAILURE, "get", ident, e.getMessage(), e);
      return null;
    }
  }

  private <T> Map<String, String> getPropertiesForSet(T... t) {
    Map<String, String> properties = Maps.newHashMap();
    for (T item : t) {
      if (item instanceof TableChange.SetProperty) {
        TableChange.SetProperty setProperty = (TableChange.SetProperty) item;
        properties.put(setProperty.getProperty(), setProperty.getValue());
      } else if (item instanceof SchemaChange.SetProperty) {
        SchemaChange.SetProperty setProperty = (SchemaChange.SetProperty) item;
        properties.put(setProperty.getProperty(), setProperty.getValue());
      } else if (item instanceof FilesetChange.SetProperty) {
        FilesetChange.SetProperty setProperty = (FilesetChange.SetProperty) item;
        properties.put(setProperty.getProperty(), setProperty.getValue());
      } else if (item instanceof TopicChange.SetProperty) {
        TopicChange.SetProperty setProperty = (TopicChange.SetProperty) item;
        properties.put(setProperty.getProperty(), setProperty.getValue());
      }
    }

    return properties;
  }

  static final class FormattedErrorMessages {
    static final String STORE_OP_FAILURE =
        "Failed to {} entity for {} in "
            + "Gravitino, with this situation the returned object will not contain the metadata from "
            + "Gravitino.";

    static final String STRING_ID_NOT_FOUND =
        "String identifier is not set in schema properties, "
            + "this is because the schema is not created by Gravitino, or the schema is created by "
            + "Gravitino but the string identifier is removed by the user.";

    static final String STRING_ID_PARSE_ERROR =
        "Failed to get string identifier from schema "
            + "properties: {}, this maybe caused by the same-name string identifier is set by the user "
            + "with unsupported format.";

    static final String ENTITY_NOT_FOUND =
        "Entity for {} doesn't exist in Gravitino, "
            + "this is unexpected if this is created by Gravitino. With this situation the "
            + "returned object will not contain the metadata from Gravitino";

    static final String ENTITY_UNMATCHED =
        "Entity {} with uid {} doesn't match the string "
            + "identifier in the property {}, this is unexpected if this object is created by "
            + "Gravitino. This might be due to some operations that are not performed through Gravitino. "
            + "With this situation the returned object will not contain the metadata from Gravitino";
  }
}
