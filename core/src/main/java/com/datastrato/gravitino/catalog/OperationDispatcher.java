/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static com.datastrato.gravitino.catalog.PropertiesMetadataHelpers.validatePropertyForAlter;

import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.StringIdentifier;
import com.datastrato.gravitino.connector.BasePropertiesMetadata;
import com.datastrato.gravitino.connector.HasPropertyMetadata;
import com.datastrato.gravitino.connector.PropertiesMetadata;
import com.datastrato.gravitino.exceptions.IllegalNameIdentifierException;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.file.FilesetChange;
import com.datastrato.gravitino.messaging.TopicChange;
import com.datastrato.gravitino.rel.SchemaChange;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.utils.ThrowableFunction;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An operation dispatcher that dispatches the operations to the underlying catalog implementation.
 */
public abstract class OperationDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(OperationDispatcher.class);

  private final CatalogManager catalogManager;

  protected final EntityStore store;

  final IdGenerator idGenerator;

  /**
   * Creates a new CatalogOperationDispatcher instance.
   *
   * @param catalogManager The CatalogManager instance to be used for catalog operations.
   * @param store The EntityStore instance to be used for catalog operations.
   * @param idGenerator The IdGenerator instance to be used for catalog operations.
   */
  public OperationDispatcher(
      CatalogManager catalogManager, EntityStore store, IdGenerator idGenerator) {
    this.catalogManager = catalogManager;
    this.store = store;
    this.idGenerator = idGenerator;
  }

  <R, E extends Throwable> R doWithCatalog(
      NameIdentifier ident, ThrowableFunction<CatalogManager.CatalogWrapper, R> fn, Class<E> ex)
      throws E {
    try {
      CatalogManager.CatalogWrapper c = catalogManager.loadCatalogAndWrap(ident);
      return fn.apply(c);
    } catch (Throwable throwable) {
      if (ex.isInstance(throwable)) {
        throw ex.cast(throwable);
      }
      if (RuntimeException.class.isAssignableFrom(throwable.getClass())) {
        throw (RuntimeException) throwable;
      }
      throw new RuntimeException(throwable);
    }
  }

  <R, E1 extends Throwable, E2 extends Throwable> R doWithCatalog(
      NameIdentifier ident,
      ThrowableFunction<CatalogManager.CatalogWrapper, R> fn,
      Class<E1> ex1,
      Class<E2> ex2)
      throws E1, E2 {
    try {
      CatalogManager.CatalogWrapper c = catalogManager.loadCatalogAndWrap(ident);
      return fn.apply(c);
    } catch (Throwable throwable) {
      if (ex1.isInstance(throwable)) {
        throw ex1.cast(throwable);
      } else if (ex2.isInstance(throwable)) {
        throw ex2.cast(throwable);
      }
      if (RuntimeException.class.isAssignableFrom(throwable.getClass())) {
        throw (RuntimeException) throwable;
      }

      throw new RuntimeException(throwable);
    }
  }

  Set<String> getHiddenPropertyNames(
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

  <T> void validateAlterProperties(
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

  StringIdentifier getStringIdFromProperties(Map<String, String> properties) {
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

  <R extends HasIdentifier> R operateOnEntity(
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

  @VisibleForTesting
  // TODO(xun): Remove this method when we implement a better way to get the catalog identifier
  //  [#257] Add an explicit get catalog functions in NameIdentifier
  NameIdentifier getCatalogIdentifier(NameIdentifier ident) {
    NameIdentifier.check(
        ident.name() != null, "The name variable in the NameIdentifier must have value.");
    Namespace.check(
        ident.namespace() != null && ident.namespace().length() > 0,
        "Catalog namespace must be non-null and have 1 level, the input namespace is %s",
        ident.namespace());

    List<String> allElems =
        Stream.concat(Arrays.stream(ident.namespace().levels()), Stream.of(ident.name()))
            .collect(Collectors.toList());
    if (allElems.size() < 2) {
      throw new IllegalNameIdentifierException(
          "Cannot create a catalog NameIdentifier less than two elements.");
    }
    return NameIdentifier.of(allElems.get(0), allElems.get(1));
  }

  boolean isManagedEntity(Map<String, String> properties) {
    return Optional.ofNullable(properties)
        .map(
            p ->
                p.getOrDefault(
                        BasePropertiesMetadata.GRAVITINO_MANAGED_ENTITY, Boolean.FALSE.toString())
                    .equals(Boolean.TRUE.toString()))
        .orElse(false);
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
