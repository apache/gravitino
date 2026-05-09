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
package org.apache.gravitino.catalog.lakehouse.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.lakehouse.paimon.ops.PaimonCatalogOps;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewChange;
import org.apache.gravitino.rel.types.Types;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.view.ViewImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests for {@link PaimonViewCatalogOps}. */
public class TestPaimonViewCatalogOps {

  private static final String DATABASE = "test_view_ops_database";
  private static final String VIEW = "test_view_ops_view";
  private static final NameIdentifier VIEW_IDENTIFIER =
      NameIdentifier.of(Namespace.of(DATABASE), VIEW);

  @TempDir private File warehouse;

  private TestablePaimonCatalogOps paimonCatalogOps;
  private PaimonViewCatalogOps paimonViewCatalogOps;

  @BeforeEach
  public void setUp() throws Exception {
    paimonCatalogOps =
        new TestablePaimonCatalogOps(
            new PaimonConfig(
                ImmutableMap.of(PaimonCatalogPropertiesMetadata.WAREHOUSE, warehouse.getPath())));
    paimonCatalogOps.setCatalog(createViewSupportedCatalog(paimonCatalogOps.catalog()));
    paimonCatalogOps.createDatabase(DATABASE, Maps.newHashMap());

    paimonViewCatalogOps =
        new PaimonViewCatalogOps(
            paimonCatalogOps, this::buildPaimonNameIdentifier, this::schemaExists);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (paimonCatalogOps != null) {
      paimonCatalogOps.close();
    }
  }

  @Test
  public void testRenameViewCannotBeCombinedWithOtherChanges() throws Exception {
    createView(VIEW_IDENTIFIER);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            paimonViewCatalogOps.alterView(
                VIEW_IDENTIFIER,
                ViewChange.rename(VIEW + "_renamed"),
                ViewChange.setProperty("k1", "v1")));

    View unchangedView = paimonViewCatalogOps.loadView(VIEW_IDENTIFIER);
    assertEquals(VIEW, unchangedView.name());
    assertFalse(unchangedView.properties().containsKey("k1"));
  }

  @Test
  public void testAlterViewPropertyChangesAndRenameOnly() throws Exception {
    createView(VIEW_IDENTIFIER);

    paimonViewCatalogOps.alterView(
        VIEW_IDENTIFIER,
        ViewChange.setProperty("k1", "v1"),
        ViewChange.setProperty("k2", "v2"),
        ViewChange.removeProperty("k1"));

    View alteredView = paimonViewCatalogOps.loadView(VIEW_IDENTIFIER);
    assertFalse(alteredView.properties().containsKey("k1"));
    assertEquals("v2", alteredView.properties().get("k2"));

    String renamedViewName = VIEW + "_renamed";
    NameIdentifier renamedIdentifier =
        NameIdentifier.of(VIEW_IDENTIFIER.namespace(), renamedViewName);
    View renamedView =
        paimonViewCatalogOps.alterView(VIEW_IDENTIFIER, ViewChange.rename(renamedViewName));
    assertEquals(renamedViewName, renamedView.name());
    assertThrows(NoSuchViewException.class, () -> paimonViewCatalogOps.loadView(VIEW_IDENTIFIER));
    assertEquals("v2", paimonViewCatalogOps.loadView(renamedIdentifier).properties().get("k2"));
  }

  private void createView(NameIdentifier identifier) throws Exception {
    String query = "SELECT col_1 FROM source_table";
    Representation[] representations =
        new Representation[] {
          SQLRepresentation.builder().withDialect("spark").withSql(query).build()
        };

    paimonViewCatalogOps.createView(
        identifier,
        "test_view_comment",
        new Column[] {Column.of("col_1", Types.IntegerType.get(), "col_1")},
        representations,
        "paimon",
        DATABASE,
        Maps.newHashMap());
  }

  private boolean schemaExists(NameIdentifier identifier) {
    try {
      paimonCatalogOps.loadDatabase(identifier.name());
      return true;
    } catch (Catalog.DatabaseNotExistException e) {
      return false;
    }
  }

  private NameIdentifier buildPaimonNameIdentifier(NameIdentifier identifier) {
    String[] levels = identifier.namespace().levels();
    return NameIdentifier.of(levels[levels.length - 1], identifier.name());
  }

  private Catalog createViewSupportedCatalog(Catalog wrappedCatalog) {
    Map<Identifier, org.apache.paimon.view.View> viewStore = new HashMap<>();
    return new DelegateCatalog(wrappedCatalog) {
      @Override
      public CatalogLoader catalogLoader() {
        return wrapped().catalogLoader();
      }

      @Override
      public List<String> listViews(String databaseName) {
        return viewStore.keySet().stream()
            .filter(identifier -> identifier.getDatabaseName().equals(databaseName))
            .map(Identifier::getObjectName)
            .collect(Collectors.toList());
      }

      @Override
      public org.apache.paimon.view.View getView(Identifier identifier)
          throws Catalog.ViewNotExistException {
        org.apache.paimon.view.View storedView = viewStore.get(identifier);
        if (storedView == null) {
          throw new Catalog.ViewNotExistException(identifier);
        }
        return storedView;
      }

      @Override
      public void createView(
          Identifier identifier, org.apache.paimon.view.View view, boolean ignoreIfExists)
          throws Catalog.ViewAlreadyExistException, Catalog.DatabaseNotExistException {
        if (viewStore.containsKey(identifier)) {
          if (!ignoreIfExists) {
            throw new Catalog.ViewAlreadyExistException(identifier);
          }
          return;
        }

        if (!listDatabases().contains(identifier.getDatabaseName())) {
          throw new Catalog.DatabaseNotExistException(identifier.getDatabaseName());
        }

        viewStore.put(identifier, copyView(identifier, view));
      }

      @Override
      public void alterView(
          Identifier identifier,
          List<org.apache.paimon.view.ViewChange> changes,
          boolean ignoreIfNotExists)
          throws Catalog.ViewNotExistException, Catalog.DialectAlreadyExistException,
              Catalog.DialectNotExistException {
        org.apache.paimon.view.View storedView = viewStore.get(identifier);
        if (storedView == null) {
          if (!ignoreIfNotExists) {
            throw new Catalog.ViewNotExistException(identifier);
          }
          return;
        }

        Map<String, String> updatedOptions = new HashMap<>(storedView.options());
        Map<String, String> updatedDialects = new HashMap<>(storedView.dialects());
        String updatedComment = storedView.comment().orElse(null);

        for (org.apache.paimon.view.ViewChange change : changes) {
          if (change instanceof org.apache.paimon.view.ViewChange.SetViewOption) {
            org.apache.paimon.view.ViewChange.SetViewOption setViewOption =
                (org.apache.paimon.view.ViewChange.SetViewOption) change;
            updatedOptions.put(setViewOption.key(), setViewOption.value());
          } else if (change instanceof org.apache.paimon.view.ViewChange.RemoveViewOption) {
            org.apache.paimon.view.ViewChange.RemoveViewOption removeViewOption =
                (org.apache.paimon.view.ViewChange.RemoveViewOption) change;
            updatedOptions.remove(removeViewOption.key());
          } else if (change instanceof org.apache.paimon.view.ViewChange.UpdateViewComment) {
            org.apache.paimon.view.ViewChange.UpdateViewComment updateViewComment =
                (org.apache.paimon.view.ViewChange.UpdateViewComment) change;
            updatedComment = updateViewComment.comment();
          } else if (change instanceof org.apache.paimon.view.ViewChange.AddDialect) {
            org.apache.paimon.view.ViewChange.AddDialect addDialect =
                (org.apache.paimon.view.ViewChange.AddDialect) change;
            if (updatedDialects.containsKey(addDialect.dialect())) {
              throw new Catalog.DialectAlreadyExistException(identifier, addDialect.dialect());
            }
            updatedDialects.put(addDialect.dialect(), addDialect.query());
          } else if (change instanceof org.apache.paimon.view.ViewChange.UpdateDialect) {
            org.apache.paimon.view.ViewChange.UpdateDialect updateDialect =
                (org.apache.paimon.view.ViewChange.UpdateDialect) change;
            if (!updatedDialects.containsKey(updateDialect.dialect())) {
              throw new Catalog.DialectNotExistException(identifier, updateDialect.dialect());
            }
            updatedDialects.put(updateDialect.dialect(), updateDialect.query());
          } else if (change instanceof org.apache.paimon.view.ViewChange.DropDialect) {
            org.apache.paimon.view.ViewChange.DropDialect dropDialect =
                (org.apache.paimon.view.ViewChange.DropDialect) change;
            if (!updatedDialects.containsKey(dropDialect.dialect())) {
              throw new Catalog.DialectNotExistException(identifier, dropDialect.dialect());
            }
            updatedDialects.remove(dropDialect.dialect());
          }
        }

        String updatedQuery =
            updatedDialects.isEmpty()
                ? storedView.query()
                : updatedDialects.values().iterator().next();
        viewStore.put(
            identifier,
            new ViewImpl(
                identifier,
                storedView.rowType().getFields(),
                updatedQuery,
                updatedDialects,
                updatedComment,
                updatedOptions));
      }

      @Override
      public void renameView(
          Identifier fromIdentifier, Identifier toIdentifier, boolean ignoreIfNotExists)
          throws Catalog.ViewNotExistException, Catalog.ViewAlreadyExistException {
        org.apache.paimon.view.View storedView = viewStore.remove(fromIdentifier);
        if (storedView == null) {
          if (!ignoreIfNotExists) {
            throw new Catalog.ViewNotExistException(fromIdentifier);
          }
          return;
        }

        if (viewStore.containsKey(toIdentifier)) {
          viewStore.put(fromIdentifier, storedView);
          throw new Catalog.ViewAlreadyExistException(toIdentifier);
        }

        viewStore.put(toIdentifier, copyView(toIdentifier, storedView));
      }

      @Override
      public void dropView(Identifier identifier, boolean ignoreIfNotExists)
          throws Catalog.ViewNotExistException {
        if (viewStore.remove(identifier) == null && !ignoreIfNotExists) {
          throw new Catalog.ViewNotExistException(identifier);
        }
      }
    };
  }

  private org.apache.paimon.view.View copyView(
      Identifier identifier, org.apache.paimon.view.View view) {
    return new ViewImpl(
        identifier,
        view.rowType().getFields(),
        view.query(),
        new HashMap<>(view.dialects()),
        view.comment().orElse(null),
        new HashMap<>(view.options()));
  }

  private static class TestablePaimonCatalogOps extends PaimonCatalogOps {

    TestablePaimonCatalogOps(PaimonConfig paimonConfig) {
      super(paimonConfig);
    }

    Catalog catalog() {
      return catalog;
    }

    void setCatalog(Catalog catalog) {
      this.catalog = catalog;
    }
  }
}
