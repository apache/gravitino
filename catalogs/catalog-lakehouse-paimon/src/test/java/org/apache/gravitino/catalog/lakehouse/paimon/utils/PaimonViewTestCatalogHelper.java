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
package org.apache.gravitino.catalog.lakehouse.paimon.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.view.View;
import org.apache.paimon.view.ViewChange;
import org.apache.paimon.view.ViewImpl;

/**
 * Utilities for creating in-memory view-capable catalog wrappers in Paimon unit tests.
 *
 * <p>The {@code alterView} change-handling logic is derived from Apache Paimon's {@code ViewChange}
 * application in {@code RESTCatalogServer#viewHandle}:
 * https://github.com/apache/paimon/blob/23fbf740dca4b11c42fc6a6fa70aef055d16155c/paimon-core/src/test/java/org/apache/paimon/rest/RESTCatalogServer.java
 */
public final class PaimonViewTestCatalogHelper {

  private PaimonViewTestCatalogHelper() {}

  public static Catalog createViewSupportedCatalog(Catalog wrappedCatalog) {
    Map<Identifier, View> viewStore = new HashMap<>();
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
      public View getView(Identifier identifier) throws Catalog.ViewNotExistException {
        View storedView = viewStore.get(identifier);
        if (storedView == null) {
          throw new Catalog.ViewNotExistException(identifier);
        }
        return storedView;
      }

      @Override
      public void createView(Identifier identifier, View view, boolean ignoreIfExists)
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
          Identifier identifier, List<ViewChange> changes, boolean ignoreIfNotExists)
          throws Catalog.ViewNotExistException, Catalog.DialectAlreadyExistException,
              Catalog.DialectNotExistException {
        View storedView = viewStore.get(identifier);
        if (storedView == null) {
          if (!ignoreIfNotExists) {
            throw new Catalog.ViewNotExistException(identifier);
          }
          return;
        }

        Map<String, String> updatedOptions = new HashMap<>(storedView.options());
        Map<String, String> updatedDialects = new HashMap<>(storedView.dialects());
        String updatedComment = storedView.comment().orElse(null);

        for (ViewChange change : changes) {
          if (change instanceof ViewChange.SetViewOption) {
            ViewChange.SetViewOption setViewOption = (ViewChange.SetViewOption) change;
            updatedOptions.put(setViewOption.key(), setViewOption.value());
          } else if (change instanceof ViewChange.RemoveViewOption) {
            ViewChange.RemoveViewOption removeViewOption = (ViewChange.RemoveViewOption) change;
            updatedOptions.remove(removeViewOption.key());
          } else if (change instanceof ViewChange.UpdateViewComment) {
            ViewChange.UpdateViewComment updateViewComment = (ViewChange.UpdateViewComment) change;
            updatedComment = updateViewComment.comment();
          } else if (change instanceof ViewChange.AddDialect) {
            ViewChange.AddDialect addDialect = (ViewChange.AddDialect) change;
            if (updatedDialects.containsKey(addDialect.dialect())) {
              throw new Catalog.DialectAlreadyExistException(identifier, addDialect.dialect());
            }
            updatedDialects.put(addDialect.dialect(), addDialect.query());
          } else if (change instanceof ViewChange.UpdateDialect) {
            ViewChange.UpdateDialect updateDialect = (ViewChange.UpdateDialect) change;
            if (!updatedDialects.containsKey(updateDialect.dialect())) {
              throw new Catalog.DialectNotExistException(identifier, updateDialect.dialect());
            }
            updatedDialects.put(updateDialect.dialect(), updateDialect.query());
          } else if (change instanceof ViewChange.DropDialect) {
            ViewChange.DropDialect dropDialect = (ViewChange.DropDialect) change;
            if (!updatedDialects.containsKey(dropDialect.dialect())) {
              throw new Catalog.DialectNotExistException(identifier, dropDialect.dialect());
            }
            updatedDialects.remove(dropDialect.dialect());
          }
        }

        viewStore.put(
            identifier,
            new ViewImpl(
                identifier,
                storedView.rowType().getFields(),
                storedView.query(),
                updatedDialects,
                updatedComment,
                updatedOptions));
      }

      @Override
      public void renameView(
          Identifier fromIdentifier, Identifier toIdentifier, boolean ignoreIfNotExists)
          throws Catalog.ViewNotExistException, Catalog.ViewAlreadyExistException {
        View storedView = viewStore.remove(fromIdentifier);
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

  private static View copyView(Identifier identifier, View view) {
    return new ViewImpl(
        identifier,
        view.rowType().getFields(),
        view.query(),
        new HashMap<>(view.dialects()),
        view.comment().orElse(null),
        new HashMap<>(view.options()));
  }
}
