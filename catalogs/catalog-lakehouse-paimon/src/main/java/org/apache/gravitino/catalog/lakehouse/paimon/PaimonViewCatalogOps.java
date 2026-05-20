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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.lakehouse.paimon.ops.PaimonCatalogOps;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.exceptions.ViewAlreadyExistsException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewChange;
import org.apache.paimon.catalog.Catalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates Paimon view operations so {@link PaimonCatalogOperations} can compose view logic.
 */
final class PaimonViewCatalogOps {

  private static final Logger LOG = LoggerFactory.getLogger(PaimonViewCatalogOps.class);

  private static final String NO_SUCH_SCHEMA_EXCEPTION =
      "Paimon schema (database) %s does not exist.";
  private static final String NO_SUCH_VIEW_EXCEPTION = "Paimon view %s does not exist.";
  private static final String VIEW_ALREADY_EXISTS_EXCEPTION = "Paimon view %s already exists.";

  private final PaimonCatalogOps paimonCatalogOps;
  private final Function<NameIdentifier, NameIdentifier> paimonIdentifierBuilder;
  private final Predicate<NameIdentifier> schemaExistsChecker;

  PaimonViewCatalogOps(
      PaimonCatalogOps paimonCatalogOps,
      Function<NameIdentifier, NameIdentifier> paimonIdentifierBuilder,
      Predicate<NameIdentifier> schemaExistsChecker) {
    Preconditions.checkArgument(paimonCatalogOps != null, "Paimon catalog ops must not be null");
    Preconditions.checkArgument(
        paimonIdentifierBuilder != null, "Paimon identifier builder must not be null");
    Preconditions.checkArgument(
        schemaExistsChecker != null, "Schema exists checker must not be null");

    this.paimonCatalogOps = paimonCatalogOps;
    this.paimonIdentifierBuilder = paimonIdentifierBuilder;
    this.schemaExistsChecker = schemaExistsChecker;
  }

  NameIdentifier[] listViews(Namespace namespace) throws NoSuchSchemaException {
    String[] levels = namespace.levels();
    NameIdentifier schemaIdentifier = NameIdentifier.of(levels[levels.length - 1]);

    List<String> views;
    try {
      views = paimonCatalogOps.listViews(schemaIdentifier.name());
    } catch (Catalog.DatabaseNotExistException e) {
      throw new NoSuchSchemaException(e, NO_SUCH_SCHEMA_EXCEPTION, namespace.toString());
    }

    return views.stream()
        .map(
            viewIdentifier -> NameIdentifier.of(ArrayUtils.add(namespace.levels(), viewIdentifier)))
        .toArray(NameIdentifier[]::new);
  }

  View loadView(NameIdentifier identifier) throws NoSuchViewException {
    org.apache.paimon.view.View view;
    try {
      NameIdentifier viewIdentifier = paimonIdentifierBuilder.apply(identifier);
      view = paimonCatalogOps.loadView(viewIdentifier.toString());
    } catch (Catalog.ViewNotExistException e) {
      throw new NoSuchViewException(e, NO_SUCH_VIEW_EXCEPTION, identifier);
    }
    return PaimonView.fromPaimonView(view);
  }

  View createView(
      NameIdentifier identifier,
      String comment,
      Column[] columns,
      Representation[] representations,
      String defaultCatalog,
      String defaultSchema,
      Map<String, String> properties)
      throws NoSuchSchemaException, ViewAlreadyExistsException {
    NameIdentifier nameIdentifier = paimonIdentifierBuilder.apply(identifier);
    NameIdentifier schemaIdentifier = NameIdentifier.of(nameIdentifier.namespace().levels());
    if (!schemaExistsChecker.test(schemaIdentifier)) {
      throw new NoSuchSchemaException(NO_SUCH_SCHEMA_EXCEPTION, schemaIdentifier);
    }

    org.apache.paimon.view.View paimonView =
        PaimonView.toPaimonView(
            identifier,
            comment,
            columns,
            representations,
            defaultCatalog,
            defaultSchema,
            properties);

    try {
      paimonCatalogOps.createView(nameIdentifier.toString(), paimonView);
    } catch (Catalog.DatabaseNotExistException e) {
      throw new NoSuchSchemaException(e, NO_SUCH_SCHEMA_EXCEPTION, schemaIdentifier);
    } catch (Catalog.ViewAlreadyExistException e) {
      throw new ViewAlreadyExistsException(e, VIEW_ALREADY_EXISTS_EXCEPTION, identifier);
    }

    return PaimonView.fromPaimonView(paimonView);
  }

  View alterView(NameIdentifier identifier, ViewChange... changes)
      throws NoSuchViewException, IllegalArgumentException {
    if (changes == null || changes.length == 0) {
      return loadView(identifier);
    }

    List<ViewChange.RenameView> renameViewChanges =
        Arrays.stream(changes)
            .filter(viewChange -> viewChange instanceof ViewChange.RenameView)
            .map(viewChange -> (ViewChange.RenameView) viewChange)
            .collect(Collectors.toList());
    if (!renameViewChanges.isEmpty()) {
      Preconditions.checkArgument(
          renameViewChanges.size() == 1,
          "Only one rename operation is allowed, but found: %s",
          renameViewChanges.size());

      List<String> otherChanges =
          Arrays.stream(changes)
              .filter(viewChange -> !(viewChange instanceof ViewChange.RenameView))
              .map(String::valueOf)
              .collect(Collectors.toList());
      Preconditions.checkArgument(
          otherChanges.isEmpty(),
          "The operation to change the view name cannot be performed together with other operations. "
              + "The list of operations that you cannot perform includes: \n%s",
          String.join("\n", otherChanges));
      return renameView(identifier, renameViewChanges.get(0));
    }

    if (containsReplaceView(changes)) {
      return replaceView(identifier, changes);
    }

    return alterViewWithoutReplace(identifier, changes);
  }

  boolean dropView(NameIdentifier identifier) {
    try {
      NameIdentifier viewIdentifier = paimonIdentifierBuilder.apply(identifier);
      paimonCatalogOps.dropView(viewIdentifier.toString());
    } catch (Catalog.ViewNotExistException e) {
      return false;
    }

    return true;
  }

  private View renameView(NameIdentifier identifier, ViewChange.RenameView renameView)
      throws NoSuchViewException, IllegalArgumentException {
    NameIdentifier sourceIdentifier = paimonIdentifierBuilder.apply(identifier);
    NameIdentifier renamedIdentifier =
        NameIdentifier.of(identifier.namespace(), renameView.getNewName());
    NameIdentifier targetIdentifier = paimonIdentifierBuilder.apply(renamedIdentifier);
    try {
      paimonCatalogOps.renameView(sourceIdentifier.toString(), targetIdentifier.toString());
    } catch (Catalog.ViewNotExistException e) {
      throw new NoSuchViewException(e, NO_SUCH_VIEW_EXCEPTION, identifier);
    } catch (Catalog.ViewAlreadyExistException e) {
      throw new ViewAlreadyExistsException(e, VIEW_ALREADY_EXISTS_EXCEPTION, renamedIdentifier);
    }

    try {
      return loadView(renamedIdentifier);
    } catch (NoSuchViewException e) {
      throw new IllegalStateException(
          String.format(
              "Paimon view %s was renamed to %s, but loading the renamed view failed.",
              identifier, renamedIdentifier),
          e);
    }
  }

  private View replaceView(NameIdentifier identifier, ViewChange... changes)
      throws NoSuchViewException, IllegalArgumentException {
    View existingView = loadView(identifier);
    Map<String, String> finalProperties = new HashMap<>(existingView.properties());
    ViewChange.ReplaceView replaceView = null;

    for (ViewChange change : changes) {
      if (change instanceof ViewChange.SetProperty) {
        ViewChange.SetProperty setProperty = (ViewChange.SetProperty) change;
        finalProperties.put(setProperty.getProperty(), setProperty.getValue());
      } else if (change instanceof ViewChange.RemoveProperty) {
        finalProperties.remove(((ViewChange.RemoveProperty) change).getProperty());
      } else if (change instanceof ViewChange.ReplaceView) {
        replaceView = (ViewChange.ReplaceView) change;
      } else {
        throw new IllegalArgumentException(
            "Unsupported view change type: " + change.getClass().getSimpleName());
      }
    }

    Preconditions.checkArgument(replaceView != null, "Replace view change is required");

    org.apache.paimon.view.View paimonView =
        PaimonView.toPaimonView(
            identifier,
            replaceView.getComment(),
            replaceView.getColumns(),
            replaceView.getRepresentations(),
            replaceView.getDefaultCatalog(),
            replaceView.getDefaultSchema(),
            finalProperties);

    NameIdentifier sourceIdentifier = paimonIdentifierBuilder.apply(identifier);

    try {
      // Paimon does not provide a native replace-view API. To align with Paimon Spark's
      // CREATE OR REPLACE VIEW behavior, replace is executed as drop-then-create.
      paimonCatalogOps.dropView(sourceIdentifier.toString());
    } catch (Catalog.ViewNotExistException e) {
      throw new NoSuchViewException(e, NO_SUCH_VIEW_EXCEPTION, identifier);
    }

    try {
      paimonCatalogOps.createView(sourceIdentifier.toString(), paimonView);
    } catch (Catalog.ViewAlreadyExistException e) {
      LOG.warn(
          "Replacing Paimon view {} is non-atomic (drop-then-create). "
              + "Create failed after drop and the original view may be lost.",
          identifier,
          e);
      throw new IllegalArgumentException(
          String.format(VIEW_ALREADY_EXISTS_EXCEPTION, identifier), e);
    } catch (Catalog.DatabaseNotExistException e) {
      LOG.warn(
          "Replacing Paimon view {} is non-atomic (drop-then-create). "
              + "Create failed after drop and the original view may be lost.",
          identifier,
          e);
      throw new IllegalArgumentException(
          String.format(NO_SUCH_SCHEMA_EXCEPTION, identifier.namespace()), e);
    }

    return loadView(identifier);
  }

  private View alterViewWithoutReplace(NameIdentifier identifier, ViewChange... changes)
      throws NoSuchViewException, IllegalArgumentException {
    List<org.apache.paimon.view.ViewChange> paimonViewChanges = new ArrayList<>();

    try {
      for (ViewChange change : changes) {
        if (change instanceof ViewChange.SetProperty) {
          ViewChange.SetProperty setProperty = (ViewChange.SetProperty) change;
          paimonViewChanges.add(
              org.apache.paimon.view.ViewChange.setOption(
                  setProperty.getProperty(), setProperty.getValue()));
        } else if (change instanceof ViewChange.RemoveProperty) {
          ViewChange.RemoveProperty removeProperty = (ViewChange.RemoveProperty) change;
          paimonViewChanges.add(
              org.apache.paimon.view.ViewChange.removeOption(removeProperty.getProperty()));
        } else {
          throw new IllegalArgumentException(
              "Unsupported view change type: " + change.getClass().getSimpleName());
        }
      }
      paimonCatalogOps.alterView(
          paimonIdentifierBuilder.apply(identifier).toString(), paimonViewChanges);
    } catch (Catalog.ViewNotExistException e) {
      throw new NoSuchViewException(e, NO_SUCH_VIEW_EXCEPTION, identifier);
    }

    return loadView(identifier);
  }

  private boolean containsReplaceView(ViewChange... changes) {
    return Arrays.stream(changes).anyMatch(change -> change instanceof ViewChange.ReplaceView);
  }
}
