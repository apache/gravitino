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

package org.apache.gravitino.listener;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.SemanticModelDispatcher;
import org.apache.gravitino.catalog.SemanticModelNormalizeDispatcher;
import org.apache.gravitino.exceptions.IllegalSemanticModelException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchSemanticModelException;
import org.apache.gravitino.exceptions.SemanticModelAlreadyExistsException;
import org.apache.gravitino.listener.api.event.semantic.AlterSemanticModelEvent;
import org.apache.gravitino.listener.api.event.semantic.AlterSemanticModelFailureEvent;
import org.apache.gravitino.listener.api.event.semantic.AlterSemanticModelPreEvent;
import org.apache.gravitino.listener.api.event.semantic.CreateSemanticModelEvent;
import org.apache.gravitino.listener.api.event.semantic.CreateSemanticModelFailureEvent;
import org.apache.gravitino.listener.api.event.semantic.CreateSemanticModelPreEvent;
import org.apache.gravitino.listener.api.event.semantic.DropSemanticModelEvent;
import org.apache.gravitino.listener.api.event.semantic.DropSemanticModelFailureEvent;
import org.apache.gravitino.listener.api.event.semantic.DropSemanticModelPreEvent;
import org.apache.gravitino.listener.api.event.semantic.ListSemanticModelEvent;
import org.apache.gravitino.listener.api.event.semantic.ListSemanticModelFailureEvent;
import org.apache.gravitino.listener.api.event.semantic.ListSemanticModelPreEvent;
import org.apache.gravitino.listener.api.event.semantic.LoadSemanticModelEvent;
import org.apache.gravitino.listener.api.event.semantic.LoadSemanticModelFailureEvent;
import org.apache.gravitino.listener.api.event.semantic.LoadSemanticModelPreEvent;
import org.apache.gravitino.listener.api.info.SemanticModelInfo;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelChange;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * Decorates a {@link SemanticModelDispatcher} to dispatch pre/post/failure Semantic Model events to
 * an {@link EventBus}, mirroring {@link ViewEventDispatcher}.
 */
public class SemanticModelEventDispatcher implements SemanticModelDispatcher {

  private final EventBus eventBus;
  private final SemanticModelDispatcher dispatcher;

  /**
   * Constructs a {@code SemanticModelEventDispatcher}.
   *
   * @param eventBus Event bus for listener plugins.
   * @param dispatcher Underlying dispatcher, for example a {@link
   *     SemanticModelNormalizeDispatcher}.
   */
  public SemanticModelEventDispatcher(EventBus eventBus, SemanticModelDispatcher dispatcher) {
    this.eventBus = eventBus;
    this.dispatcher = dispatcher;
  }

  @Override
  public NameIdentifier[] listSemanticModels(Namespace namespace) throws NoSuchSchemaException {
    eventBus.dispatchEvent(
        new ListSemanticModelPreEvent(PrincipalUtils.getCurrentUserName(), namespace));
    try {
      NameIdentifier[] identifiers = dispatcher.listSemanticModels(namespace);
      eventBus.dispatchEvent(
          new ListSemanticModelEvent(
              PrincipalUtils.getCurrentUserName(),
              namespace,
              identifiers != null ? identifiers.length : -1));
      return identifiers;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new ListSemanticModelFailureEvent(PrincipalUtils.getCurrentUserName(), namespace, e));
      throw e;
    }
  }

  @Override
  public SemanticModel loadSemanticModel(NameIdentifier ident) throws NoSuchSemanticModelException {
    eventBus.dispatchEvent(
        new LoadSemanticModelPreEvent(PrincipalUtils.getCurrentUserName(), ident));
    try {
      SemanticModel semanticModel = dispatcher.loadSemanticModel(ident);
      eventBus.dispatchEvent(
          new LoadSemanticModelEvent(
              PrincipalUtils.getCurrentUserName(), ident, new SemanticModelInfo(semanticModel)));
      return semanticModel;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new LoadSemanticModelFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e));
      throw e;
    }
  }

  @Override
  public boolean semanticModelExists(NameIdentifier ident) {
    return dispatcher.semanticModelExists(ident);
  }

  @Override
  public SemanticModel createSemanticModel(
      NameIdentifier ident,
      @Nullable String comment,
      SemanticModelDefinition definition,
      Map<String, String> properties)
      throws NoSuchSchemaException, SemanticModelAlreadyExistsException,
          IllegalSemanticModelException {
    SemanticModelInfo createRequest =
        new SemanticModelInfo(ident.name(), comment, definition, properties, null);
    eventBus.dispatchEvent(
        new CreateSemanticModelPreEvent(PrincipalUtils.getCurrentUserName(), ident, createRequest));
    try {
      SemanticModel semanticModel =
          dispatcher.createSemanticModel(ident, comment, definition, properties);
      eventBus.dispatchEvent(
          new CreateSemanticModelEvent(
              PrincipalUtils.getCurrentUserName(), ident, new SemanticModelInfo(semanticModel)));
      return semanticModel;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new CreateSemanticModelFailureEvent(
              PrincipalUtils.getCurrentUserName(), ident, e, createRequest));
      throw e;
    }
  }

  @Override
  public SemanticModel alterSemanticModel(NameIdentifier ident, SemanticModelChange... changes)
      throws NoSuchSemanticModelException, SemanticModelAlreadyExistsException,
          IllegalSemanticModelException {
    eventBus.dispatchEvent(
        new AlterSemanticModelPreEvent(PrincipalUtils.getCurrentUserName(), ident, changes));
    try {
      SemanticModel semanticModel = dispatcher.alterSemanticModel(ident, changes);
      eventBus.dispatchEvent(
          new AlterSemanticModelEvent(
              PrincipalUtils.getCurrentUserName(),
              ident,
              changes,
              new SemanticModelInfo(semanticModel)));
      return semanticModel;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new AlterSemanticModelFailureEvent(
              PrincipalUtils.getCurrentUserName(), ident, e, changes));
      throw e;
    }
  }

  @Override
  public boolean dropSemanticModel(NameIdentifier ident) {
    eventBus.dispatchEvent(
        new DropSemanticModelPreEvent(PrincipalUtils.getCurrentUserName(), ident));
    try {
      boolean existed = dispatcher.dropSemanticModel(ident);
      eventBus.dispatchEvent(
          new DropSemanticModelEvent(PrincipalUtils.getCurrentUserName(), ident, existed));
      return existed;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new DropSemanticModelFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e));
      throw e;
    }
  }
}
