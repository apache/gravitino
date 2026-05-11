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
import org.apache.gravitino.catalog.ViewDispatcher;
import org.apache.gravitino.catalog.ViewOperationDispatcher;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.exceptions.ViewAlreadyExistsException;
import org.apache.gravitino.listener.api.event.AlterViewEvent;
import org.apache.gravitino.listener.api.event.AlterViewFailureEvent;
import org.apache.gravitino.listener.api.event.AlterViewPreEvent;
import org.apache.gravitino.listener.api.event.CreateViewEvent;
import org.apache.gravitino.listener.api.event.CreateViewFailureEvent;
import org.apache.gravitino.listener.api.event.CreateViewPreEvent;
import org.apache.gravitino.listener.api.event.DropViewEvent;
import org.apache.gravitino.listener.api.event.DropViewFailureEvent;
import org.apache.gravitino.listener.api.event.DropViewPreEvent;
import org.apache.gravitino.listener.api.event.ListViewEvent;
import org.apache.gravitino.listener.api.event.ListViewFailureEvent;
import org.apache.gravitino.listener.api.event.ListViewPreEvent;
import org.apache.gravitino.listener.api.event.LoadViewEvent;
import org.apache.gravitino.listener.api.event.LoadViewFailureEvent;
import org.apache.gravitino.listener.api.event.LoadViewPreEvent;
import org.apache.gravitino.listener.api.info.ViewInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewChange;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * Decorates a {@link ViewDispatcher} to dispatch pre/post/failure view events to an {@link
 * EventBus}, mirroring {@link TableEventDispatcher}.
 */
public class ViewEventDispatcher implements ViewDispatcher {

  private final EventBus eventBus;
  private final ViewDispatcher dispatcher;

  /**
   * @param eventBus event bus for listener plugins
   * @param dispatcher underlying dispatcher (for example {@link ViewOperationDispatcher} behind a
   *     normalize layer)
   */
  public ViewEventDispatcher(EventBus eventBus, ViewDispatcher dispatcher) {
    this.eventBus = eventBus;
    this.dispatcher = dispatcher;
  }

  @Override
  public NameIdentifier[] listViews(Namespace namespace) throws NoSuchSchemaException {
    eventBus.dispatchEvent(new ListViewPreEvent(PrincipalUtils.getCurrentUserName(), namespace));
    try {
      NameIdentifier[] identifiers = dispatcher.listViews(namespace);
      eventBus.dispatchEvent(new ListViewEvent(PrincipalUtils.getCurrentUserName(), namespace));
      return identifiers;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new ListViewFailureEvent(PrincipalUtils.getCurrentUserName(), namespace, e));
      throw e;
    }
  }

  @Override
  public View loadView(NameIdentifier ident) throws NoSuchViewException {
    eventBus.dispatchEvent(new LoadViewPreEvent(PrincipalUtils.getCurrentUserName(), ident));
    try {
      View view = dispatcher.loadView(ident);
      eventBus.dispatchEvent(
          new LoadViewEvent(PrincipalUtils.getCurrentUserName(), ident, new ViewInfo(view)));
      return view;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new LoadViewFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e));
      throw e;
    }
  }

  @Override
  public boolean viewExists(NameIdentifier ident) {
    return dispatcher.viewExists(ident);
  }

  @Override
  public View createView(
      NameIdentifier ident,
      @Nullable String comment,
      Column[] columns,
      Representation[] representations,
      @Nullable String defaultCatalog,
      @Nullable String defaultSchema,
      Map<String, String> properties)
      throws NoSuchSchemaException, ViewAlreadyExistsException {
    ViewInfo createRequest =
        new ViewInfo(
            ident.name(),
            columns,
            comment,
            representations,
            defaultCatalog,
            defaultSchema,
            properties,
            null);
    eventBus.dispatchEvent(
        new CreateViewPreEvent(PrincipalUtils.getCurrentUserName(), ident, createRequest));
    try {
      View view =
          dispatcher.createView(
              ident, comment, columns, representations, defaultCatalog, defaultSchema, properties);
      eventBus.dispatchEvent(
          new CreateViewEvent(PrincipalUtils.getCurrentUserName(), ident, new ViewInfo(view)));
      return view;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new CreateViewFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e, createRequest));
      throw e;
    }
  }

  @Override
  public View alterView(NameIdentifier ident, ViewChange... changes)
      throws NoSuchViewException, IllegalArgumentException {
    eventBus.dispatchEvent(
        new AlterViewPreEvent(PrincipalUtils.getCurrentUserName(), ident, changes));
    try {
      View view = dispatcher.alterView(ident, changes);
      eventBus.dispatchEvent(
          new AlterViewEvent(
              PrincipalUtils.getCurrentUserName(), ident, changes, new ViewInfo(view)));
      return view;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new AlterViewFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e, changes));
      throw e;
    }
  }

  @Override
  public boolean dropView(NameIdentifier ident) {
    eventBus.dispatchEvent(new DropViewPreEvent(PrincipalUtils.getCurrentUserName(), ident));
    try {
      boolean existed = dispatcher.dropView(ident);
      eventBus.dispatchEvent(
          new DropViewEvent(PrincipalUtils.getCurrentUserName(), ident, existed));
      return existed;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new DropViewFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e));
      throw e;
    }
  }
}
