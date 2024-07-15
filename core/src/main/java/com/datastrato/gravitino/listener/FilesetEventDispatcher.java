/*
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

package com.datastrato.gravitino.listener;

import com.apache.gravitino.NameIdentifier;
import com.apache.gravitino.Namespace;
import com.apache.gravitino.exceptions.FilesetAlreadyExistsException;
import com.apache.gravitino.exceptions.NoSuchFilesetException;
import com.apache.gravitino.exceptions.NoSuchSchemaException;
import com.apache.gravitino.file.Fileset;
import com.apache.gravitino.file.FilesetChange;
import com.datastrato.gravitino.catalog.FilesetDispatcher;
import com.datastrato.gravitino.listener.api.event.AlterFilesetEvent;
import com.datastrato.gravitino.listener.api.event.AlterFilesetFailureEvent;
import com.datastrato.gravitino.listener.api.event.CreateFilesetEvent;
import com.datastrato.gravitino.listener.api.event.CreateFilesetFailureEvent;
import com.datastrato.gravitino.listener.api.event.DropFilesetEvent;
import com.datastrato.gravitino.listener.api.event.DropFilesetFailureEvent;
import com.datastrato.gravitino.listener.api.event.ListFilesetEvent;
import com.datastrato.gravitino.listener.api.event.ListFilesetFailureEvent;
import com.datastrato.gravitino.listener.api.event.LoadFilesetEvent;
import com.datastrato.gravitino.listener.api.event.LoadFilesetFailureEvent;
import com.datastrato.gravitino.listener.api.info.FilesetInfo;
import com.datastrato.gravitino.utils.PrincipalUtils;
import java.util.Map;

/**
 * {@code FilesetEventDispatcher} is a decorator for {@link FilesetDispatcher} that not only
 * delegates fileset operations to the underlying catalog dispatcher but also dispatches
 * corresponding events to an {@link com.datastrato.gravitino.listener.EventBus} after each
 * operation is completed. This allows for event-driven workflows or monitoring of fileset
 * operations.
 */
public class FilesetEventDispatcher implements FilesetDispatcher {
  private final EventBus eventBus;
  private final FilesetDispatcher dispatcher;

  public FilesetEventDispatcher(EventBus eventBus, FilesetDispatcher dispatcher) {
    this.eventBus = eventBus;
    this.dispatcher = dispatcher;
  }

  @Override
  public NameIdentifier[] listFilesets(Namespace namespace) throws NoSuchSchemaException {
    try {
      NameIdentifier[] nameIdentifiers = dispatcher.listFilesets(namespace);
      eventBus.dispatchEvent(new ListFilesetEvent(PrincipalUtils.getCurrentUserName(), namespace));
      return nameIdentifiers;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new ListFilesetFailureEvent(PrincipalUtils.getCurrentUserName(), namespace, e));
      throw e;
    }
  }

  @Override
  public Fileset loadFileset(NameIdentifier ident) throws NoSuchFilesetException {
    try {
      Fileset fileset = dispatcher.loadFileset(ident);
      eventBus.dispatchEvent(
          new LoadFilesetEvent(
              PrincipalUtils.getCurrentUserName(), ident, new FilesetInfo(fileset)));
      return fileset;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new LoadFilesetFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e));
      throw e;
    }
  }

  @Override
  public Fileset createFileset(
      NameIdentifier ident,
      String comment,
      Fileset.Type type,
      String storageLocation,
      Map<String, String> properties)
      throws NoSuchSchemaException, FilesetAlreadyExistsException {
    try {
      Fileset fileset = dispatcher.createFileset(ident, comment, type, storageLocation, properties);
      eventBus.dispatchEvent(
          new CreateFilesetEvent(
              PrincipalUtils.getCurrentUserName(), ident, new FilesetInfo(fileset)));
      return fileset;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new CreateFilesetFailureEvent(
              PrincipalUtils.getCurrentUserName(),
              ident,
              e,
              new FilesetInfo(ident.name(), comment, type, storageLocation, properties, null)));
      throw e;
    }
  }

  @Override
  public Fileset alterFileset(NameIdentifier ident, FilesetChange... changes)
      throws NoSuchFilesetException, IllegalArgumentException {
    try {
      Fileset fileset = dispatcher.alterFileset(ident, changes);
      eventBus.dispatchEvent(
          new AlterFilesetEvent(
              PrincipalUtils.getCurrentUserName(), ident, changes, new FilesetInfo(fileset)));
      return fileset;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new AlterFilesetFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e, changes));
      throw e;
    }
  }

  @Override
  public boolean dropFileset(NameIdentifier ident) {
    try {
      boolean isExists = dispatcher.dropFileset(ident);
      eventBus.dispatchEvent(
          new DropFilesetEvent(PrincipalUtils.getCurrentUserName(), ident, isExists));
      return isExists;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new DropFilesetFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e));
      throw e;
    }
  }
}
