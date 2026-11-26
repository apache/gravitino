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

import java.util.function.BiFunction;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.listener.api.event.article.ArticleEvent;
import org.apache.gravitino.listener.api.event.article.ArticleFailureEvent;
import org.apache.gravitino.listener.api.event.article.ArticlePreEvent;
import org.apache.gravitino.utils.PrincipalUtils;

/** Delivers Article operations to the configured listeners. */
public final class ArticleEventDispatcher {
  @Nullable private final EventBus eventBus;

  /**
   * Creates a dispatcher for the Article manager.
   *
   * @param eventBus the server event bus, or null when no listeners are configured
   */
  public ArticleEventDispatcher(@Nullable EventBus eventBus) {
    this.eventBus = eventBus;
  }

  /**
   * @return whether the manager should capture a change for listeners
   */
  public boolean enabled() {
    return eventBus != null;
  }

  /**
   * Dispatches an operation's events around its execution.
   *
   * @param <T> the operation result type
   * @param operation the Article operation
   * @param requested the requested Article or metalake
   * @param action the operation to execute
   * @param successFactory produces the post-event or returns null for an unchanged or pending write
   * @return the operation result
   */
  public <T> T execute(
      OperationType operation,
      NameIdentifier requested,
      Supplier<T> action,
      BiFunction<String, T, ArticleEvent> successFactory) {
    if (eventBus == null) {
      return action.get();
    }
    String user = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new ArticlePreEvent(user, requested, operation));
    try {
      T result = action.get();
      ArticleEvent event = successFactory.apply(user, result);
      if (event != null) {
        success(event);
      }
      return result;
    } catch (RuntimeException failure) {
      eventBus.dispatchEvent(new ArticleFailureEvent(user, requested, operation, failure));
      throw failure;
    }
  }

  /**
   * Publishes a completed operation, including a write committed by an enclosing transaction.
   *
   * @param event the completed operation
   */
  public void success(ArticleEvent event) {
    if (eventBus != null) {
      eventBus.dispatchEvent(event);
    }
  }
}
