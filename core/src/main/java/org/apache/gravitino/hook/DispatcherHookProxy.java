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
package org.apache.gravitino.hook;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.List;
import java.util.function.BiConsumer;

class DispatcherHookProxy<T> implements InvocationHandler {
  private final DispatcherHooks hooks;
  private final T dispatcher;

  DispatcherHookProxy(T dispatcher, DispatcherHooks hooks) {
    this.hooks = hooks;
    this.dispatcher = dispatcher;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    Object result = method.invoke(dispatcher, args);
    List<BiConsumer> postHooks = hooks.getPostHooks(method.getName());
    for (BiConsumer hook : postHooks) {
      hook.accept(args, result);
    }
    return result;
  }
}
