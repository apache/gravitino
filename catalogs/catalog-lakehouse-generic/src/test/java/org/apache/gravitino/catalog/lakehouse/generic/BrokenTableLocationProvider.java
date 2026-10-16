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
package org.apache.gravitino.catalog.lakehouse.generic;

/**
 * A provider that stands in for a broken third-party jar on the classpath. It is registered in the
 * test {@code META-INF/services} file alongside the working one, so every factory lookup in this
 * module has to get past it.
 *
 * <p>It fails from {@link #name()} with an {@link Error} rather than an exception, which is the way
 * a plugin loaded through its own classloader fails when a class it needs is missing. The factory
 * calls {@code name()} itself rather than going through the loader, so nothing wraps this into a
 * {@link java.util.ServiceConfigurationError} on the way out.
 */
public class BrokenTableLocationProvider implements TableLocationProvider {

  @Override
  public String name() {
    throw new NoClassDefFoundError("com/example/a/class/this/plugin/was/built/against");
  }

  @Override
  public String provisionTableLocation(TableLocationContext context) {
    throw new UnsupportedOperationException("This provider can never be selected");
  }

  @Override
  public void unprovisionTableLocation(TableLocationContext context) {
    throw new UnsupportedOperationException("This provider can never be selected");
  }
}
