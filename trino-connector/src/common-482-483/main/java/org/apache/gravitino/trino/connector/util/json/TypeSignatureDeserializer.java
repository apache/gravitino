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
package org.apache.gravitino.trino.connector.util.json;

import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * Registers the Trino {@code TypeSignature} deserializer.
 *
 * <p>Trino 482 removed {@code io.trino.spi.type.TypeSignature}, so there is nothing to register.
 */
public final class TypeSignatureDeserializer {

  private TypeSignatureDeserializer() {}

  /**
   * Registers the {@code TypeSignature} deserializer on the given module; a no-op on Trino 482+.
   *
   * @param module the Jackson module to register the deserializer on
   * @param classLoader the class loader of the Trino type manager
   */
  public static void register(SimpleModule module, ClassLoader classLoader) {}
}
