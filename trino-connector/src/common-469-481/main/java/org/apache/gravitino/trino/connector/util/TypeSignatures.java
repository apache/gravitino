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
package org.apache.gravitino.trino.connector.util;

import io.trino.spi.type.Type;

/**
 * Renders the SQL signature of a Trino {@link Type}, quoting row field names unlike {@link
 * Type#getDisplayName()}.
 *
 * <p>Trino 440-481 expose {@code Type.getTypeSignature()}; Trino 482 replaced it with {@code
 * getTypeDescriptor()}.
 */
public final class TypeSignatures {

  private TypeSignatures() {}

  /**
   * Returns the string signature of the given Trino type.
   *
   * @param type the Trino type to render
   * @return the string signature of the type
   */
  public static String signature(Type type) {
    return type.getTypeSignature().toString();
  }
}
