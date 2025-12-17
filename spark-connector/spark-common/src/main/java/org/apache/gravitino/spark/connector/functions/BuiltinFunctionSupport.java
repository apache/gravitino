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
package org.apache.gravitino.spark.connector.functions;

import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;

public class BuiltinFunctionSupport {

  private static final UnboundFunction MY_STR_LEN_FUNCTION = new MyStringLengthFunction();
  private static final Identifier[] BUILTIN_IDENTIFIERS =
      new Identifier[] {Identifier.of(new String[0], MyStringLengthFunction.NAME)};

  public Identifier[] listFunctions(String[] namespace, String defaultNamespace)
      throws NoSuchNamespaceException {
    if (isSupportedNamespace(namespace, defaultNamespace)) {
      return BUILTIN_IDENTIFIERS;
    }
    throw new NoSuchNamespaceException(namespace);
  }

  public UnboundFunction loadFunction(Identifier identifier, String defaultNamespace)
      throws NoSuchFunctionException {
    if (isSupportedNamespace(identifier.namespace(), defaultNamespace)
        && MyStringLengthFunction.NAME.equalsIgnoreCase(identifier.name())) {
      return MY_STR_LEN_FUNCTION;
    }
    throw new NoSuchFunctionException(identifier);
  }

  private boolean isSupportedNamespace(String[] namespace, String defaultNamespace) {
    return namespace.length == 0
        || (namespace.length == 1 && namespace[0].equalsIgnoreCase(defaultNamespace));
  }
}
