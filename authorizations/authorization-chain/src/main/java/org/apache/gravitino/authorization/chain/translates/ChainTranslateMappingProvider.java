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
package org.apache.gravitino.authorization.chain.translates;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.authorization.SecurableObject;

/**
 * The ChainTranslateMappingProvider class provides a mapping of chain translation entities to their
 * respective chain translators.
 */
public class ChainTranslateMappingProvider {
  public interface ChainTranslate {
    List<SecurableObject> translate(List<SecurableObject> securableObjects);
  }

  private static final Map<ChainTranslateEntity, ChainTranslate> translators = new HashMap<>();

  public static ChainTranslate getChainTranslator(ChainTranslateEntity provider) {
    return translators.get(provider);
  }

  public static void put(ChainTranslateEntity provider, ChainTranslate translator) {
    translators.put(provider, translator);
  }
}
