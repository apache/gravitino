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

package org.apache.gravitino.auxiliary;

import java.util.Map;

/**
 * GravitinoAuxiliaryService could be managed as Aux Service in GravitinoServer with isolated
 * classpath
 */
public interface GravitinoAuxiliaryService {

  /**
   * The name of GravitinoAuxiliaryService implementation, GravitinoServer will automatically start
   * the aux service implementation if the name is added to `gravitino.auxService.AuxServiceNames`
   *
   * @return the name of GravitinoAuxiliaryService implementation
   */
  String shortName();

  /**
   * @param config GravitinoServer will pass the config with prefix
   *     `gravitino.auxService.{shortName}.` to aux server
   */
  void serviceInit(Map<String, String> config);

  /** Start aux service */
  void serviceStart();

  /**
   * Stop aux service
   *
   * @throws Exception if the stop operation fails
   */
  void serviceStop() throws Exception;
}
