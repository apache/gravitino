/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.aux;

import java.util.Map;

/**
 * GravitonAuxiliaryService could be managed as Aux Service in GravitonServer with isolated
 * classpath
 */
public interface GravitonAuxiliaryService {

  /**
   * The name of GravitonAuxiliaryService implementation, GravitonServer will automatically start
   * the aux service implementation if the name is added to `graviton.auxService.AuxServiceNames`
   */
  String shortName();

  /**
   * @param config , GravitonServer will pass the config with prefix
   *     `graviton.auxService.{shortName}.` to aux server
   */
  void serviceInit(Map<String, String> config);

  /** Start aux service */
  void serviceStart();

  /** Stop aux service */
  void serviceStop() throws Exception;
}
