/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.aux;

import java.util.Map;

/**
 * GravitinoAuxiliaryService could be managed as Aux Service in GravitinoServer with isolated
 * classpath
 */
public interface GravitinoAuxiliaryService {

  /**
   * The name of GravitinoAuxiliaryService implementation, GravitinoServer will automatically start
   * the aux service implementation if the name is added to `gravitino.auxService.AuxServiceNames`
   */
  String shortName();

  /**
   * @param config GravitinoServer will pass the config with prefix
   *     `gravitino.auxService.{shortName}.` to aux server
   */
  void serviceInit(Map<String, String> config);

  /** Start aux service */
  void serviceStart();

  /** Stop aux service */
  void serviceStop() throws Exception;
}
