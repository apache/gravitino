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
package org.apache.gravitino.job;

import java.util.List;

/**
 * JobTemplateProvider exposes job templates that can be registered into Gravitino.
 *
 * <p>Implementations are expected to be discoverable (for example via SPI) so that Gravitino can
 * onboard built-in job templates automatically.
 */
public interface JobTemplateProvider {

  /** Prefix for built-in job template names. */
  String BUILTIN_NAME_PREFIX = "builtin-";

  /** Regex to validate built-in job template names. */
  String BUILTIN_NAME_PATTERN = "^" + BUILTIN_NAME_PREFIX + "[\\w-]+$";

  /** Property key used to carry the built-in job template version. */
  String PROPERTY_VERSION_KEY = "version";

  /** Regex for version property value (e.g., v1, v2). */
  String VERSION_VALUE_PATTERN = "v\\d+";

  /**
   * Return all job templates provided by this implementation.
   *
   * @return a list of job templates to register
   */
  List<? extends JobTemplate> jobTemplates();
}
