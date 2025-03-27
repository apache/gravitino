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

package org.apache.gravitino;

/* User friendly error messages. */
public class ErrorMessages {
  public static final String SCHEMA_DOES_NOT_EXIST = "Schema %s does not exist";
  public static final String MODEL_DOES_NOT_EXIST = "Model %s does not exist";
  public static final String MODEL_ALREADY_EXISTS = "Model %s already exists";
  public static final String MODEL_VERSION_ALREADY_EXISTS =
      "Model version aliases %s already exist";
  public static final String FAILED_TO_LOAD_MODEL = "Failed to load model %s";
  public static final String FAILED_TO_DELETE_MODEL = "Failed to delete model %s";
  public static final String FAILED_TO_LIST_MODEL_VERSIONS =
      "Failed to list model versions for model %s";
  public static final String FAILED_TO_LIST_MODELS = "Failed to list models under namespace %s";
  public static final String FAILED_TO_LINK_MODEL_VERSION = "Failed to link model version %s";
  public static final String FAILED_TO_GET_MODEL = "Failed to get model version %s";
  public static final String FAILED_TO_REGISTER_MODEL = "Failed to register model %s";
  public static final String PROPERTY_NOT_NULL = "Property string identifier should not be null";
  public static final String FAILED_TO_DELETE_MODEL_VERSION = "Failed to delete model version %s";
}
