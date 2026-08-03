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

package org.apache.gravitino.secret;

import org.apache.gravitino.annotation.DeveloperApi;

/**
 * Context passed to {@link SecretProvider#writeSecret(String, SecretWriteContext)}.
 *
 * <p>This is intentionally a marker interface. Different providers may require different write
 * metadata; implementations should document the concrete context type they accept (for example
 * {@link ServiceSecretWriteContext} for entity write-through), and callers must supply a compatible
 * instance.
 */
@DeveloperApi
public interface SecretWriteContext {}
