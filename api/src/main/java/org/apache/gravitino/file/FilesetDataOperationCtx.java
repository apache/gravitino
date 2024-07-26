/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.file;

import org.apache.gravitino.annotation.Evolving;

/**
 * An interface representing a fileset data operation context. This interface defines some
 * information need to report to the server.
 *
 * <p>{@link FilesetDataOperationCtx} defines the basic properties of a fileset data operation
 * context object.
 */
@Evolving
public interface FilesetDataOperationCtx {
  /** @return The sub path which is operated by the data operation . */
  String subPath();

  /** @return The data operation type. */
  String operation();

  /** @return The client type of the data operation. */
  String clientType();
}
