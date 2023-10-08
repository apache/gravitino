/*
<<<<<<< HEAD
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
=======
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
>>>>>>> c6f7db1
 */
package com.datastrato.graviton.trino.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTransactionHandle;

/**
 * The GravitonTransaction is used to make Graviton metadata operations transactional and wrap the
 * inner connector transaction for data access.
 */
public class GravitonTransactionHandle implements ConnectorTransactionHandle {
  ConnectorTransactionHandle internalTransactionHandle;

  @JsonCreator
  public GravitonTransactionHandle(
      @JsonProperty("internalTransactionHandle")
          ConnectorTransactionHandle internalTransactionHandler) {
    this.internalTransactionHandle = internalTransactionHandler;
  }

  @JsonProperty
  public ConnectorTransactionHandle getInternalTransactionHandle() {
    return internalTransactionHandle;
  }
}
