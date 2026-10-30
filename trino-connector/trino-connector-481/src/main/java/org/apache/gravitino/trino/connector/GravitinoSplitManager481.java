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
package org.apache.gravitino.trino.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;

/**
 * Trino 481 split manager that wraps internal splits so Gravitino can unwrap them on the workers.
 */
public class GravitinoSplitManager481 extends GravitinoSplitManager {

  /**
   * Constructs a new GravitinoSplitManager481 with the specified split manager.
   *
   * <p>internalSplitManager the internal connector split manager
   */
  public GravitinoSplitManager481(ConnectorSplitManager internalSplitManager) {
    super(internalSplitManager);
  }

  @Override
  protected ConnectorSplitSource createSplitSource(ConnectorSplitSource splits) {
    return new GravitinoSplitSource481(splits);
  }

  static class GravitinoSplitSource481 extends GravitinoSplitSource {

    GravitinoSplitSource481(ConnectorSplitSource connectorSplitSource) {
      super(connectorSplitSource);
    }

    @Override
    protected ConnectorSplit createSplit(ConnectorSplit split) {
      return new GravitinoSplit481(split);
    }
  }

  /** A Gravitino split wrapper for Trino 481. */
  public static class GravitinoSplit481 extends GravitinoSplit {

    /**
     * Constructs a new GravitinoSplit481 from a serialized handle string.
     *
     * <p>handleString the serialized handle string
     */
    @JsonCreator
    public GravitinoSplit481(@JsonProperty(HANDLE_STRING) String handleString) {
      super(handleString);
    }

    /**
     * Constructs a new GravitinoSplit481 from a ConnectorSplit.
     *
     * <p>split the internal connector split
     */
    public GravitinoSplit481(ConnectorSplit split) {
      super(split);
    }
  }
}
