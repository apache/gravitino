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

public class GravitinoSplitManager435 extends GravitinoSplitManager {

  public GravitinoSplitManager435(ConnectorSplitManager internalSplitManager) {
    super(internalSplitManager);
  }

  @Override
  protected ConnectorSplitSource createSplitSource(ConnectorSplitSource splits) {
    return new GravitinoSplitSource435(splits);
  }

  static class GravitinoSplitSource435 extends GravitinoSplitSource {

    GravitinoSplitSource435(ConnectorSplitSource connectorSplitSource) {
      super(connectorSplitSource);
    }

    @Override
    protected ConnectorSplit createSplit(ConnectorSplit split) {
      return new GravitinoSplit435(split);
    }
  }

  public static class GravitinoSplit435 extends GravitinoSplit {

    @JsonCreator
    public GravitinoSplit435(@JsonProperty(HANDLE_STRING) String handleString) {
      super(handleString);
    }

    public GravitinoSplit435(ConnectorSplit split) {
      super(split);
    }

    @Override
    public Object getInfo() {
      return getInternalHandle().getInfo();
    }
  }
}
