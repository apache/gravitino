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
package org.apache.gravitino.catalog.hudi;

import org.apache.gravitino.connector.BaseTable;
import org.apache.gravitino.connector.TableOperations;

public abstract class HudiTable extends BaseTable {

  @Override
  public TableOperations newOps() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public abstract static class Builder<T> extends BaseTableBuilder<Builder<T>, HudiTable> {
    T backendTable;

    public Builder<T> withBackendTable(T backendTable) {
      this.backendTable = backendTable;
      return this;
    }

    @Override
    protected HudiTable internalBuild() {
      return backendTable == null ? simpleBuild() : buildFromTable(backendTable);
    }

    protected abstract HudiTable simpleBuild();

    protected abstract HudiTable buildFromTable(T backendTable);
  }
}
