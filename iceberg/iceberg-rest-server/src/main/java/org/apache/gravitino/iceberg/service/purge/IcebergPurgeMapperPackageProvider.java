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

package org.apache.gravitino.iceberg.service.purge;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.gravitino.storage.relational.mapper.provider.MapperPackageProvider;

/**
 * Registers the Iceberg async-purge mapper into the Gravitino entity store's MyBatis configuration
 * via {@link java.util.ServiceLoader}, so the purge job store reuses the shared relational backend
 * rather than opening its own JDBC connections.
 */
public class IcebergPurgeMapperPackageProvider implements MapperPackageProvider {

  @Override
  public List<Class<?>> getMapperClasses() {
    return ImmutableList.of(IcebergPurgeJobMapper.class);
  }
}
