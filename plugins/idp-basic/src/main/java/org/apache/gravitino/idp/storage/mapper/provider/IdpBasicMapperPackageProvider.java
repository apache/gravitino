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
package org.apache.gravitino.idp.storage.mapper.provider;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.gravitino.idp.storage.gc.IdpLegacyGarbageCollectorManager;
import org.apache.gravitino.idp.storage.mapper.IdpGroupMetaMapper;
import org.apache.gravitino.idp.storage.mapper.IdpUserGroupRelMapper;
import org.apache.gravitino.idp.storage.mapper.IdpUserMetaMapper;
import org.apache.gravitino.storage.relational.mapper.provider.MapperPackageProvider;

/** Supplies built-in IdP mapper classes from the idp-basic plugin. */
public class IdpBasicMapperPackageProvider implements MapperPackageProvider {

  @Override
  public List<Class<?>> getMapperClasses() {
    IdpLegacyGarbageCollectorManager.getInstance().ensureStarted();
    return ImmutableList.of(
        IdpUserMetaMapper.class, IdpGroupMetaMapper.class, IdpUserGroupRelMapper.class);
  }
}
