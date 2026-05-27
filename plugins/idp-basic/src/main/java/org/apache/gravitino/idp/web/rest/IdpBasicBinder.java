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
package org.apache.gravitino.idp.web.rest;

import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.idp.IdpUserGroupManager;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

/** Registers built-in IdP REST dependencies for Jersey/HK2 injection. */
public class IdpBasicBinder extends AbstractBinder {

  @Override
  protected void configure() {
    GravitinoEnv gravitinoEnv = GravitinoEnv.getInstance();
    bind(IdpUserGroupManager.getInstance(gravitinoEnv.config(), gravitinoEnv.idGenerator()))
        .to(IdpUserGroupManager.class);
  }
}
