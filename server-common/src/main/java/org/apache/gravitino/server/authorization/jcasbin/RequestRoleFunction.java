/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.server.authorization.jcasbin;

import com.googlecode.aviator.runtime.type.AviatorBoolean;
import com.googlecode.aviator.runtime.type.AviatorObject;
import java.util.Map;
import java.util.Set;
import org.casbin.jcasbin.util.function.CustomFunction;

/** Matches a policy role against immutable request subjects in constant time. */
final class RequestRoleFunction extends CustomFunction {

  @Override
  public String getName() {
    return "hasRole";
  }

  @Override
  public AviatorObject call(Map<String, Object> env, AviatorObject subjects, AviatorObject role) {
    Object value = subjects.getValue(env);
    return AviatorBoolean.valueOf(
        value instanceof Set && ((Set<?>) value).contains(role.getValue(env)));
  }
}
