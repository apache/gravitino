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

import java.util.List;
import org.casbin.jcasbin.model.Model;
import org.casbin.jcasbin.persist.Adapter;

/**
 * The {@link Adapter} in Jcasbin is used to load the policy from the Adpater when initializing the
 * {@link org.casbin.jcasbin.main.Enforcer} , and to persist the policy when the method of executing
 * the Enforcer changes the policy
 *
 * <p>GravitinoAdapter will not perform any actions because there is no need to persist Privilege.
 * All Privileges will be temporarily loaded into the Jcasbin cache when a user requests them.
 */
public class GravitinoAdapter implements Adapter {

  /** Gravitino does not require an initialization strategy when an Enforcer is instantiated */
  @Override
  public void loadPolicy(Model model) {}

  /** Gravitino does not need persistent Policy when modifying the permission policy */
  @Override
  public void savePolicy(Model model) {}

  /** Gravitino does not need persistent Policy when modifying the permission policy */
  @Override
  public void addPolicy(String jcasbinModelSection, String policyType, List<String> rule) {}

  /** Gravitino does not need persistent Policy when modifying the permission policy */
  @Override
  public void removePolicy(String jcasbinModelSection, String policyType, List<String> rule) {}

  /** Gravitino does not need persistent Policy when modifying the permission policy */
  @Override
  public void removeFilteredPolicy(
      String jcasbinModelSection,
      String policyType,
      int policyFieldIndex,
      String... policyFieldValues) {}
}
