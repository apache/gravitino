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
package org.apache.gravitino.authorization.ranger;

import com.google.common.collect.Lists;
import java.util.List;

public class RangerPrivileges {
  static List<Class<? extends Enum<? extends RangerPrivilege>>> allRangerPrivileges =
      Lists.newArrayList(
          RangerPrivilege.RangerHivePrivilege.class, RangerPrivilege.RangerHdfsPrivilege.class);

  public static RangerPrivilege valueOf(String string) {
    RangerHelper.check(string != null, "Privilege name string cannot be null!");

    String strPrivilege = string.trim().toLowerCase();
    for (Class<? extends Enum<? extends RangerPrivilege>> enumClass : allRangerPrivileges) {
      for (Enum<? extends RangerPrivilege> privilege : enumClass.getEnumConstants()) {
        if (((RangerPrivilege) privilege).equalsTo(strPrivilege)) {
          return (RangerPrivilege) privilege;
        }
      }
    }
    throw new IllegalArgumentException("Unknown privilege string: " + string);
  }
}
