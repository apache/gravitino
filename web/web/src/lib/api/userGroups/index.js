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

import { defHttp } from '@/lib/utils/axios'

const Apis = {
  GET: ({ metalake }) => `/api/metalakes/${encodeURIComponent(metalake)}/groups`,
  GET_DETAIL: ({ group }) => `/api/tags/${encodeURIComponent(group)}`,
  CREATE: ({ metalake }) => `/api/metalakes/${encodeURIComponent(metalake)}/groups`,
  DELETE: ({ metalake, group }) => `/api/metalakes/${encodeURIComponent(metalake)}/groups/${encodeURIComponent(group)}`,
  GRANT_ROLES: ({ metalake, group }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/permissions/groups/${encodeURIComponent(group)}/grant`,
  REVOKE_ROLES: ({ metalake, group }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/permissions/groups/${encodeURIComponent(group)}/revoke`
}

export const getUserGroupsApi = ({ metalake, details }) => {
  return defHttp.get({
    url: Apis.GET({ metalake }),
    params: { details }
  })
}

export const createUserGroupApi = ({ metalake, data }) => {
  return defHttp.post({
    url: Apis.CREATE({ metalake }),
    data
  })
}

export const deleteUserGroupApi = ({ metalake, group }) => {
  return defHttp.delete({
    url: Apis.DELETE({ metalake, group })
  })
}

export const grantRolesForUserGroupApi = ({ metalake, group, data }) => {
  return defHttp.put({
    url: Apis.GRANT_ROLES({ metalake, group }),
    data
  })
}

export const revokeRolesForUserGroupApi = ({ metalake, group, data }) => {
  return defHttp.put({
    url: Apis.REVOKE_ROLES({ metalake, group }),
    data
  })
}
