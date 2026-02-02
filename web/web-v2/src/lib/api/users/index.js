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
  GET: ({ metalake }) => `/api/metalakes/${encodeURIComponent(metalake)}/users`,
  GET_DETAIL: ({ user }) => `/api/users/${encodeURIComponent(user)}`,
  CREATE: ({ metalake }) => `/api/metalakes/${encodeURIComponent(metalake)}/users`,
  DELETE: ({ metalake, user }) => `/api/metalakes/${encodeURIComponent(metalake)}/users/${encodeURIComponent(user)}`,
  GRANT_ROLES: ({ metalake, user }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/permissions/users/${encodeURIComponent(user)}/grant`,
  REVOKE_ROLES: ({ metalake, user }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/permissions/users/${encodeURIComponent(user)}/revoke`
}

export const getUsersApi = ({ metalake, details }) => {
  return defHttp.get({
    url: Apis.GET({ metalake }),
    params: { details }
  })
}

export const createUserApi = ({ metalake, data }) => {
  return defHttp.post({
    url: Apis.CREATE({ metalake }),
    data
  })
}

export const deleteUserApi = ({ metalake, user }) => {
  return defHttp.delete({
    url: Apis.DELETE({ metalake, user })
  })
}

export const grantRolesForUserApi = ({ metalake, user, data }) => {
  return defHttp.put({
    url: Apis.GRANT_ROLES({ metalake, user }),
    data
  })
}

export const revokeRolesForUserApi = ({ metalake, user, data }) => {
  return defHttp.put({
    url: Apis.REVOKE_ROLES({ metalake, user }),
    data
  })
}
