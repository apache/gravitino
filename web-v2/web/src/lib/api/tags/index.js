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
  GET: ({ metalake }) => `/api/metalakes/${encodeURIComponent(metalake)}/tags`,
  GET_DETAIL: ({ metalake, tag }) => `/api/metalakes/${encodeURIComponent(metalake)}/tags/${encodeURIComponent(tag)}`,
  CREATE: ({ metalake }) => `/api/metalakes/${encodeURIComponent(metalake)}/tags`,
  DELETE: ({ metalake, tag }) => `/api/metalakes/${encodeURIComponent(metalake)}/tags/${encodeURIComponent(tag)}`,
  UPDATE: ({ metalake, tag }) => `/api/metalakes/${encodeURIComponent(metalake)}/tags/${encodeURIComponent(tag)}`,
  ASSOCIATE: ({ metalake, metadataObjectType, metadataObjectFullName }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/objects/${encodeURIComponent(metadataObjectType)}/${encodeURIComponent(metadataObjectFullName)}/tags`
}

export const getTagsApi = ({ metalake, details }) => {
  return defHttp.get({
    url: Apis.GET({ metalake }),
    params: { details }
  })
}

export const getTagDetailsApi = ({ metalake, tag }) => {
  return defHttp.get({
    url: Apis.GET_DETAIL({ metalake, tag })
  })
}

export const createTagApi = ({ metalake, data }) => {
  return defHttp.post({
    url: Apis.CREATE({ metalake }),
    data
  })
}

export const deleteTagApi = ({ metalake, tag }) => {
  return defHttp.delete({
    url: Apis.DELETE({ metalake, tag })
  })
}

export const updateTagApi = ({ metalake, tag, data }) => {
  return defHttp.put({
    url: Apis.UPDATE({ metalake, tag }),
    data
  })
}

export const associateTagApi = ({ metalake, metadataObjectType, metadataObjectFullName, data }) => {
  return defHttp.post({
    url: Apis.ASSOCIATE({ metalake, metadataObjectType, metadataObjectFullName }),
    data
  })
}
