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
import { Api } from '@mui/icons-material'

const Apis = {
  GET: ({ metalake }) => `/api/metalakes/${encodeURIComponent(metalake)}/catalogs?details=true`,
  GET_DETAIL: ({ metalake, catalog }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/catalogs/${encodeURIComponent(catalog)}`,
  CREATE: ({ metalake }) => `/api/metalakes/${encodeURIComponent(metalake)}/catalogs`,
  UPDATE: ({ metalake, catalog }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/catalogs/${encodeURIComponent(catalog)}`,
  DELETE: ({ metalake, catalog }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/catalogs/${encodeURIComponent(catalog)}?force=true`,
  UPDATEINUSE: ({ metalake, catalog }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/catalogs/${encodeURIComponent(catalog)}`
}

export const getCatalogsApi = params => {
  return defHttp.get({
    url: `${Apis.GET(params)}`
  })
}

export const getCatalogDetailsApi = ({ metalake, catalog }) => {
  return defHttp.get({
    url: `${Apis.GET_DETAIL({ metalake, catalog })}`
  })
}

export const createCatalogApi = ({ data, metalake }) => {
  return defHttp.post({
    url: `${Apis.CREATE({ metalake })}`,
    data
  })
}

export const updateCatalogApi = ({ metalake, catalog, data }) => {
  return defHttp.put({
    url: `${Apis.UPDATE({ metalake, catalog })}`,
    data
  })
}

export const deleteCatalogApi = ({ metalake, catalog }) => {
  return defHttp.delete({
    url: `${Apis.DELETE({ metalake, catalog })}`
  })
}

export const switchInUseApi = ({ metalake, catalog, isInUse }) => {
  return defHttp.patch({
    url: `${Apis.UPDATEINUSE({ metalake, catalog })}`,
    data: { inUse: isInUse }
  })
}
