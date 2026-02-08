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
  GET: ({ metalake, catalog, schema }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/catalogs/${encodeURIComponent(
      catalog
    )}/schemas/${encodeURIComponent(schema)}/filesets`,
  GET_DETAIL: ({ metalake, catalog, schema, fileset }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/catalogs/${encodeURIComponent(
      catalog
    )}/schemas/${encodeURIComponent(schema)}/filesets/${encodeURIComponent(fileset)}`,
  LIST_FILES: ({ metalake, catalog, schema, fileset, subPath, locationName }) => {
    const params = new URLSearchParams()
    if (subPath) params.append('sub_path', subPath)
    if (locationName) params.append('location_name', locationName)
    const queryString = params.toString()

    return `/api/metalakes/${encodeURIComponent(metalake)}/catalogs/${encodeURIComponent(
      catalog
    )}/schemas/${encodeURIComponent(schema)}/filesets/${encodeURIComponent(fileset)}/files${queryString ? `?${queryString}` : ''}`
  },
  CREATE: ({ metalake, catalog, schema }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/catalogs/${encodeURIComponent(catalog)}/schemas/${encodeURIComponent(schema)}/filesets`,
  UPDATE: ({ metalake, catalog, schema, fileset }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/catalogs/${encodeURIComponent(catalog)}/schemas/${encodeURIComponent(schema)}/filesets/${encodeURIComponent(fileset)}`,
  DELETE: ({ metalake, catalog, schema, fileset }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/catalogs/${encodeURIComponent(catalog)}/schemas/${encodeURIComponent(schema)}/filesets/${encodeURIComponent(fileset)}`
}

export const getFilesetsApi = params => {
  return defHttp.get({
    url: `${Apis.GET(params)}`
  })
}

export const getFilesetDetailsApi = ({ metalake, catalog, schema, fileset }) => {
  return defHttp.get({
    url: `${Apis.GET_DETAIL({ metalake, catalog, schema, fileset })}`
  })
}

export const listFilesetFilesApi = ({ metalake, catalog, schema, fileset, subPath = '/', locationName }) => {
  return defHttp.get({
    url: `${Apis.LIST_FILES({ metalake, catalog, schema, fileset, subPath, locationName })}`
  })
}

export const createFilesetApi = ({ metalake, catalog, schema, data }) => {
  return defHttp.post({ url: `${Apis.CREATE({ metalake, catalog, schema })}`, data })
}

export const updateFilesetApi = ({ metalake, catalog, schema, fileset, data }) => {
  return defHttp.put({ url: `${Apis.UPDATE({ metalake, catalog, schema, fileset })}`, data })
}

export const deleteFilesetApi = ({ metalake, catalog, schema, fileset }) => {
  return defHttp.delete({ url: `${Apis.DELETE({ metalake, catalog, schema, fileset })}` })
}
