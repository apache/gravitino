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
    )}/schemas/${encodeURIComponent(schema)}/functions`,
  GET_DETAIL: ({ metalake, catalog, schema, functionName }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/catalogs/${encodeURIComponent(
      catalog
    )}/schemas/${encodeURIComponent(schema)}/functions/${encodeURIComponent(functionName)}`
}

export const getFunctionsApi = ({ metalake, catalog, schema, details = true }) => {
  return defHttp.get({
    url: `${Apis.GET({ metalake, catalog, schema })}`,
    params: { details }
  })
}

export const getFunctionDetailsApi = ({ metalake, catalog, schema, functionName }) => {
  return defHttp.get({
    url: `${Apis.GET_DETAIL({ metalake, catalog, schema, functionName })}`
  })
}
