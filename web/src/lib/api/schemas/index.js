/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import { defHttp } from '@/lib/utils/axios'

const Apis = {
  GET: ({ metalake, catalog }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/catalogs/${encodeURIComponent(catalog)}/schemas`,
  GET_DETAIL: ({ metalake, catalog, schema }) =>
    `/api/metalakes/${encodeURIComponent(metalake)}/catalogs/${encodeURIComponent(
      catalog
    )}/schemas/${encodeURIComponent(schema)}`
}

export const getSchemasApi = params => {
  return defHttp.get({
    url: `${Apis.GET(params)}`
  })
}

export const getSchemaDetailsApi = ({ metalake, catalog, schema }) => {
  return defHttp.get({
    url: `${Apis.GET_DETAIL({ metalake, catalog, schema })}`
  })
}
