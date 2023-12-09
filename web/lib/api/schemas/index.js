/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import defHttp from '@/lib/api'

const Apis = {
  GET: ({ metalake, catalog }) => `/api/metalakes/${metalake}/catalogs/${catalog}/schemas`,
  GET_DETAIL: ({ metalake, catalog, schema }) => `/api/metalakes/${metalake}/catalogs/${catalog}/schemas/${schema}`
}

export const getSchemasApi = params => {
  return defHttp.request({
    url: `${Apis.GET(params)}`,
    method: 'get'
  })
}

export const getSchemaDetailsApi = ({ metalake, catalog, schema }) => {
  return defHttp.request({
    url: `${Apis.GET_DETAIL({ metalake, catalog, schema })}`,
    method: 'get'
  })
}
