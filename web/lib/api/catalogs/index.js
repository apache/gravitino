/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import defHttp from '@/lib/api'

const Apis = {
  GET: ({ metalake }) => `/api/metalakes/${metalake}/catalogs`,
  GET_DETAIL: ({ metalake, catalog }) => `/api/metalakes/${metalake}/catalogs/${catalog}`
}

export const getCatalogsApi = params => {
  return defHttp.request({
    url: `${Apis.GET(params)}`,
    method: 'get'
  })
}

export const getCatalogDetailsApi = ({ metalake, catalog }) => {
  return defHttp.request({
    url: `${Apis.GET_DETAIL({ metalake, catalog })}`,
    method: 'get'
  })
}
