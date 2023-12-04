/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import axios from 'axios'

const Apis = {
  GET: ({ metalake, catalog, schema }) => `/api/metalakes/${metalake}/catalogs/${catalog}/schemas/${schema}/tables`,
  GET_DETAIL: ({ metalake, catalog, schema, table }) =>
    `/api/metalakes/${metalake}/catalogs/${catalog}/schemas/${schema}/tables/${table}`
}

export const getTablesApi = params => {
  return axios({
    url: `${Apis.GET(params)}`,
    method: 'get',
    headers: {
      Accept: 'application/vnd.gravitino.v1+json'
    }
  })
}

export const getTableDetailsApi = ({ metalake, catalog, schema, table }) => {
  return axios({
    url: `${Apis.GET_DETAIL({ metalake, catalog, schema, table })}`,
    method: 'get',
    headers: {
      Accept: 'application/vnd.gravitino.v1+json'
    }
  })
}
