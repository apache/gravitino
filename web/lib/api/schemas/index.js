/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import axios from 'axios'

const Apis = {
  GET: ({ metalake, catalog }) => `/api/metalakes/${metalake}/catalogs/${catalog}/schemas`,
  GET_DETAIL: ({ metalake, catalog, schema }) => `/api/metalakes/${metalake}/catalogs/${catalog}/schemas/${schema}`
}

export const getSchemasApi = params => {
  return axios({
    url: `${Apis.GET(params)}`,
    method: 'get',
    headers: {
      Accept: 'application/vnd.gravitino.v1+json'
    }
  })
}

export const getSchemaDetailsApi = ({ metalake, catalog, schema }) => {
  return axios({
    url: `${Apis.GET_DETAIL({ metalake, catalog, schema })}`,
    method: 'get',
    headers: {
      Accept: 'application/vnd.gravitino.v1+json'
    }
  })
}
