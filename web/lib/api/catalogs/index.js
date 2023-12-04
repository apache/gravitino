/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import axios from 'axios'

const Apis = {
  GET: ({ metalake }) => `/api/metalakes/${metalake}/catalogs`,
  GET_DETAIL: ({ metalake, catalog }) => `/api/metalakes/${metalake}/catalogs/${catalog}`
}

export const getCatalogsApi = params => {
  return axios({
    url: `${Apis.GET(params)}`,
    method: 'get',
    headers: {
      Accept: 'application/vnd.gravitino.v1+json'
    }
  })
}

export const getCatalogDetailsApi = ({ metalake, catalog }) => {
  return axios({
    url: `${Apis.GET_DETAIL({ metalake, catalog })}`,
    method: 'get',
    headers: {
      Accept: 'application/vnd.gravitino.v1+json'
    }
  })
}
