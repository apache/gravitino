/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import axios from 'axios'

const Apis = {
  GET: '/api/metalakes',
  CREATE: '/api/metalakes',
  DELETE: '/api/metalakes',
  UPDATE: '/api/metalakes'
}

export const getMetalakesApi = () => {
  return axios({
    url: `${Apis.GET}`,
    method: 'get',
    headers: {
      Accept: 'application/vnd.gravitino.v1+json'
    }
  })
}

export const createMetalakeApi = data => {
  return axios({
    url: `${Apis.CREATE}`,
    method: 'post',
    data,
    headers: {
      Accept: 'application/vnd.gravitino.v1+json'
    }
  })
}

export const deleteMetalakeApi = name => {
  return axios({
    url: `${Apis.DELETE}/${name}`,
    method: 'delete',
    headers: {
      Accept: 'application/vnd.gravitino.v1+json'
    }
  })
}

export const updateMetalakeApi = ({ name, data }) => {
  return axios({
    url: `${Apis.UPDATE}/${name}`,
    method: 'put',
    headers: {
      Accept: 'application/vnd.gravitino.v1+json'
    },
    data
  })
}
