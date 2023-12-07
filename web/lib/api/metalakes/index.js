/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import defHttp from '@/lib/api'

const Apis = {
  GET: '/api/metalakes',
  CREATE: '/api/metalakes',
  DELETE: '/api/metalakes',
  UPDATE: '/api/metalakes'
}

export const getMetalakesApi = () => {
  return defHttp.request({
    url: `${Apis.GET}`,
    method: 'get'
  })
}

export const getMetalakeDetailsApi = name => {
  return defHttp.request({
    url: `${Apis.GET}/${name}`,
    method: 'get'
  })
}

export const createMetalakeApi = data => {
  return defHttp.request({
    url: `${Apis.CREATE}`,
    method: 'post',
    data
  })
}

export const deleteMetalakeApi = name => {
  return defHttp.request({
    url: `${Apis.DELETE}/${name}`,
    method: 'delete'
  })
}

export const updateMetalakeApi = ({ name, data }) => {
  return defHttp.request({
    url: `${Apis.UPDATE}/${name}`,
    method: 'put',
    data
  })
}
