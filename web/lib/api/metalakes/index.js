/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import { defHttp } from '@/lib/utils/axios'

const Apis = {
  GET: '/api/metalakes',
  CREATE: '/api/metalakes',
  DELETE: '/api/metalakes',
  UPDATE: '/api/metalakes'
}

export const getMetalakesApi = () => {
  return defHttp.get({
    url: `${Apis.GET}`
  })
}

export const getMetalakeDetailsApi = name => {
  return defHttp.get({
    url: `${Apis.GET}/${name}`
  })
}

export const createMetalakeApi = data => {
  return defHttp.post({
    url: `${Apis.CREATE}`,
    data
  })
}

export const deleteMetalakeApi = name => {
  return defHttp.delete({
    url: `${Apis.DELETE}/${name}`
  })
}

export const updateMetalakeApi = ({ name, data }) => {
  return defHttp.put({
    url: `${Apis.UPDATE}/${name}`,
    data
  })
}

export const getMetalakeDataApi = url => {
  return defHttp.get({
    url: `/api${url}`
  })
}
