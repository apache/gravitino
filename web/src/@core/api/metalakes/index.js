/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import axios from 'axios'

import { MetalakesApiEnum } from 'src/@core/enums/apiEnum'

const ApiEnum = {
  ...MetalakesApiEnum
}

export const getMetalakesApi = () => {
  return axios({
    url: `${ApiEnum.GET}`,
    method: 'get',
    headers: {
      Accept: 'application/vnd.gravitino.v1+json'
    }
  })
}

export const createMetalakeApi = data => {
  return axios({
    url: `${ApiEnum.CREATE}`,
    method: 'post',
    data,
    headers: {
      Accept: 'application/vnd.gravitino.v1+json'
    }
  })
}

export const deleteMetalakeApi = name => {
  return axios({
    url: `${ApiEnum.DELETE}/${name}`,
    method: 'delete',
    headers: {
      Accept: 'application/vnd.gravitino.v1+json'
    }
  })
}

export const updateMetalakeApi = ({ name, data }) => {
  return axios({
    url: `${ApiEnum.UPDATE}/${name}`,
    method: 'put',
    headers: {
      Accept: 'application/vnd.gravitino.v1+json'
    },
    data
  })
}
