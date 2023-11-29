/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import axios from 'axios'

import { getCatalogsApi } from '../catalogs'
import { getSchemasApi } from '../schemas'
import { getTablesApi } from '../tables'

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

export const getMetalakeTreeFromApi = async metalake => {
  const tree = []

  const catalogsData = await getCatalogsApi({ metalake })

  const { identifiers: catalogs } = catalogsData.data

  for (const catalog of catalogs) {
    const catalogNode = {
      node: 'catalog',
      id: `${metalake}-${catalog.name}`,
      name: catalog.name,
      schemas: []
    }

    const schemasData = await getSchemasApi({ metalake, catalog: catalog.name })

    const { identifiers: schemas } = schemasData.data

    for (const schema of schemas) {
      const schemaNode = {
        node: 'schema',
        id: `${metalake}-${catalog.name}-${schema.name}`,
        name: schema.name,
        tables: []
      }

      const tablesData = await getTablesApi({ metalake, catalog: catalog.name, schema: schema.name })

      const { identifiers: tables } = tablesData.data

      for (const table of tables) {
        const tableNode = {
          node: 'table',
          id: `${metalake}-${catalog.name}-${schema.name}--${table.name}`,
          name: table.name
        }

        schemaNode.tables.push(tableNode)
      }

      catalogNode.schemas.push(schemaNode)
    }

    tree.push(catalogNode)
  }

  return tree
}
