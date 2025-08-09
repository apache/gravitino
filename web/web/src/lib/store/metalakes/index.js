/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { createSlice, createAsyncThunk } from '@reduxjs/toolkit'

import { to, extractPlaceholder, updateTreeData, findInTree } from '@/lib/utils'
import toast from 'react-hot-toast'

import _ from 'lodash-es'

import {
  createMetalakeApi,
  getMetalakesApi,
  deleteMetalakeApi,
  updateMetalakeApi,
  getMetalakeDetailsApi
} from '@/lib/api/metalakes'

import {
  getCatalogsApi,
  getCatalogDetailsApi,
  createCatalogApi,
  updateCatalogApi,
  deleteCatalogApi
} from '@/lib/api/catalogs'
import {
  getSchemasApi,
  getSchemaDetailsApi,
  createSchemaApi,
  updateSchemaApi,
  deleteSchemaApi
} from '@/lib/api/schemas'
import { getTablesApi, getTableDetailsApi, createTableApi, updateTableApi, deleteTableApi } from '@/lib/api/tables'
import {
  getFilesetsApi,
  getFilesetDetailsApi,
  listFilesetFilesApi,
  createFilesetApi,
  updateFilesetApi,
  deleteFilesetApi
} from '@/lib/api/filesets'
import { getTopicsApi, getTopicDetailsApi, createTopicApi, updateTopicApi, deleteTopicApi } from '@/lib/api/topics'
import {
  getModelsApi,
  getModelDetailsApi,
  registerModelApi,
  updateModelApi,
  deleteModelApi,
  getModelVersionsApi,
  getVersionDetailsApi,
  linkVersionApi,
  updateVersionApi,
  deleteVersionApi
} from '@/lib/api/models'

export const fetchMetalakes = createAsyncThunk('appMetalakes/fetchMetalakes', async (params, { getState }) => {
  const [err, res] = await to(getMetalakesApi())

  if (err || !res) {
    throw new Error(err)
  }

  const { metalakes } = res

  metalakes.sort((a, b) => new Date(b.audit.createTime) - new Date(a.audit.createTime))

  return { metalakes }
})

export const createMetalake = createAsyncThunk('appMetalakes/createMetalake', async (data, { getState, dispatch }) => {
  const [err, res] = await to(createMetalakeApi(data))

  if (err || !res) {
    return { err: true }
  }

  dispatch(fetchMetalakes())

  return res.metalake
})

export const deleteMetalake = createAsyncThunk('appMetalakes/deleteMetalake', async (name, { dispatch }) => {
  const [err, res] = await to(deleteMetalakeApi(name))

  if (err || !res) {
    throw new Error(err)
  }

  dispatch(fetchMetalakes())

  return res
})

export const updateMetalake = createAsyncThunk('appMetalakes/updateMetalake', async ({ name, data }, { dispatch }) => {
  const [err, res] = await to(updateMetalakeApi({ name, data }))

  if (err || !res) {
    return { err: true }
  }

  dispatch(fetchMetalakes())

  return res
})

export const setIntoTreeNodeWithFetch = createAsyncThunk(
  'appMetalakes/setIntoTreeNodeWithFetch',
  async ({ key, reload }, { getState, dispatch }) => {
    let result = {
      key,
      data: [],
      tree: getState().metalakes.metalakeTree
    }

    const pathArr = extractPlaceholder(key)
    const [metalake, catalog, type, schema, entity] = pathArr

    if (pathArr.length === 1) {
      const [err, res] = await to(getCatalogsApi({ metalake }))

      if (err || !res) {
        throw new Error(err)
      }

      const { catalogs = [] } = res

      result.data = catalogs.map(catalogItem => {
        return {
          ...catalogItem,
          node: 'catalog',
          id: `{{${metalake}}}{{${catalogItem.name}}}{{${catalogItem.type}}}`,
          key: `{{${metalake}}}{{${catalogItem.name}}}{{${catalogItem.type}}}`,
          path: `?${new URLSearchParams({ metalake, catalog: catalogItem.name, type: catalogItem.type }).toString()}`,
          name: catalogItem.name,
          title: catalogItem.name,
          namespace: [metalake],
          schemas: [],
          children: []
        }
      })
    } else if (pathArr.length === 3) {
      const [err, res] = await to(getSchemasApi({ metalake, catalog }))

      if (err || !res) {
        throw new Error(err)
      }

      const { identifiers = [] } = res
      const expandedKeys = getState().metalakes.expandedNodes
      const loadedNodes = getState().metalakes.loadedNodes
      let reloadedEpxpandedKeys = []
      let reloadedKeys = []

      result.data = identifiers.map(schemaItem => {
        if (reload) {
          if (expandedKeys.includes(`{{${metalake}}}{{${catalog}}}{{${type}}}{{${schemaItem.name}}}`)) {
            reloadedEpxpandedKeys.push(`{{${metalake}}}{{${catalog}}}{{${type}}}{{${schemaItem.name}}}`)
          }
          if (loadedNodes.includes(`{{${metalake}}}{{${catalog}}}{{${type}}}{{${schemaItem.name}}}`)) {
            reloadedKeys.push(`{{${metalake}}}{{${catalog}}}{{${type}}}{{${schemaItem.name}}}`)
          }
        }

        return {
          ...schemaItem,
          node: 'schema',
          id: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schemaItem.name}}}`,
          key: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schemaItem.name}}}`,
          path: `?${new URLSearchParams({ metalake, catalog, type, schema: schemaItem.name }).toString()}`,
          isLeaf: reload ? false : undefined,
          name: schemaItem.name,
          title: schemaItem.name,
          tables: [],
          children: []
        }
      })
      if (reloadedEpxpandedKeys.length > 0) {
        const epxpanded = expandedKeys.filter(key => !reloadedEpxpandedKeys.includes(key))
        dispatch(resetExpandNode(epxpanded))
      }
      if (reloadedKeys.length > 0) {
        const loaded = loadedNodes.filter(key => !reloadedKeys.includes(key))
        dispatch(setLoadedNodes(loaded))
      }
    } else if (pathArr.length === 4 && type === 'relational') {
      const [err, res] = await to(getTablesApi({ metalake, catalog, schema }))

      if (err || !res) {
        throw new Error(err)
      }

      const { identifiers = [] } = res

      result.data = identifiers.map(tableItem => {
        return {
          ...tableItem,
          node: 'table',
          id: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema}}}{{${tableItem.name}}}`,
          key: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema}}}{{${tableItem.name}}}`,
          path: `?${new URLSearchParams({ metalake, catalog, type, schema, table: tableItem.name }).toString()}`,
          name: tableItem.name,
          title: tableItem.name,
          isLeaf: true,
          columns: [],
          children: []
        }
      })
    } else if (pathArr.length === 4 && type === 'fileset') {
      const [err, res] = await to(getFilesetsApi({ metalake, catalog, schema }))

      if (err || !res) {
        throw new Error(err)
      }

      const { identifiers = [] } = res

      result.data = identifiers.map(filesetItem => {
        return {
          ...filesetItem,
          node: 'fileset',
          id: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema}}}{{${filesetItem.name}}}`,
          key: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema}}}{{${filesetItem.name}}}`,
          path: `?${new URLSearchParams({ metalake, catalog, type, schema, fileset: filesetItem.name }).toString()}`,
          name: filesetItem.name,
          title: filesetItem.name,
          isLeaf: true
        }
      })
    } else if (pathArr.length === 4 && type === 'messaging') {
      const [err, res] = await to(getTopicsApi({ metalake, catalog, schema }))

      if (err || !res) {
        throw new Error(err)
      }

      const { identifiers = [] } = res

      result.data = identifiers.map(topicItem => {
        return {
          ...topicItem,
          node: 'topic',
          id: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema}}}{{${topicItem.name}}}`,
          key: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema}}}{{${topicItem.name}}}`,
          path: `?${new URLSearchParams({ metalake, catalog, type, schema, topic: topicItem.name }).toString()}`,
          name: topicItem.name,
          title: topicItem.name,
          isLeaf: true
        }
      })
    } else if (pathArr.length === 4 && type === 'model') {
      const [err, res] = await to(getModelsApi({ metalake, catalog, schema }))

      if (err || !res) {
        throw new Error(err)
      }

      const { identifiers = [] } = res

      result.data = identifiers.map(modelItem => {
        return {
          ...modelItem,
          node: 'model',
          id: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema}}}{{${modelItem.name}}}`,
          key: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema}}}{{${modelItem.name}}}`,
          path: `?${new URLSearchParams({ metalake, catalog, type, schema, model: modelItem.name }).toString()}`,
          name: modelItem.name,
          title: modelItem.name,
          isLeaf: true
        }
      })
    }

    return result
  }
)

export const getMetalakeDetails = createAsyncThunk('appMetalakes/getMetalakeDetails', async ({ metalake }) => {
  const [err, res] = await to(getMetalakeDetailsApi(metalake))

  if (err || !res) {
    throw new Error(err)
  }

  const { metalake: resMetalake } = res

  return resMetalake
})

export const fetchCatalogs = createAsyncThunk(
  'appMetalakes/fetchCatalogs',
  async ({ init, update, page, metalake }, { getState, dispatch }) => {
    if (init) {
      dispatch(resetTree())
      dispatch(resetTableData())
      dispatch(setTableLoading(true))
    }
    const [err, res] = await to(getCatalogsApi({ metalake }))
    dispatch(setTableLoading(false))

    if (err || !res) {
      dispatch(resetTableData())
      throw new Error(err)
    }

    const { catalogs = [] } = res

    const catalogsData = catalogs.map(catalog => {
      return {
        ...catalog,
        node: 'catalog',
        id: `{{${metalake}}}{{${catalog.name}}}{{${catalog.type}}}`,
        key: `{{${metalake}}}{{${catalog.name}}}{{${catalog.type}}}`,
        path: `?${new URLSearchParams({ metalake, catalog: catalog.name, type: catalog.type }).toString()}`,
        type: catalog.type,
        provider: catalog.provider,
        inUse: catalog.properties['in-use'],
        name: catalog.name,
        title: catalog.name,
        namespace: [metalake],
        schemas: [],
        children: []
      }
    })

    if (init) {
      if (update && update.catalog) {
        if (update.catalog !== update.newCatalog.name) {
          const tree = getState().metalakes.metalakeTree.map(catalog => {
            if (catalog.name === update.catalog) {
              const schemas = catalog.children
                ? catalog.children.map(schema => {
                    const tables = schema.children
                      ? schema.children.map(table => {
                          return {
                            ...table,
                            id: `{{${metalake}}}{{${update.newCatalog.name}}}{{${update.newCatalog.type}}}{{${schema.name}}}{{${table.name}}}`,
                            key: `{{${metalake}}}{{${update.newCatalog.name}}}{{${update.newCatalog.type}}}{{${schema.name}}}{{${table.name}}}`,
                            path: `?${new URLSearchParams({
                              metalake,
                              catalog: update.newCatalog.name,
                              type: update.newCatalog.type,
                              schema: schema.name,
                              table: table.name
                            }).toString()}`
                          }
                        })
                      : []

                    return {
                      ...schema,
                      id: `{{${metalake}}}{{${update.newCatalog.name}}}{{${update.newCatalog.type}}}{{${schema.name}}}`,
                      key: `{{${metalake}}}{{${update.newCatalog.name}}}{{${update.newCatalog.type}}}{{${schema.name}}}`,
                      path: `?${new URLSearchParams({
                        metalake,
                        catalog: update.newCatalog.name,
                        type: update.newCatalog.type,
                        schema: schema.name
                      }).toString()}`,
                      tables: tables,
                      children: tables
                    }
                  })
                : []

              return {
                ...catalog,
                id: `{{${metalake}}}{{${update.newCatalog.name}}}{{${update.newCatalog.type}}}`,
                key: `{{${metalake}}}{{${update.newCatalog.name}}}{{${update.newCatalog.type}}}`,
                path: `?${new URLSearchParams({
                  metalake,
                  catalog: update.newCatalog.name,
                  type: update.newCatalog.type
                }).toString()}`,
                type: update.newCatalog.type,
                provider: update.newCatalog.provider,
                name: update.newCatalog.name,
                title: update.newCatalog.name,
                schemas: schemas,
                children: schemas
              }
            } else {
              return { ...catalog }
            }
          })

          dispatch(setMetalakeTree(tree))

          const expandedNodes = getState().metalakes.expandedNodes.map(node => {
            const [metalake, catalog, schema, table] = extractPlaceholder(node)
            if (catalog === update.catalog) {
              const updatedNode = `{{${metalake}}}{{${update.newCatalog.name}}}{{${update.newCatalog.type}}}${
                schema ? `{{${schema}}}` : ''
              }${table ? `{{${table}}}` : ''}`

              return updatedNode
            }

            return node
          })

          const expanded = expandedNodes.filter(i => !i.startsWith(`{{${metalake}}}{{${update.catalog}}}`))
          dispatch(setExpanded(expanded))

          const loadedNodes = getState().metalakes.loadedNodes.map(node => {
            const [metalake, catalog, schema, table] = extractPlaceholder(node)
            if (catalog === update.catalog) {
              const updatedNode = `{{${metalake}}}{{${update.newCatalog.name}}}{{${update.newCatalog.type}}}${
                schema ? `{{${schema}}}` : ''
              }${table ? `{{${table}}}` : ''}`

              return updatedNode
            }

            return node
          })

          const loaded = loadedNodes.filter(i => !i.startsWith(`{{${metalake}}}{{${update.catalog}}}`))
          dispatch(setLoadedNodes(loaded))
        }
      } else {
        const mergedTree = _.values(
          _.merge(_.keyBy(catalogsData, 'key'), _.keyBy(getState().metalakes.metalakeTree, 'key'))
        )
        dispatch(setMetalakeTree(mergedTree))
      }
    }

    return {
      catalogs: catalogsData,
      page,
      init
    }
  }
)

export const getCatalogDetails = createAsyncThunk('appMetalakes/getCatalogDetails', async ({ metalake, catalog }) => {
  const [err, res] = await to(getCatalogDetailsApi({ metalake, catalog }))

  if (err || !res) {
    throw new Error(err)
  }

  const { catalog: resCatalog } = res

  return resCatalog
})

export const createCatalog = createAsyncThunk(
  'appMetalakes/createCatalog',
  async ({ data, metalake }, { dispatch }) => {
    dispatch(setTableLoading(true))
    const [err, res] = await to(createCatalogApi({ data, metalake }))
    dispatch(setTableLoading(false))

    if (err || !res) {
      return { err: true }
    }

    const { catalog: catalogItem } = res

    const catalogData = {
      ...catalogItem,
      node: 'catalog',
      id: `{{${metalake}}}{{${catalogItem.name}}}{{${catalogItem.type}}}`,
      key: `{{${metalake}}}{{${catalogItem.name}}}{{${catalogItem.type}}}`,
      path: `?${new URLSearchParams({ metalake, catalog: catalogItem.name, type: catalogItem.type }).toString()}`,
      type: catalogItem.type,
      provider: catalogItem.provider,
      name: catalogItem.name,
      title: catalogItem.name,
      namespace: [metalake],
      schemas: [],
      children: []
    }

    dispatch(fetchCatalogs({ metalake, init: true }))

    dispatch(addCatalogToTree(catalogData))

    return catalogData
  }
)

export const updateCatalog = createAsyncThunk(
  'appMetalakes/updateCatalog',
  async ({ metalake, catalog, data }, { dispatch }) => {
    const [err, res] = await to(updateCatalogApi({ metalake, catalog, data }))
    if (err || !res) {
      return { err: true }
    }
    dispatch(fetchCatalogs({ metalake, update: { catalog, newCatalog: res.catalog }, init: true }))

    return res.catalog
  }
)

export const deleteCatalog = createAsyncThunk(
  'appMetalakes/deleteCatalog',
  async ({ metalake, catalog, type }, { dispatch }) => {
    dispatch(setTableLoading(true))
    const [err, res] = await to(deleteCatalogApi({ metalake, catalog }))
    dispatch(setTableLoading(false))

    if (err || !res) {
      throw new Error(err)
    }

    dispatch(fetchCatalogs({ metalake, catalog, page: 'metalakes', init: true }))

    dispatch(removeCatalogFromTree(`{{${metalake}}}{{${catalog}}}{{${type}}}`))

    return res
  }
)

export const fetchSchemas = createAsyncThunk(
  'appMetalakes/fetchSchemas',
  async ({ init, page, metalake, catalog, type }, { getState, dispatch }) => {
    if (init) {
      dispatch(setTableLoading(true))
    }
    const [err, res] = await to(getSchemasApi({ metalake, catalog }))
    dispatch(setTableLoading(false))

    if (err || !res) {
      dispatch(resetTableData())
      throw new Error(err)
    }

    const { identifiers = [] } = res

    const schemas = identifiers.map(schema => {
      const schemaItem = findInTree(
        getState().metalakes.metalakeTree,
        'key',
        `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema.name}}}`
      )

      return {
        ...schema,
        node: 'schema',
        id: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema.name}}}`,
        key: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema.name}}}`,
        path: `?${new URLSearchParams({ metalake, catalog, type, schema: schema.name }).toString()}`,
        name: schema.name,
        title: schema.name,
        tables: schemaItem ? schemaItem.children : [],
        children: schemaItem ? schemaItem.children : []
      }
    })

    if (init && getState().metalakes.loadedNodes.includes(`{{${metalake}}}{{${catalog}}}{{${type}}}`)) {
      dispatch(
        setIntoTreeNodes({
          key: `{{${metalake}}}{{${catalog}}}{{${type}}}`,
          data: schemas,
          tree: getState().metalakes.metalakeTree
        })
      )
    }

    dispatch(setExpandedNodes([`{{${metalake}}}`, `{{${metalake}}}{{${catalog}}}{{${type}}}`]))

    return { schemas, page, init }
  }
)

export const getSchemaDetails = createAsyncThunk(
  'appMetalakes/getSchemaDetails',
  async ({ metalake, catalog, schema }) => {
    const [err, res] = await to(getSchemaDetailsApi({ metalake, catalog, schema }))

    if (err || !res) {
      throw new Error(err)
    }

    const { schema: resSchema } = res

    return resSchema
  }
)

export const createSchema = createAsyncThunk(
  'appMetalakes/createSchema',
  async ({ data, metalake, catalog, type }, { dispatch }) => {
    dispatch(setTableLoading(true))
    const [err, res] = await to(createSchemaApi({ data, metalake, catalog }))
    dispatch(setTableLoading(false))

    if (err || !res) {
      return { err: true }
    }

    const { schema: schemaItem } = res

    const schemaData = {
      ...schemaItem,
      node: 'schema',
      id: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schemaItem.name}}}`,
      key: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schemaItem.name}}}`,
      path: `?${new URLSearchParams({ metalake, catalog, type, schema: schemaItem.name }).toString()}`,
      name: schemaItem.name,
      title: schemaItem.name,
      tables: [],
      children: []
    }

    dispatch(fetchSchemas({ metalake, catalog, type, init: true }))

    return schemaData
  }
)

export const updateSchema = createAsyncThunk(
  'appMetalakes/updateSchema',
  async ({ metalake, catalog, type, schema, data }, { dispatch }) => {
    const [err, res] = await to(updateSchemaApi({ metalake, catalog, schema, data }))
    if (err || !res) {
      return { err: true }
    }
    dispatch(fetchSchemas({ metalake, catalog, type, init: true }))

    return res.schema
  }
)

export const deleteSchema = createAsyncThunk(
  'appMetalakes/deleteSchema',
  async ({ metalake, catalog, type, schema }, { dispatch }) => {
    dispatch(setTableLoading(true))
    const [err, res] = await to(deleteSchemaApi({ metalake, catalog, schema }))
    dispatch(setTableLoading(false))

    if (err || !res) {
      throw new Error(err)
    }

    dispatch(fetchSchemas({ metalake, catalog, type, page: 'catalogs', init: true }))

    return res
  }
)

export const fetchTables = createAsyncThunk(
  'appMetalakes/fetchTables',
  async ({ init, page, metalake, catalog, schema }, { getState, dispatch }) => {
    if (init) {
      dispatch(setTableLoading(true))
    }
    const [err, res] = await to(getTablesApi({ metalake, catalog, schema }))
    dispatch(setTableLoading(false))

    if (init && (err || !res)) {
      dispatch(resetTableData())
      throw new Error(err)
    }

    const { identifiers = [] } = res

    const tables = identifiers.map(table => {
      return {
        ...table,
        node: 'table',
        id: `{{${metalake}}}{{${catalog}}}{{${'relational'}}}{{${schema}}}{{${table.name}}}`,
        key: `{{${metalake}}}{{${catalog}}}{{${'relational'}}}{{${schema}}}{{${table.name}}}`,
        path: `?${new URLSearchParams({
          metalake,
          catalog,
          type: 'relational',
          schema,
          table: table.name
        }).toString()}`,
        name: table.name,
        title: table.name,
        isLeaf: true,
        columns: [],
        children: []
      }
    })

    if (
      init &&
      getState().metalakes.loadedNodes.includes(`{{${metalake}}}{{${catalog}}}{{${'relational'}}}{{${schema}}}`)
    ) {
      dispatch(
        setIntoTreeNodes({
          key: `{{${metalake}}}{{${catalog}}}{{${'relational'}}}{{${schema}}}`,
          data: tables,
          tree: getState().metalakes.metalakeTree
        })
      )
    }

    dispatch(
      setExpandedNodes([
        `{{${metalake}}}`,
        `{{${metalake}}}{{${catalog}}}{{${'relational'}}}`,
        `{{${metalake}}}{{${catalog}}}{{${'relational'}}}{{${schema}}}`
      ])
    )

    return { tables, page, init }
  }
)

export const getTableDetails = createAsyncThunk(
  'appMetalakes/getTableDetails',
  async ({ init, metalake, catalog, schema, table }, { getState, dispatch }) => {
    dispatch(resetTableData())
    if (init) {
      dispatch(setTableLoading(true))
    }
    const [err, res] = await to(getTableDetailsApi({ metalake, catalog, schema, table }))
    dispatch(setTableLoading(false))

    if (err || !res) {
      dispatch(resetTableData())
      throw new Error(err)
    }

    const { table: resTable } = res

    const { distribution, sortOrders = [], partitioning = [], indexes = [] } = resTable

    const tableProps = [
      {
        type: 'partitioning',
        icon: 'tabler:circle-letter-p-filled',
        items: partitioning.map(i => {
          let fields = i.fieldName || []
          let sub = ''
          let last = i.fieldName

          switch (i.strategy) {
            case 'bucket':
              sub = `[${i.numBuckets}]`
              fields = i.fieldNames
              last = i.fieldNames.map(v => v[0]).join(',')
              break
            case 'truncate':
              sub = `[${i.width}]`
              break
            case 'list':
              fields = i.fieldNames
              last = i.fieldNames.map(v => v[0]).join(',')
              break
            case 'function':
              sub = `[${i.funcName}]`
              fields = i.funcArgs.map(v => v.fieldName)
              last = fields.join(',')
              break
            default:
              break
          }

          return {
            strategy: i.strategy,
            numBuckets: i.numBuckets,
            width: i.width,
            funcName: i.funcName,
            fields,
            text: `${i.strategy}${sub}(${last})`
          }
        })
      },
      {
        type: 'sortOrders',
        icon: 'mdi:letter-s-circle',
        items: sortOrders.map(i => {
          return {
            fields: i.sortTerm.fieldName,
            dir: i.direction,
            no: i.nullOrdering,
            text: `${i.sortTerm.fieldName[0]} ${i.direction} ${i.nullOrdering}`
          }
        })
      },
      {
        type: 'distribution',
        icon: 'tabler:circle-letter-d-filled',
        items: distribution.funcArgs.map(i => {
          return {
            fields: i.fieldName,
            number: distribution.number,
            strategy: distribution.strategy,
            text:
              distribution.strategy === ''
                ? ``
                : `${distribution.strategy}${
                    distribution.number === 0 ? '' : `[${distribution.number}]`
                  }(${i.fieldName.join(',')})`
          }
        })
      },
      {
        type: 'indexes',
        icon: 'mdi:letter-i-circle',
        items: indexes.map(i => {
          return {
            fields: i.fieldNames,
            name: i.name,
            indexType: i.indexType,
            text: `${i.name}(${i.fieldNames.map(v => v.join('.')).join(',')})`
          }
        })
      }
    ]

    dispatch(setTableProps(tableProps))

    dispatch(
      setExpandedNodes([
        `{{${metalake}}}`,
        `{{${metalake}}}{{${catalog}}}{{${'relational'}}}`,
        `{{${metalake}}}{{${catalog}}}{{${'relational'}}}{{${schema}}}`
      ])
    )

    return resTable
  }
)

export const createTable = createAsyncThunk(
  'appMetalakes/createTable',
  async ({ data, metalake, catalog, type, schema }, { dispatch }) => {
    dispatch(setTableLoading(true))
    const [err, res] = await to(createTableApi({ data, metalake, catalog, schema }))
    dispatch(setTableLoading(false))

    if (err || !res) {
      return { err: true }
    }

    const { table: tableItem } = res

    const tableData = {
      ...tableItem,
      node: 'table',
      id: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema}}}{{${tableItem.name}}}`,
      key: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema}}}{{${tableItem.name}}}`,
      path: `?${new URLSearchParams({ metalake, catalog, type, schema, table: tableItem.name }).toString()}`,
      name: tableItem.name,
      title: tableItem.name,
      tables: [],
      children: []
    }

    dispatch(fetchTables({ metalake, catalog, schema, type, init: true }))

    return tableData
  }
)

export const updateTable = createAsyncThunk(
  'appMetalakes/updateTable',
  async ({ metalake, catalog, type, schema, table, data }, { dispatch }) => {
    const [err, res] = await to(updateTableApi({ metalake, catalog, schema, table, data }))
    if (err || !res) {
      return { err: true }
    }
    dispatch(fetchTables({ metalake, catalog, type, schema, init: true }))

    return res.table
  }
)

export const deleteTable = createAsyncThunk(
  'appMetalakes/deleteTable',
  async ({ metalake, catalog, type, schema, table }, { dispatch }) => {
    dispatch(setTableLoading(true))
    const [err, res] = await to(deleteTableApi({ metalake, catalog, schema, table }))
    dispatch(setTableLoading(false))

    if (err || !res) {
      throw new Error(err)
    }

    dispatch(fetchTables({ metalake, catalog, type, schema, page: 'schemas', init: true }))

    return res
  }
)

export const fetchFilesets = createAsyncThunk(
  'appMetalakes/fetchFilesets',
  async ({ init, page, metalake, catalog, schema }, { getState, dispatch }) => {
    if (init) {
      dispatch(setTableLoading(true))
    }

    const [err, res] = await to(getFilesetsApi({ metalake, catalog, schema }))
    dispatch(setTableLoading(false))

    if (init && (err || !res)) {
      dispatch(resetTableData())
      throw new Error(err)
    }

    const { identifiers = [] } = res

    const filesets = identifiers.map(fileset => {
      return {
        ...fileset,
        node: 'fileset',
        id: `{{${metalake}}}{{${catalog}}}{{${'fileset'}}}{{${schema}}}{{${fileset.name}}}`,
        key: `{{${metalake}}}{{${catalog}}}{{${'fileset'}}}{{${schema}}}{{${fileset.name}}}`,
        path: `?${new URLSearchParams({
          metalake,
          catalog,
          type: 'fileset',
          schema,
          fileset: fileset.name
        }).toString()}`,
        name: fileset.name,
        title: fileset.name,
        isLeaf: true
      }
    })

    if (
      init &&
      getState().metalakes.loadedNodes.includes(`{{${metalake}}}{{${catalog}}}{{${'fileset'}}}{{${schema}}}`)
    ) {
      dispatch(
        setIntoTreeNodes({
          key: `{{${metalake}}}{{${catalog}}}{{${'fileset'}}}{{${schema}}}`,
          data: filesets,
          tree: getState().metalakes.metalakeTree
        })
      )
    }

    dispatch(
      setExpandedNodes([
        `{{${metalake}}}`,
        `{{${metalake}}}{{${catalog}}}{{${'fileset'}}}`,
        `{{${metalake}}}{{${catalog}}}{{${'fileset'}}}{{${schema}}}`
      ])
    )

    return { filesets, page, init }
  }
)

export const getFilesetDetails = createAsyncThunk(
  'appMetalakes/getFilesetDetails',
  async ({ init, metalake, catalog, schema, fileset }, { getState, dispatch }) => {
    dispatch(resetTableData())
    if (init) {
      dispatch(setTableLoading(true))
    }
    const [err, res] = await to(getFilesetDetailsApi({ metalake, catalog, schema, fileset }))
    dispatch(setTableLoading(false))

    if (err || !res) {
      dispatch(resetTableData())
      throw new Error(err)
    }

    const { fileset: resFileset } = res

    dispatch(
      setExpandedNodes([
        `{{${metalake}}}`,
        `{{${metalake}}}{{${catalog}}}{{${'fileset'}}}`,
        `{{${metalake}}}{{${catalog}}}{{${'fileset'}}}{{${schema}}}`
      ])
    )

    return resFileset
  }
)

export const createFileset = createAsyncThunk(
  'appMetalakes/createFileset',
  async ({ data, metalake, catalog, type, schema }, { dispatch }) => {
    dispatch(setTableLoading(true))
    const [err, res] = await to(createFilesetApi({ data, metalake, catalog, schema }))
    dispatch(setTableLoading(false))

    if (err || !res) {
      return { err: true }
    }

    const { fileset: filesetItem } = res

    const filesetData = {
      ...filesetItem,
      node: 'fileset',
      id: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema}}}{{${filesetItem.name}}}`,
      key: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema}}}{{${filesetItem.name}}}`,
      path: `?${new URLSearchParams({ metalake, catalog, type, schema, fileset: filesetItem.name }).toString()}`,
      name: filesetItem.name,
      title: filesetItem.name,
      tables: [],
      children: []
    }

    dispatch(fetchFilesets({ metalake, catalog, schema, type, init: true }))

    return filesetData
  }
)

export const updateFileset = createAsyncThunk(
  'appMetalakes/updateFileset',
  async ({ metalake, catalog, type, schema, fileset, data }, { dispatch }) => {
    const [err, res] = await to(updateFilesetApi({ metalake, catalog, schema, fileset, data }))
    if (err || !res) {
      return { err: true }
    }
    dispatch(fetchFilesets({ metalake, catalog, type, schema, init: true }))

    return res.fileset
  }
)

export const deleteFileset = createAsyncThunk(
  'appMetalakes/deleteFileset',
  async ({ metalake, catalog, type, schema, fileset }, { dispatch }) => {
    dispatch(setTableLoading(true))
    const [err, res] = await to(deleteFilesetApi({ metalake, catalog, schema, fileset }))
    dispatch(setTableLoading(false))

    if (err || !res) {
      throw new Error(err)
    }

    dispatch(fetchFilesets({ metalake, catalog, type, schema, page: 'schemas', init: true }))

    return res
  }
)

export const getFilesetFiles = createAsyncThunk(
  'appMetalakes/getFilesetFiles',
  async ({ metalake, catalog, schema, fileset, subPath = '/', locationName }, { getState, dispatch }) => {
    dispatch(setTableLoading(true))
    const [err, res] = await to(listFilesetFilesApi({ metalake, catalog, schema, fileset, subPath, locationName }))
    dispatch(setTableLoading(false))

    if (err || !res) {
      dispatch(resetTableData())
      throw new Error(err)
    }

    const { files = [] } = res

    dispatch(
      setExpandedNodes([
        `{{${metalake}}}`,
        `{{${metalake}}}{{${catalog}}}{{${'fileset'}}}`,
        `{{${metalake}}}{{${catalog}}}{{${'fileset'}}}{{${schema}}}`
      ])
    )

    return { files, subPath, locationName }
  }
)

export const fetchTopics = createAsyncThunk(
  'appMetalakes/fetchTopics',
  async ({ init, page, metalake, catalog, schema }, { getState, dispatch }) => {
    if (init) {
      dispatch(setTableLoading(true))
    }

    const [err, res] = await to(getTopicsApi({ metalake, catalog, schema }))
    dispatch(setTableLoading(false))

    if (init && (err || !res)) {
      dispatch(resetTableData())
      throw new Error(err)
    }

    const { identifiers = [] } = res

    const topics = identifiers.map(topic => {
      return {
        ...topic,
        node: 'topic',
        id: `{{${metalake}}}{{${catalog}}}{{${'messaging'}}}{{${schema}}}{{${topic.name}}}`,
        key: `{{${metalake}}}{{${catalog}}}{{${'messaging'}}}{{${schema}}}{{${topic.name}}}`,
        path: `?${new URLSearchParams({
          metalake,
          catalog,
          type: 'messaging',
          schema,
          topic: topic.name
        }).toString()}`,
        name: topic.name,
        title: topic.name,
        isLeaf: true
      }
    })

    if (
      init &&
      getState().metalakes.loadedNodes.includes(`{{${metalake}}}{{${catalog}}}{{${'messaging'}}}{{${schema}}}`)
    ) {
      dispatch(
        setIntoTreeNodes({
          key: `{{${metalake}}}{{${catalog}}}{{${'messaging'}}}{{${schema}}}`,
          data: topics,
          tree: getState().metalakes.metalakeTree
        })
      )
    }

    dispatch(
      setExpandedNodes([
        `{{${metalake}}}`,
        `{{${metalake}}}{{${catalog}}}{{${'messaging'}}}`,
        `{{${metalake}}}{{${catalog}}}{{${'messaging'}}}{{${schema}}}`
      ])
    )

    return { topics, page, init }
  }
)

export const getTopicDetails = createAsyncThunk(
  'appMetalakes/getTopicDetails',
  async ({ init, metalake, catalog, schema, topic }, { getState, dispatch }) => {
    dispatch(resetTableData())
    if (init) {
      dispatch(setTableLoading(true))
    }
    const [err, res] = await to(getTopicDetailsApi({ metalake, catalog, schema, topic }))
    dispatch(setTableLoading(false))

    if (err || !res) {
      dispatch(resetTableData())
      throw new Error(err)
    }

    const { topic: resTopic } = res

    dispatch(
      setExpandedNodes([
        `{{${metalake}}}`,
        `{{${metalake}}}{{${catalog}}}{{${'messaging'}}}`,
        `{{${metalake}}}{{${catalog}}}{{${'messaging'}}}{{${schema}}}`
      ])
    )

    return resTopic
  }
)

export const createTopic = createAsyncThunk(
  'appMetalakes/createTopic',
  async ({ data, metalake, catalog, type, schema }, { dispatch }) => {
    dispatch(setTableLoading(true))
    const [err, res] = await to(createTopicApi({ data, metalake, catalog, schema }))
    dispatch(setTableLoading(false))

    if (err || !res) {
      return { err: true }
    }

    const { topic: topicItem } = res

    const topicData = {
      ...topicItem,
      node: 'topic',
      id: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema}}}{{${topicItem.name}}}`,
      key: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema}}}{{${topicItem.name}}}`,
      path: `?${new URLSearchParams({ metalake, catalog, type, schema, topic: topicItem.name }).toString()}`,
      name: topicItem.name,
      title: topicItem.name,
      tables: [],
      children: []
    }

    dispatch(fetchTopics({ metalake, catalog, schema, type, init: true }))

    return topicData
  }
)

export const updateTopic = createAsyncThunk(
  'appMetalakes/updateTopic',
  async ({ metalake, catalog, type, schema, topic, data }, { dispatch }) => {
    const [err, res] = await to(updateTopicApi({ metalake, catalog, schema, topic, data }))
    if (err || !res) {
      return { err: true }
    }
    dispatch(fetchTopics({ metalake, catalog, type, schema, init: true }))

    return res.topic
  }
)

export const deleteTopic = createAsyncThunk(
  'appMetalakes/deleteTopic',
  async ({ metalake, catalog, type, schema, topic }, { dispatch }) => {
    dispatch(setTableLoading(true))
    const [err, res] = await to(deleteTopicApi({ metalake, catalog, schema, topic }))
    dispatch(setTableLoading(false))

    if (err || !res) {
      throw new Error(err)
    }

    dispatch(fetchTopics({ metalake, catalog, type, schema, page: 'topics', init: true }))

    return res
  }
)

export const fetchModels = createAsyncThunk(
  'appMetalakes/fetchModels',
  async ({ init, page, metalake, catalog, schema }, { getState, dispatch }) => {
    if (init) {
      dispatch(setTableLoading(true))
    }

    const [err, res] = await to(getModelsApi({ metalake, catalog, schema }))
    dispatch(setTableLoading(false))

    if (init && (err || !res)) {
      dispatch(resetTableData())
      throw new Error(err)
    }

    const { identifiers = [] } = res

    const models = identifiers.map(model => {
      return {
        ...model,
        node: 'model',
        id: `{{${metalake}}}{{${catalog}}}{{${'model'}}}{{${schema}}}{{${model.name}}}`,
        key: `{{${metalake}}}{{${catalog}}}{{${'model'}}}{{${schema}}}{{${model.name}}}`,
        path: `?${new URLSearchParams({
          metalake,
          catalog,
          type: 'model',
          schema,
          model: model.name
        }).toString()}`,
        name: model.name,
        title: model.name,
        isLeaf: true
      }
    })

    if (init && getState().metalakes.loadedNodes.includes(`{{${metalake}}}{{${catalog}}}{{${'model'}}}{{${schema}}}`)) {
      dispatch(
        setIntoTreeNodes({
          key: `{{${metalake}}}{{${catalog}}}{{${'model'}}}{{${schema}}}`,
          data: models,
          tree: getState().metalakes.metalakeTree
        })
      )
    }

    dispatch(
      setExpandedNodes([
        `{{${metalake}}}`,
        `{{${metalake}}}{{${catalog}}}{{${'model'}}}`,
        `{{${metalake}}}{{${catalog}}}{{${'model'}}}{{${schema}}}`
      ])
    )

    return { models, page, init }
  }
)

export const getModelDetails = createAsyncThunk(
  'appMetalakes/getModelDetails',
  async ({ init, metalake, catalog, schema, model }, { getState, dispatch }) => {
    const [err, res] = await to(getModelDetailsApi({ metalake, catalog, schema, model }))

    if (err || !res) {
      throw new Error(err)
    }

    const { model: resModel } = res

    return resModel
  }
)

export const registerModel = createAsyncThunk(
  'appMetalakes/registerModel',
  async ({ data, metalake, catalog, type, schema }, { dispatch }) => {
    dispatch(setTableLoading(true))
    const [err, res] = await to(registerModelApi({ data, metalake, catalog, schema }))
    dispatch(setTableLoading(false))

    if (err || !res) {
      return { err: true }
    }

    const { model: modelItem } = res

    const modelData = {
      ...modelItem,
      node: 'model',
      id: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema}}}{{${modelItem.name}}}`,
      key: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema}}}{{${modelItem.name}}}`,
      path: `?${new URLSearchParams({ metalake, catalog, type, schema, model: modelItem.name }).toString()}`,
      name: modelItem.name,
      title: modelItem.name,
      tables: [],
      children: []
    }

    dispatch(fetchModels({ metalake, catalog, schema, type, init: true }))

    return modelData
  }
)

export const updateModel = createAsyncThunk(
  'appMetalakes/updateModel',
  async ({ metalake, catalog, type, schema, model, data }, { dispatch }) => {
    const [err, res] = await to(updateModelApi({ metalake, catalog, schema, model, data }))
    if (err || !res) {
      return { err: true }
    }
    dispatch(fetchModels({ metalake, catalog, type, schema, init: true }))

    return res.model
  }
)

export const deleteModel = createAsyncThunk(
  'appMetalakes/deleteModel',
  async ({ metalake, catalog, type, schema, model }, { dispatch }) => {
    dispatch(setTableLoading(true))
    const [err, res] = await to(deleteModelApi({ metalake, catalog, schema, model }))
    dispatch(setTableLoading(false))

    if (err || !res) {
      throw new Error(err)
    }

    dispatch(fetchModels({ metalake, catalog, type, schema, page: 'models', init: true }))

    return res
  }
)

export const fetchModelVersions = createAsyncThunk(
  'appMetalakes/fetchModelVersions',
  async ({ init, page, metalake, catalog, schema, model }, { getState, dispatch }) => {
    if (init) {
      dispatch(setTableLoading(true))
    }

    const [err, res] = await to(getModelVersionsApi({ metalake, catalog, schema, model }))
    dispatch(setTableLoading(false))

    if (init && (err || !res)) {
      dispatch(resetTableData())
      throw new Error(err)
    }

    const { versions = [] } = res

    const versionsData = versions.map(version => {
      return {
        node: 'version',
        id: `{{${metalake}}}{{${catalog}}}{{${'model'}}}{{${schema}}}{{${model}}}{{${version}}}`,
        key: `{{${metalake}}}{{${catalog}}}{{${'model'}}}{{${schema}}}{{${model}}}{{${version}}}`,
        path: `?${new URLSearchParams({
          metalake,
          catalog,
          type: 'model',
          schema,
          model,
          version
        }).toString()}`,
        name: version,
        title: version,
        isLeaf: true
      }
    })

    return { versions: versionsData, page, init }
  }
)

export const getVersionDetails = createAsyncThunk(
  'appMetalakes/getVersionDetails',
  async ({ init, metalake, catalog, schema, model, version }, { getState, dispatch }) => {
    dispatch(resetTableData())
    if (init) {
      dispatch(setTableLoading(true))
    }
    const [err, res] = await to(getVersionDetailsApi({ metalake, catalog, schema, model, version }))
    dispatch(setTableLoading(false))

    if (err || !res) {
      dispatch(resetTableData())
      throw new Error(err)
    }

    const { modelVersion } = res

    return modelVersion
  }
)

export const linkVersion = createAsyncThunk(
  'appMetalakes/linkVersion',
  async ({ data, metalake, catalog, type, schema, model }, { dispatch }) => {
    dispatch(setTableLoading(true))
    const [err, res] = await to(linkVersionApi({ data, metalake, catalog, schema, model }))
    dispatch(setTableLoading(false))

    if (err || !res) {
      return { err: true }
    }

    const { version: versionItem } = res

    const versionData = {
      node: 'version',
      id: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema}}}{{${model}}}{{${versionItem}}}`,
      key: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema}}}{{${model}}}{{${versionItem}}}`,
      path: `?${new URLSearchParams({ metalake, catalog, type, schema, version: versionItem }).toString()}`,
      name: versionItem,
      title: versionItem,
      tables: [],
      children: []
    }

    dispatch(fetchModelVersions({ metalake, catalog, schema, type, model, init: true }))

    return versionData
  }
)

export const updateVersion = createAsyncThunk(
  'appMetalakes/updateVersion',
  async ({ metalake, catalog, type, schema, model, version, data }, { dispatch }) => {
    const [err, res] = await to(updateVersionApi({ metalake, catalog, schema, model, version, data }))
    if (err || !res) {
      return { err: true }
    }
    dispatch(fetchModelVersions({ metalake, catalog, type, schema, model, init: true }))

    return res.modelVersion
  }
)

export const deleteVersion = createAsyncThunk(
  'appMetalakes/deleteVersion',
  async ({ metalake, catalog, type, schema, model, version }, { dispatch }) => {
    dispatch(setTableLoading(true))
    const [err, res] = await to(deleteVersionApi({ metalake, catalog, schema, model, version }))
    dispatch(setTableLoading(false))

    if (err || !res) {
      throw new Error(err)
    }

    dispatch(fetchModelVersions({ metalake, catalog, type, schema, model, page: 'versions', init: true }))

    return res
  }
)

export const appMetalakesSlice = createSlice({
  name: 'appMetalakes',
  initialState: {
    metalakes: [],
    filteredMetalakes: [],
    tableData: [],
    tableProps: [],
    catalogs: [],
    schemas: [],
    tables: [],
    columns: [],
    filesets: [],
    filesetFiles: [],
    topics: [],
    models: [],
    versions: [],
    metalakeTree: [],
    loadedNodes: [],
    selectedNodes: [],
    expandedNodes: [],
    activatedDetails: null,
    tableLoading: false,
    treeLoading: false
  },
  reducers: {
    setFilteredMetalakes(state, action) {
      state.filteredMetalakes = action.payload
    },
    setMetalakeTree(state, action) {
      state.metalakeTree = action.payload
    },
    setLoadedNodes(state, action) {
      state.loadedNodes = action.payload
    },
    setSelectedNodes(state, action) {
      state.selectedNodes = action.payload
    },
    setExpanded(state, action) {
      state.expandedNodes = action.payload
    },
    setExpandedNodes(state, action) {
      const expandedNodes = JSON.parse(JSON.stringify(state.expandedNodes))
      const nodes = Array.from(new Set([...expandedNodes, action.payload].flat()))
      state.expandedNodes = nodes
    },
    removeExpandedNode(state, action) {
      const expandedNodes = state.expandedNodes.filter(i => i !== action.payload)
      state.expandedNodes = expandedNodes
    },
    resetExpandNode(state, action) {
      state.expandedNodes = action.payload || []
    },
    resetTableData(state, action) {
      state.tableData = []
      state.tableProps = []
    },
    resetTree(state, action) {
      state.metalakeTree = []
      state.expandedNodes = []
      state.selectedNodes = []
      state.loadedNodes = []

      state.tableData = []
      state.tableProps = []
      state.catalogs = []
      state.schemas = []
      state.tables = []
      state.columns = []
      state.filesets = []
      state.topics = []
      state.models = []
      state.versions = []
    },
    setTableLoading(state, action) {
      state.tableLoading = action.payload
    },
    setTreeLoading(state, action) {
      state.treeLoading = action.payload
    },
    setIntoTreeNodes(state, action) {
      const { key, data, tree } = action.payload
      state.metalakeTree = updateTreeData(tree, key, data)
    },
    addCatalogToTree(state, action) {
      const catalogIndex = state.metalakeTree.findIndex(c => c.key === action.payload.key)
      if (catalogIndex === -1) {
        state.metalakeTree.push(action.payload)
      } else {
        state.metalakeTree.splice(catalogIndex, 1, action.payload)
      }
    },
    removeCatalogFromTree(state, action) {
      state.metalakeTree = state.metalakeTree.filter(i => i.key !== action.payload)
    },
    setCatalogInUse(state, action) {
      const { name, catalogType, metalake, isInUse } = action.payload
      for (let i = 0; i < state.catalogs.length; i++) {
        if (state.catalogs[i].name === name) {
          state.catalogs[i].inUse = isInUse + ''
          state.tableData[i].inUse = isInUse + ''
          const catalogItem = state.metalakeTree[i]
          catalogItem.inUse = isInUse + ''
          state.metalakeTree.splice(i, 1, catalogItem)
          break
        }
      }
      if (!isInUse) {
        state.expandedNodes = state.expandedNodes.filter(key => !key.includes(name))
        state.loadedNodes = state.loadedNodes.filter(key => !key.includes(name))
        state.metalakeTree = updateTreeData(state.metalakeTree, `{{${metalake}}}{{${name}}}{{${catalogType}}}`, [])
      } else {
        state.metalakeTree = updateTreeData(state.metalakeTree, `{{${metalake}}}{{${name}}}{{${catalogType}}}`, null)
      }
    },
    setMetalakeInUse(state, action) {
      for (let i = 0; i < state.metalakes.length; i++) {
        if (state.metalakes[i].name === action.payload.name) {
          state.metalakes[i].properties['in-use'] = action.payload.isInUse + ''
          break
        }
      }
    },
    setTableProps(state, action) {
      state.tableProps = action.payload
    },
    resetMetalakeStore(state, action) {}
  },
  extraReducers: builder => {
    builder.addCase(fetchMetalakes.fulfilled, (state, action) => {
      state.metalakes = action.payload.metalakes
    })
    builder.addCase(fetchMetalakes.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(setIntoTreeNodeWithFetch.fulfilled, (state, action) => {
      const { key, data, tree } = action.payload

      state.metalakeTree = updateTreeData(tree, key, data)
    })
    builder.addCase(setIntoTreeNodeWithFetch.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(getMetalakeDetails.fulfilled, (state, action) => {
      state.activatedDetails = action.payload
    })
    builder.addCase(getMetalakeDetails.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(createCatalog.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(updateCatalog.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(fetchCatalogs.fulfilled, (state, action) => {
      state.catalogs = action.payload.catalogs

      if (action.payload.init) {
        state.tableData = action.payload.catalogs
      }

      if (state.metalakeTree.length === 0) {
        state.metalakeTree = action.payload.catalogs
      }
    })
    builder.addCase(fetchCatalogs.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(getCatalogDetails.fulfilled, (state, action) => {
      state.activatedDetails = action.payload
    })
    builder.addCase(getCatalogDetails.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(deleteCatalog.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(fetchSchemas.fulfilled, (state, action) => {
      state.schemas = action.payload.schemas
      if (action.payload.init) {
        state.tableData = action.payload.schemas
      }
    })
    builder.addCase(fetchSchemas.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(getSchemaDetails.fulfilled, (state, action) => {
      state.activatedDetails = action.payload
    })
    builder.addCase(getSchemaDetails.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(createSchema.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(updateSchema.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(deleteSchema.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(fetchTables.fulfilled, (state, action) => {
      state.tables = action.payload.tables
      if (action.payload.init) {
        state.tableData = action.payload.tables
      }
    })
    builder.addCase(fetchTables.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(getTableDetails.fulfilled, (state, action) => {
      state.activatedDetails = action.payload
      state.tableData = action.payload.columns || []
    })
    builder.addCase(getTableDetails.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(createTable.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(updateTable.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(deleteTable.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(fetchFilesets.fulfilled, (state, action) => {
      state.filesets = action.payload.filesets
      if (action.payload.init) {
        state.tableData = action.payload.filesets
      }
    })
    builder.addCase(fetchFilesets.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(getFilesetDetails.fulfilled, (state, action) => {
      state.activatedDetails = action.payload
      state.tableData = []
    })
    builder.addCase(getFilesetDetails.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(createFileset.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(updateFileset.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(deleteFileset.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(getFilesetFiles.fulfilled, (state, action) => {
      state.filesetFiles = action.payload.files
      state.tableData = action.payload.files
    })
    builder.addCase(getFilesetFiles.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(fetchTopics.fulfilled, (state, action) => {
      state.topics = action.payload.topics
      if (action.payload.init) {
        state.tableData = action.payload.topics
      }
    })
    builder.addCase(fetchTopics.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(getTopicDetails.fulfilled, (state, action) => {
      state.activatedDetails = action.payload
      state.tableData = []
    })
    builder.addCase(getTopicDetails.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(createTopic.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(updateTopic.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(deleteTopic.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(fetchModels.fulfilled, (state, action) => {
      state.models = action.payload.models
      if (action.payload.init) {
        state.tableData = action.payload.models
      }
    })
    builder.addCase(fetchModels.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(getModelDetails.fulfilled, (state, action) => {
      state.activatedDetails = action.payload
    })
    builder.addCase(getModelDetails.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(registerModel.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(updateModel.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(deleteModel.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(fetchModelVersions.fulfilled, (state, action) => {
      state.versions = action.payload.versions
      if (action.payload.init) {
        state.tableData = action.payload.versions
      }
    })
    builder.addCase(fetchModelVersions.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(getVersionDetails.fulfilled, (state, action) => {
      state.activatedDetails = action.payload
    })
    builder.addCase(getVersionDetails.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(linkVersion.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(updateVersion.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(deleteVersion.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
  }
})

export const {
  setFilteredMetalakes,
  setMetalakeTree,
  setSelectedNodes,
  removeExpandedNode,
  resetMetalakeStore,
  resetExpandNode,
  resetTableData,
  resetTree,
  setTableLoading,
  setTreeLoading,
  setIntoTreeNodes,
  setLoadedNodes,
  setExpanded,
  setExpandedNodes,
  addCatalogToTree,
  setCatalogInUse,
  setMetalakeInUse,
  removeCatalogFromTree,
  setTableProps
} = appMetalakesSlice.actions

export default appMetalakesSlice.reducer
