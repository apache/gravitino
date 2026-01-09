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
  switchInUseApi,
  getMetalakeDetailsApi,
  getEntityOwnerApi,
  setEntityOwnerApi,
  getCurrentEntityTagsApi,
  getCurrentEntityPoliciesApi,
  getMetadataObjectsForTagApi,
  getMetadataObjectsForPolicyApi
} from '@/lib/api/metalakes'

import {
  getCatalogsApi,
  getCatalogDetailsApi,
  createCatalogApi,
  testCatalogConnectionApi,
  updateCatalogApi,
  deleteCatalogApi,
  switchInUseCatalogApi
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

export const switchMetalakeInUse = createAsyncThunk(
  'appMetalakes/switchMetalakeInUse',
  async ({ name, isInUse }, { dispatch }) => {
    const [err, res] = await to(switchInUseApi({ name, isInUse }))

    if (err || !res) {
      throw new Error(err)
    }

    dispatch(fetchMetalakes())

    return res
  }
)

export const getCurrentEntityOwner = createAsyncThunk(
  'appMetalakes/getCurrentEntityOwner',
  async ({ metalake, metadataObjectType, metadataObjectFullName }, { dispatch }) => {
    const [err, res] = await to(getEntityOwnerApi(metalake, metadataObjectType, metadataObjectFullName))

    if (err || !res) {
      return { err: true }
    }

    return { owner: res.owner, metadataObjectType }
  }
)

export const setEntityOwner = createAsyncThunk(
  'appMetalakes/setEntityOwner',
  async ({ metalake, metadataObjectType, metadataObjectFullName, data }, { dispatch }) => {
    const [err, res] = await to(setEntityOwnerApi(metalake, metadataObjectType, metadataObjectFullName, data))
    if (err || !res) {
      return { err: true }
    }

    return res
  }
)

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
          path: `?${new URLSearchParams({ metalake, catalog: catalogItem.name, catalogType: catalogItem.type }).toString()}`,
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

      const { identifiers = [] } = res || {}
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
          path: `?${new URLSearchParams({ metalake, catalog, catalogType: type, schema: schemaItem.name }).toString()}`,
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

      const { identifiers = [] } = res || {}

      result.data = identifiers.map(tableItem => {
        return {
          ...tableItem,
          node: 'table',
          id: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema}}}{{${tableItem.name}}}`,
          key: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema}}}{{${tableItem.name}}}`,
          path: `?${new URLSearchParams({ metalake, catalog, catalogType: type, schema, table: tableItem.name }).toString()}`,
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

      const { identifiers = [] } = res || {}

      result.data = identifiers.map(filesetItem => {
        return {
          ...filesetItem,
          node: 'fileset',
          id: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema}}}{{${filesetItem.name}}}`,
          key: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema}}}{{${filesetItem.name}}}`,
          path: `?${new URLSearchParams({ metalake, catalog, catalogType: type, schema, fileset: filesetItem.name }).toString()}`,
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

      const { identifiers = [] } = res || {}

      result.data = identifiers.map(topicItem => {
        return {
          ...topicItem,
          node: 'topic',
          id: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema}}}{{${topicItem.name}}}`,
          key: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema}}}{{${topicItem.name}}}`,
          path: `?${new URLSearchParams({ metalake, catalog, catalogType: type, schema, topic: topicItem.name }).toString()}`,
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

      const { identifiers = [] } = res || {}

      result.data = identifiers.map(modelItem => {
        return {
          ...modelItem,
          node: 'model',
          id: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema}}}{{${modelItem.name}}}`,
          key: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema}}}{{${modelItem.name}}}`,
          path: `?${new URLSearchParams({ metalake, catalog, catalogType: type, schema, model: modelItem.name }).toString()}`,
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

export const getCurrentEntityTags = createAsyncThunk(
  'appMetalakes/getCurrentEntityTags',
  async ({ init, metalake, metadataObjectType, metadataObjectFullName, details }) => {
    const [err, res] = await to(getCurrentEntityTagsApi(metalake, metadataObjectType, metadataObjectFullName, details))

    if (err || !res) {
      throw new Error(err)
    }

    return { tags: res.tags, init }
  }
)

export const getCurrentEntityPolicies = createAsyncThunk(
  'appMetalakes/getCurrentEntityPolicies',
  async ({ init, metalake, metadataObjectType, metadataObjectFullName, details }) => {
    const [err, res] = await to(
      getCurrentEntityPoliciesApi({ metalake, metadataObjectType, metadataObjectFullName, details })
    )

    if (err || !res) {
      throw new Error(err)
    }

    return { policies: res.policies, init }
  }
)

export const getMetadataObjectsForTag = createAsyncThunk(
  'appMetalakes/getMetadataObjectsForTag',
  async ({ metalake, tag }) => {
    const [err, res] = await to(getMetadataObjectsForTagApi(metalake, tag))

    if (err || !res) {
      throw new Error(err)
    }

    return res.metadataObjects
  }
)

export const getMetadataObjectsForPolicy = createAsyncThunk(
  'appMetalakes/getMetadataObjectsForPolicy',
  async ({ metalake, policy }) => {
    const [err, res] = await to(getMetadataObjectsForPolicyApi(metalake, policy))

    if (err || !res) {
      throw new Error(err)
    }

    return res.metadataObjects
  }
)

export const fetchCatalogs = createAsyncThunk(
  'appMetalakes/fetchCatalogs',
  async ({ init, update, page, metalake }, { getState, dispatch }) => {
    if (init) {
      await dispatch(resetTree())
      await dispatch(resetTableData())
      await dispatch(setTableLoading(true))
    }
    const [err, res] = await to(getCatalogsApi({ metalake }))
    await dispatch(setTableLoading(false))

    if (err || !res) {
      await dispatch(resetTableData())
      throw new Error(err)
    }

    const { catalogs = [] } = res

    const catalogsData = catalogs.map(catalog => {
      return {
        ...catalog,
        node: 'catalog',
        id: `{{${metalake}}}{{${catalog.name}}}{{${catalog.type}}}`,
        key: `{{${metalake}}}{{${catalog.name}}}{{${catalog.type}}}`,
        path: `?${new URLSearchParams({ metalake, catalog: catalog.name, catalogType: catalog.type }).toString()}`,
        catalogType: catalog.type,
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
                              catalogType: update.newCatalog.type,
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
                        catalogType: update.newCatalog.type,
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
                  catalogType: update.newCatalog.type
                }).toString()}`,
                catalogType: update.newCatalog.type,
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

export const getCatalogDetails = createAsyncThunk(
  'appMetalakes/getCatalogDetails',
  async ({ init, metalake, catalog }, { dispatch }) => {
    const [err, res] = await to(getCatalogDetailsApi({ metalake, catalog }))

    if (err || !res) {
      throw new Error(err)
    }

    const { catalog: resCatalog } = res

    return { catalog: resCatalog, init }
  }
)

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
      path: `?${new URLSearchParams({ metalake, catalog: catalogItem.name, catalogType: catalogItem.type }).toString()}`,
      catalogType: catalogItem.type,
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

export const testCatalogConnection = createAsyncThunk(
  'appMetalakes/testCatalogConnection',
  async ({ data, metalake }) => {
    const [err, res] = await to(testCatalogConnectionApi({ data, metalake }))
    console.log('testCatalogConnection', err, res)

    if (err || !res) {
      return { err: true }
    }

    return res
  }
)

export const updateCatalog = createAsyncThunk(
  'appMetalakes/updateCatalog',
  async ({ init, metalake, catalog, data }, { dispatch }) => {
    const [err, res] = await to(updateCatalogApi({ metalake, catalog, data }))
    if (err || !res) {
      return { err: true }
    }
    if (init) {
      dispatch(fetchCatalogs({ metalake, update: { catalog, newCatalog: res.catalog }, init }))
    } else {
      dispatch(getCatalogDetails({ init: true, metalake, catalog }))
    }

    return res.catalog
  }
)

export const deleteCatalog = createAsyncThunk(
  'appMetalakes/deleteCatalog',
  async ({ metalake, catalog, catalogType }, { dispatch }) => {
    dispatch(setTableLoading(true))
    const [err, res] = await to(deleteCatalogApi({ metalake, catalog }))
    dispatch(setTableLoading(false))

    if (err || !res) {
      throw new Error(err)
    }

    dispatch(fetchCatalogs({ metalake, catalog, page: 'metalakes', init: true }))

    dispatch(removeCatalogFromTree(`{{${metalake}}}{{${catalog}}}{{${catalogType}}}`))

    return res
  }
)

export const switchInUseCatalog = createAsyncThunk(
  'appMetalakes/switchInUseCatalog',
  async ({ metalake, catalog, isInUse }, { dispatch }) => {
    const [err, res] = await to(switchInUseCatalogApi({ metalake, catalog, isInUse }))

    if (err || !res) {
      throw new Error(err)
    }

    dispatch(fetchCatalogs({ metalake, init: true }))

    return res
  }
)

export const fetchSchemas = createAsyncThunk(
  'appMetalakes/fetchSchemas',
  async ({ init, page, metalake, catalog, catalogType }, { getState, dispatch }) => {
    if (init) {
      await dispatch(resetTableData())
      await dispatch(setTableLoading(true))
    }
    const [err, res] = await to(getSchemasApi({ metalake, catalog }))
    await dispatch(setTableLoading(false))

    if (err || !res) {
      throw new Error(err)
    }

    const { identifiers = [] } = res || {}

    const schemas = identifiers.map(schema => {
      const schemaItem = findInTree(
        getState().metalakes.metalakeTree,
        'key',
        `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schema.name}}}`
      )

      return {
        ...schema,
        node: 'schema',
        id: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schema.name}}}`,
        key: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schema.name}}}`,
        path: `?${new URLSearchParams({ metalake, catalog, catalogType, schema: schema.name }).toString()}`,
        name: schema.name,
        title: schema.name,
        tables: schemaItem ? schemaItem.children : [],
        children: schemaItem ? schemaItem.children : []
      }
    })

    if (init) {
      const catalogKey = `{{${metalake}}}{{${catalog}}}{{${catalogType}}}`

      // Always update tree nodes when init is true
      dispatch(
        setIntoTreeNodes({
          key: catalogKey,
          data: schemas
        })
      )

      // Add catalog to loadedNodes if not already present
      if (!getState().metalakes.loadedNodes.includes(catalogKey)) {
        dispatch(setLoadedNodes([...getState().metalakes.loadedNodes, catalogKey]))
      }
    }

    dispatch(setExpandedNodes([`{{${metalake}}}`, `{{${metalake}}}{{${catalog}}}{{${catalogType}}}`]))

    return { schemas, page, init }
  }
)

export const getSchemaDetails = createAsyncThunk(
  'appMetalakes/getSchemaDetails',
  async ({ init, metalake, catalog, schema }, { dispatch }) => {
    const [err, res] = await to(getSchemaDetailsApi({ metalake, catalog, schema }))

    if (err || !res) {
      throw new Error(err)
    }

    const { schema: resSchema } = res

    return { schema: resSchema, init }
  }
)

export const createSchema = createAsyncThunk(
  'appMetalakes/createSchema',
  async ({ data, metalake, catalog, catalogType }, { dispatch }) => {
    await dispatch(setTableLoading(true))
    const [err, res] = await to(createSchemaApi({ data, metalake, catalog }))
    await dispatch(setTableLoading(false))

    if (err || !res) {
      return { err: true }
    }

    const { schema: schemaItem } = res

    const schemaData = {
      ...schemaItem,
      node: 'schema',
      id: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schemaItem.name}}}`,
      key: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schemaItem.name}}}`,
      path: `?${new URLSearchParams({ metalake, catalog, catalogType, schema: schemaItem.name }).toString()}`,
      name: schemaItem.name,
      title: schemaItem.name,
      tables: [],
      children: []
    }

    dispatch(fetchSchemas({ metalake, catalog, catalogType, init: true }))

    return schemaData
  }
)

export const updateSchema = createAsyncThunk(
  'appMetalakes/updateSchema',
  async ({ init, metalake, catalog, catalogType, schema, data }, { dispatch }) => {
    const [err, res] = await to(updateSchemaApi({ metalake, catalog, schema, data }))
    if (err || !res) {
      return { err: true }
    }
    if (init) {
      dispatch(getSchemaDetails({ init, metalake, catalog, schema }))
    } else {
      dispatch(fetchSchemas({ metalake, catalog, catalogType, init: true }))
    }

    return res.schema
  }
)

export const deleteSchema = createAsyncThunk(
  'appMetalakes/deleteSchema',
  async ({ metalake, catalog, catalogType, schema }, { dispatch }) => {
    await dispatch(setTableLoading(true))
    const [err, res] = await to(deleteSchemaApi({ metalake, catalog, schema }))
    await dispatch(setTableLoading(false))

    if (err || !res) {
      throw new Error(err)
    }

    dispatch(fetchSchemas({ metalake, catalog, catalogType, page: 'catalogs', init: true }))

    return res
  }
)

export const fetchTables = createAsyncThunk(
  'appMetalakes/fetchTables',
  async ({ init, page, metalake, catalog, schema }, { getState, dispatch }) => {
    if (init) {
      await dispatch(resetTableData())
      await dispatch(setTableLoading(true))
    }
    const [err, res] = await to(getTablesApi({ metalake, catalog, schema }))
    await dispatch(setTableLoading(false))

    if (init && (err || !res)) {
      dispatch(resetTableData())
      throw new Error(err)
    }

    const { identifiers = [] } = res || {}

    const tables = identifiers.map(table => {
      return {
        ...table,
        node: 'table',
        id: `{{${metalake}}}{{${catalog}}}{{${'relational'}}}{{${schema}}}{{${table.name}}}`,
        key: `{{${metalake}}}{{${catalog}}}{{${'relational'}}}{{${schema}}}{{${table.name}}}`,
        path: `?${new URLSearchParams({
          metalake,
          catalog,
          catalogType: 'relational',
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
          data: tables
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
    if (init) {
      await dispatch(resetTableData())
      await dispatch(setTableLoading(true))
    }
    const [err, res] = await to(getTableDetailsApi({ metalake, catalog, schema, table }))
    await dispatch(setTableLoading(false))
    if (err || !res) {
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
          const fields =
            i.sortTerm?.type === 'field' ? i.sortTerm.fieldName : i.sortTerm?.funcArgs.find(f => f.fieldName)?.fieldName

          return {
            fields: fields,
            dir: i.direction,
            no: i.nullOrdering,
            text: `${fields[0]} ${i.direction} ${i.nullOrdering}`
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

    return { table: resTable, init }
  }
)

export const createTable = createAsyncThunk(
  'appMetalakes/createTable',
  async ({ data, metalake, catalog, catalogType, schema }, { dispatch }) => {
    await dispatch(setTableLoading(true))
    const [err, res] = await to(createTableApi({ data, metalake, catalog, schema }))
    await dispatch(setTableLoading(false))

    if (err || !res) {
      return { err: true }
    }

    const { table: tableItem } = res

    const tableData = {
      ...tableItem,
      node: 'table',
      id: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schema}}}{{${tableItem.name}}}`,
      key: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schema}}}{{${tableItem.name}}}`,
      path: `?${new URLSearchParams({ metalake, catalog, catalogType, schema, table: tableItem.name }).toString()}`,
      name: tableItem.name,
      title: tableItem.name,
      tables: [],
      children: []
    }

    dispatch(fetchTables({ metalake, catalog, schema, catalogType, init: true }))

    return tableData
  }
)

export const updateTable = createAsyncThunk(
  'appMetalakes/updateTable',
  async ({ init, metalake, catalog, catalogType, schema, table, data }, { dispatch }) => {
    const [err, res] = await to(updateTableApi({ metalake, catalog, schema, table, data }))
    if (err || !res) {
      return { err: true }
    }

    if (init) {
      await dispatch(getTableDetails({ init, metalake, catalog, catalogType, schema, table }))
    } else {
      await dispatch(fetchTables({ metalake, catalog, catalogType, schema, page: 'schemas', init: true }))
    }

    return res.table
  }
)

export const deleteTable = createAsyncThunk(
  'appMetalakes/deleteTable',
  async ({ metalake, catalog, catalogType, schema, table }, { dispatch }) => {
    await dispatch(setTableLoading(true))
    const [err, res] = await to(deleteTableApi({ metalake, catalog, schema, table }))
    await dispatch(setTableLoading(false))

    if (err || !res) {
      throw new Error(err)
    }

    await dispatch(fetchTables({ metalake, catalog, catalogType, schema, page: 'schemas', init: true }))

    return res
  }
)

export const fetchFilesets = createAsyncThunk(
  'appMetalakes/fetchFilesets',
  async ({ init, page, metalake, catalog, schema }, { getState, dispatch }) => {
    if (init) {
      await dispatch(resetTableData())
      await dispatch(setTableLoading(true))
    }

    const [err, res] = await to(getFilesetsApi({ metalake, catalog, schema }))
    await dispatch(setTableLoading(false))

    if (init && (err || !res)) {
      await dispatch(resetTableData())
      throw new Error(err)
    }

    const { identifiers = [] } = res || {}

    const filesets = identifiers.map(fileset => {
      return {
        ...fileset,
        node: 'fileset',
        id: `{{${metalake}}}{{${catalog}}}{{${'fileset'}}}{{${schema}}}{{${fileset.name}}}`,
        key: `{{${metalake}}}{{${catalog}}}{{${'fileset'}}}{{${schema}}}{{${fileset.name}}}`,
        path: `?${new URLSearchParams({
          metalake,
          catalog,
          catalogType: 'fileset',
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
          data: filesets
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
    if (init) {
      await dispatch(resetTableData())
      await dispatch(setTableLoading(true))
    }
    const [err, res] = await to(getFilesetDetailsApi({ metalake, catalog, schema, fileset }))
    init && (await dispatch(setTableLoading(false)))

    if (err || !res) {
      throw new Error(err)
    }

    const { fileset: resFileset } = res

    await dispatch(
      setExpandedNodes([
        `{{${metalake}}}`,
        `{{${metalake}}}{{${catalog}}}{{${'fileset'}}}`,
        `{{${metalake}}}{{${catalog}}}{{${'fileset'}}}{{${schema}}}`
      ])
    )

    return { fileset: resFileset, init }
  }
)

export const createFileset = createAsyncThunk(
  'appMetalakes/createFileset',
  async ({ data, metalake, catalog, catalogType, schema }, { dispatch }) => {
    await dispatch(setTableLoading(true))
    const [err, res] = await to(createFilesetApi({ data, metalake, catalog, schema }))
    await dispatch(setTableLoading(false))

    if (err || !res) {
      return { err: true }
    }

    const { fileset: filesetItem } = res

    const filesetData = {
      ...filesetItem,
      node: 'fileset',
      id: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schema}}}{{${filesetItem.name}}}`,
      key: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schema}}}{{${filesetItem.name}}}`,
      path: `?${new URLSearchParams({ metalake, catalog, catalogType, schema, fileset: filesetItem.name }).toString()}`,
      name: filesetItem.name,
      title: filesetItem.name,
      tables: [],
      children: []
    }

    await dispatch(fetchFilesets({ metalake, catalog, schema, catalogType, init: true }))

    return filesetData
  }
)

export const updateFileset = createAsyncThunk(
  'appMetalakes/updateFileset',
  async ({ metalake, catalog, catalogType, schema, fileset, data }, { dispatch }) => {
    const [err, res] = await to(updateFilesetApi({ metalake, catalog, schema, fileset, data }))
    if (err || !res) {
      return { err: true }
    }
    await dispatch(fetchFilesets({ metalake, catalog, catalogType, schema, init: true }))

    return res.fileset
  }
)

export const deleteFileset = createAsyncThunk(
  'appMetalakes/deleteFileset',
  async ({ metalake, catalog, catalogType, schema, fileset }, { dispatch }) => {
    await dispatch(setTableLoading(true))
    const [err, res] = await to(deleteFilesetApi({ metalake, catalog, schema, fileset }))
    await dispatch(setTableLoading(false))

    if (err || !res) {
      throw new Error(err)
    }

    await dispatch(fetchFilesets({ metalake, catalog, catalogType, schema, page: 'schemas', init: true }))

    return res
  }
)

export const getFilesetFiles = createAsyncThunk(
  'appMetalakes/getFilesetFiles',
  async ({ metalake, catalog, schema, fileset, subPath = '/', locationName }, { getState, dispatch }) => {
    await dispatch(setTableLoading(true))
    const [err, res] = await to(listFilesetFilesApi({ metalake, catalog, schema, fileset, subPath, locationName }))
    await dispatch(setTableLoading(false))

    if (err || !res) {
      await dispatch(resetTableData())
      throw new Error(err)
    }

    const { files = [] } = res

    return { files, subPath, locationName }
  }
)

export const fetchTopics = createAsyncThunk(
  'appMetalakes/fetchTopics',
  async ({ init, page, metalake, catalog, schema }, { getState, dispatch }) => {
    if (init) {
      await dispatch(resetTableData())
      await dispatch(setTableLoading(true))
    }

    const [err, res] = await to(getTopicsApi({ metalake, catalog, schema }))
    await dispatch(setTableLoading(false))
    if (init && (err || !res)) {
      await dispatch(resetTableData())
      throw new Error(err)
    }

    const { identifiers = [] } = res || {}

    const topics = identifiers.map(topic => {
      return {
        ...topic,
        node: 'topic',
        id: `{{${metalake}}}{{${catalog}}}{{${'messaging'}}}{{${schema}}}{{${topic.name}}}`,
        key: `{{${metalake}}}{{${catalog}}}{{${'messaging'}}}{{${schema}}}{{${topic.name}}}`,
        path: `?${new URLSearchParams({
          metalake,
          catalog,
          catalogType: 'messaging',
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
      await dispatch(
        setIntoTreeNodes({
          key: `{{${metalake}}}{{${catalog}}}{{${'messaging'}}}{{${schema}}}`,
          data: topics
        })
      )
    }

    await dispatch(
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
    if (init) {
      await dispatch(setTableLoading(true))
    }
    const [err, res] = await to(getTopicDetailsApi({ metalake, catalog, schema, topic }))
    await dispatch(setTableLoading(false))

    if (err || !res) {
      throw new Error(err)
    }

    const { topic: resTopic } = res

    await dispatch(
      setExpandedNodes([
        `{{${metalake}}}`,
        `{{${metalake}}}{{${catalog}}}{{${'messaging'}}}`,
        `{{${metalake}}}{{${catalog}}}{{${'messaging'}}}{{${schema}}}`
      ])
    )

    return { topic: resTopic, init }
  }
)

export const createTopic = createAsyncThunk(
  'appMetalakes/createTopic',
  async ({ data, metalake, catalog, catalogType, schema }, { dispatch }) => {
    await dispatch(setTableLoading(true))
    const [err, res] = await to(createTopicApi({ data, metalake, catalog, schema }))
    await dispatch(setTableLoading(false))

    if (err || !res) {
      return { err: true }
    }

    const { topic: topicItem } = res

    const topicData = {
      ...topicItem,
      node: 'topic',
      id: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schema}}}{{${topicItem.name}}}`,
      key: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schema}}}{{${topicItem.name}}}`,
      path: `?${new URLSearchParams({ metalake, catalog, catalogType, schema, topic: topicItem.name }).toString()}`,
      name: topicItem.name,
      title: topicItem.name,
      tables: [],
      children: []
    }

    await dispatch(fetchTopics({ metalake, catalog, schema, catalogType, init: true }))

    return topicData
  }
)

export const updateTopic = createAsyncThunk(
  'appMetalakes/updateTopic',
  async ({ metalake, catalog, catalogType, schema, topic, data }, { dispatch }) => {
    const [err, res] = await to(updateTopicApi({ metalake, catalog, schema, topic, data }))
    if (err || !res) {
      return { err: true }
    }
    await dispatch(fetchTopics({ metalake, catalog, catalogType, schema, init: true }))

    return res.topic
  }
)

export const deleteTopic = createAsyncThunk(
  'appMetalakes/deleteTopic',
  async ({ metalake, catalog, catalogType, schema, topic }, { dispatch }) => {
    const [err, res] = await to(deleteTopicApi({ metalake, catalog, schema, topic }))

    if (err || !res) {
      throw new Error(err)
    }
    await dispatch(setTableLoading(true))
    await dispatch(fetchTopics({ metalake, catalog, catalogType, schema, page: 'topics', init: true }))
    await dispatch(setTableLoading(false))

    return res
  }
)

export const fetchModels = createAsyncThunk(
  'appMetalakes/fetchModels',
  async ({ init, page, metalake, catalog, schema }, { getState, dispatch }) => {
    if (init) {
      await dispatch(resetTableData())
      await dispatch(setTableLoading(true))
    }

    const [err, res] = await to(getModelsApi({ metalake, catalog, schema }))
    await dispatch(setTableLoading(false))

    if (init && (err || !res)) {
      throw new Error(err)
    }

    const { identifiers = [] } = res || {}

    const models = identifiers.map(model => {
      return {
        ...model,
        node: 'model',
        id: `{{${metalake}}}{{${catalog}}}{{${'model'}}}{{${schema}}}{{${model.name}}}`,
        key: `{{${metalake}}}{{${catalog}}}{{${'model'}}}{{${schema}}}{{${model.name}}}`,
        path: `?${new URLSearchParams({
          metalake,
          catalog,
          catalogType: 'model',
          schema,
          model: model.name
        }).toString()}`,
        name: model.name,
        title: model.name,
        isLeaf: true
      }
    })

    if (init && getState().metalakes.loadedNodes.includes(`{{${metalake}}}{{${catalog}}}{{${'model'}}}{{${schema}}}`)) {
      await dispatch(
        setIntoTreeNodes({
          key: `{{${metalake}}}{{${catalog}}}{{${'model'}}}{{${schema}}}`,
          data: models
        })
      )
    }

    await dispatch(
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

    return { model: resModel, init }
  }
)

export const registerModel = createAsyncThunk(
  'appMetalakes/registerModel',
  async ({ data, metalake, catalog, catalogType, schema }, { dispatch }) => {
    await dispatch(setTableLoading(true))
    const [err, res] = await to(registerModelApi({ data, metalake, catalog, schema }))
    await dispatch(setTableLoading(false))

    if (err || !res) {
      return { err: true }
    }

    const { model: modelItem } = res

    const modelData = {
      ...modelItem,
      node: 'model',
      id: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schema}}}{{${modelItem.name}}}`,
      key: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schema}}}{{${modelItem.name}}}`,
      path: `?${new URLSearchParams({ metalake, catalog, catalogType, schema, model: modelItem.name }).toString()}`,
      name: modelItem.name,
      title: modelItem.name,
      tables: [],
      children: []
    }

    await dispatch(fetchModels({ metalake, catalog, schema, catalogType, init: true }))

    return modelData
  }
)

export const updateModel = createAsyncThunk(
  'appMetalakes/updateModel',
  async ({ metalake, catalog, catalogType, schema, model, data }, { dispatch }) => {
    const [err, res] = await to(updateModelApi({ metalake, catalog, schema, model, data }))
    if (err || !res) {
      return { err: true }
    }
    await dispatch(fetchModels({ metalake, catalog, catalogType, schema, init: true }))

    return res.model
  }
)

export const deleteModel = createAsyncThunk(
  'appMetalakes/deleteModel',
  async ({ metalake, catalog, catalogType, schema, model }, { dispatch }) => {
    await dispatch(setTableLoading(true))
    const [err, res] = await to(deleteModelApi({ metalake, catalog, schema, model }))
    await dispatch(setTableLoading(false))

    if (err || !res) {
      throw new Error(err)
    }

    await dispatch(fetchModels({ metalake, catalog, catalogType, schema, page: 'models', init: true }))

    return res
  }
)

export const fetchModelVersions = createAsyncThunk(
  'appMetalakes/fetchModelVersions',
  async ({ init, page, metalake, catalog, schema, model }, { getState, dispatch }) => {
    if (init) {
      await dispatch(resetTableData())
      await dispatch(setTableLoading(true))
    }

    const [err, res] = await to(getModelVersionsApi({ metalake, catalog, schema, model }))
    await dispatch(setTableLoading(false))
    if (err || !res) {
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
          catalogType: 'model',
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
    const [err, res] = await to(getVersionDetailsApi({ metalake, catalog, schema, model, version }))

    if (err || !res) {
      throw new Error(err)
    }

    const { modelVersion } = res

    return modelVersion
  }
)

export const linkVersion = createAsyncThunk(
  'appMetalakes/linkVersion',
  async ({ data, metalake, catalog, catalogType, schema, model }, { dispatch }) => {
    await dispatch(setTableLoading(true))
    const [err, res] = await to(linkVersionApi({ data, metalake, catalog, schema, model }))
    await dispatch(setTableLoading(false))

    if (err || !res) {
      return { err: true }
    }

    const { version: versionItem } = res

    const versionData = {
      node: 'version',
      id: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schema}}}{{${model}}}{{${versionItem}}}`,
      key: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schema}}}{{${model}}}{{${versionItem}}}`,
      path: `?${new URLSearchParams({ metalake, catalog, catalogType, schema, version: versionItem }).toString()}`,
      name: versionItem,
      title: versionItem,
      tables: [],
      children: []
    }

    await dispatch(fetchModelVersions({ metalake, catalog, schema, catalogType, model, init: true }))

    return versionData
  }
)

export const updateVersion = createAsyncThunk(
  'appMetalakes/updateVersion',
  async ({ metalake, catalog, catalogType, schema, model, version, data }, { dispatch }) => {
    const [err, res] = await to(updateVersionApi({ metalake, catalog, schema, model, version, data }))
    if (err || !res) {
      return { err: true }
    }
    await dispatch(fetchModelVersions({ metalake, catalog, catalogType, schema, model, init: true }))

    return res.modelVersion
  }
)

export const deleteVersion = createAsyncThunk(
  'appMetalakes/deleteVersion',
  async ({ metalake, catalog, catalogType, schema, model, version }, { dispatch }) => {
    const [err, res] = await to(deleteVersionApi({ metalake, catalog, schema, model, version: version.toString() }))

    if (err || !res) {
      throw new Error(err)
    }

    await dispatch(fetchModelVersions({ metalake, catalog, catalogType, schema, model, page: 'versions', init: true }))

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
    topics: [],
    models: [],
    versions: [],
    metalakeTree: [],
    loadedNodes: [],
    selectedNodes: [],
    expandedNodes: [],
    activatedDetails: null,
    activatedDetailsLoading: false,
    tableLoading: false,
    treeLoading: false,
    currentMetalakeOwner: null,
    currentEntityTags: [],
    currentEntityPolicies: []
  },
  reducers: {
    setFilteredMetalakes(state, action) {
      state.filteredMetalakes = action.payload
    },
    setCurrentMetalakeOwner(state, action) {
      state.setCurrentMetalakeOwner = action.payload
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
    resetActivatedDetails(state, action) {
      state.activatedDetails = null

      // Also clear table-related data to avoid stale content when switching details
      state.tableData = []
      state.tableProps = []
    },
    setActivatedDetailsLoading(state, action) {
      state.activatedDetailsLoading = action.payload
    },
    setIntoTreeNodes(state, action) {
      const { key, data } = action.payload
      state.metalakeTree = updateTreeData(state.metalakeTree, key, data)
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
    resetMetalakeStore(state, action) {
      state.metalakes = []
      state.filteredMetalakes = []
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
      state.metalakeTree = []
      state.loadedNodes = []
      state.selectedNodes = []
      state.expandedNodes = []
      state.activatedDetails = null
      state.activatedDetailsLoading = false
      state.tableLoading = false
      state.treeLoading = false
      state.currentMetalakeOwner = null
      state.currentEntityTags = []
      state.currentEntityPolicies = []
    }
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
      const { key, data } = action.payload

      state.metalakeTree = updateTreeData(state.metalakeTree, key, data)
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
    builder.addCase(getCurrentEntityOwner.fulfilled, (state, action) => {
      if (action.payload.metadataObjectType === 'metalake') {
        state.currentMetalakeOwner = action.payload.owner
      }
    })
    builder.addCase(getCurrentEntityOwner.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
      state.currentMetalakeOwner = null
    })
    builder.addCase(setEntityOwner.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(getCurrentEntityTags.fulfilled, (state, action) => {
      if (action.payload.init) {
        state.currentEntityTags = action.payload.tags
      }
    })
    builder.addCase(getCurrentEntityTags.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(getCurrentEntityPolicies.fulfilled, (state, action) => {
      if (action.payload.init) {
        state.currentEntityPolicies = action.payload.policies
      }
    })
    builder.addCase(getCurrentEntityPolicies.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(getMetadataObjectsForTag.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(getMetadataObjectsForPolicy.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(createCatalog.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(testCatalogConnection.rejected, (state, action) => {
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
      if (action.payload.init) {
        state.activatedDetails = action.payload.catalog
      }
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
      if (action.payload.init) {
        state.activatedDetails = action.payload.schema
      }
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
      if (action.payload.init) {
        state.activatedDetails = action.payload.table
        state.tableData = action.payload.columns || []
      }
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
      if (action.payload.init) {
        state.activatedDetails = action.payload.fileset
      }

      // state.tableData = []
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
      if (action.payload.init) {
        state.activatedDetails = action.payload.topic
      }
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
      if (action.payload.init) {
        state.activatedDetails = action.payload.model
      }
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
  setActivatedDetailsLoading,
  setIntoTreeNodes,
  setLoadedNodes,
  setExpanded,
  setExpandedNodes,
  addCatalogToTree,
  setCatalogInUse,
  setMetalakeInUse,
  removeCatalogFromTree,
  setTableProps,
  resetActivatedDetails
} = appMetalakesSlice.actions

export default appMetalakesSlice.reducer
