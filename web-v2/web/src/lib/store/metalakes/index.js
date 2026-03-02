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
import { getFunctionsApi, getFunctionDetailsApi } from '@/lib/api/functions'

const remapExpandedAndLoadedNodes = ({ getState, mapNode }) => {
  const expandedNodes = getState().metalakes.expandedNodes.map(mapNode)
  const loadedNodes = getState().metalakes.loadedNodes.map(mapNode)

  return {
    expanded: expandedNodes,
    loaded: loadedNodes
  }
}

const mergeWithFunctionNodes = ({ tree, key, entities }) => {
  const existingNode = findInTree(tree, 'key', key)
  const functions = existingNode?.children?.filter(child => child?.node === 'function') || []

  return _.uniqBy([...entities, ...functions], 'key')
}

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
    } else if (pathArr.length === 4) {
      let entityPromise = Promise.resolve(null)
      switch (type) {
        case 'relational':
          entityPromise = getTablesApi({ metalake, catalog, schema })
          break
        case 'fileset':
          entityPromise = getFilesetsApi({ metalake, catalog, schema })
          break
        case 'messaging':
          entityPromise = getTopicsApi({ metalake, catalog, schema })
          break
        case 'model':
          entityPromise = getModelsApi({ metalake, catalog, schema })
          break
        default:
          break
      }

      const [funcResult, entityResult] = await Promise.allSettled([
        getFunctionsApi({ metalake, catalog, schema, details: false }),
        entityPromise
      ])

      const functions =
        funcResult.status === 'fulfilled'
          ? (funcResult.value?.identifiers || []).map(functionItem => {
              const functionName = functionItem?.name || functionItem?.identifier?.name || functionItem

              return {
                ...functionItem,
                node: 'function',
                id: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema}}}{{${functionName}}}`,
                key: `{{${metalake}}}{{${catalog}}}{{${type}}}{{${schema}}}{{${functionName}}}`,
                path: `?${new URLSearchParams({ metalake, catalog, catalogType: type, schema, function: functionName }).toString()}`,
                name: functionName,
                title: functionName,
                isLeaf: true,
                children: []
              }
            })
          : []

      let entities = []
      if (entityResult.status === 'fulfilled' && entityResult.value) {
        switch (type) {
          case 'relational': {
            const { identifiers: tableIdentifiers = [] } = entityResult.value || {}
            entities = tableIdentifiers.map(tableItem => {
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
            break
          }
          case 'fileset': {
            const { identifiers: filesetIdentifiers = [] } = entityResult.value || {}
            entities = filesetIdentifiers.map(filesetItem => {
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
            break
          }
          case 'messaging': {
            const { identifiers: topicIdentifiers = [] } = entityResult.value || {}
            entities = topicIdentifiers.map(topicItem => {
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
            break
          }
          case 'model': {
            const { identifiers: modelIdentifiers = [] } = entityResult.value || {}
            entities = modelIdentifiers.map(modelItem => {
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
            break
          }
          default:
            break
        }
      }

      result.data = [...entities, ...functions]
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
  async ({ init, metalake, catalog, data }, { getState, dispatch }) => {
    const [err, res] = await to(updateCatalogApi({ metalake, catalog, data }))
    if (err || !res) {
      return { err: true }
    }
    if (init) {
      dispatch(getCatalogDetails({ init, metalake, catalog }))
    } else {
      dispatch(updateCatalogInStore({ metalake, catalog, newCatalog: res.catalog }))

      if (catalog !== res.catalog.name) {
        const tree = getState().metalakes.metalakeTree.map(catalogItem => {
          if (catalogItem.name === catalog) {
            const schemas = catalogItem.children
              ? catalogItem.children.map(schema => {
                  const tables = schema.children
                    ? schema.children.map(table => {
                        return {
                          ...table,
                          id: `{{${metalake}}}{{${res.catalog.name}}}{{${res.catalog.type}}}{{${schema.name}}}{{${table.name}}}`,
                          key: `{{${metalake}}}{{${res.catalog.name}}}{{${res.catalog.type}}}{{${schema.name}}}{{${table.name}}}`,
                          path: `?${new URLSearchParams({
                            metalake,
                            catalog: res.catalog.name,
                            catalogType: res.catalog.type,
                            schema: schema.name,
                            table: table.name
                          }).toString()}`
                        }
                      })
                    : []

                  return {
                    ...schema,
                    id: `{{${metalake}}}{{${res.catalog.name}}}{{${res.catalog.type}}}{{${schema.name}}}`,
                    key: `{{${metalake}}}{{${res.catalog.name}}}{{${res.catalog.type}}}{{${schema.name}}}`,
                    path: `?${new URLSearchParams({
                      metalake,
                      catalog: res.catalog.name,
                      catalogType: res.catalog.type,
                      schema: schema.name
                    }).toString()}`,
                    tables: tables,
                    children: tables
                  }
                })
              : []

            return {
              ...catalogItem,
              id: `{{${metalake}}}{{${res.catalog.name}}}{{${res.catalog.type}}}`,
              key: `{{${metalake}}}{{${res.catalog.name}}}{{${res.catalog.type}}}`,
              path: `?${new URLSearchParams({
                metalake,
                catalog: res.catalog.name,
                catalogType: res.catalog.type
              }).toString()}`,
              catalogType: res.catalog.type,
              provider: res.catalog.provider,
              name: res.catalog.name,
              title: res.catalog.name,
              schemas: schemas,
              children: schemas
            }
          }

          return { ...catalogItem }
        })

        dispatch(setMetalakeTree(tree))

        const { expanded, loaded } = remapExpandedAndLoadedNodes({
          getState,
          mapNode: node => {
            const [currentMetalake, currentCatalog, currentType, currentSchema, entity] = extractPlaceholder(node)
            if (currentCatalog !== catalog) {
              return node
            }

            return `{{${currentMetalake}}}{{${res.catalog.name}}}{{${res.catalog.type}}}${
              currentSchema ? `{{${currentSchema}}}` : ''
            }${entity ? `{{${entity}}}` : ''}`
          }
        })

        dispatch(setExpanded(expanded))
        dispatch(setLoadedNodes(loaded))
      }
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
  async ({ init, metalake, catalog, catalogType, schema, data }, { getState, dispatch }) => {
    const [err, res] = await to(updateSchemaApi({ metalake, catalog, schema, data }))
    if (err || !res) {
      return { err: true }
    }
    if (init) {
      dispatch(getSchemaDetails({ init, metalake, catalog, schema }))
    } else {
      dispatch(updateSchemaInStore({ metalake, catalog, catalogType, schema, newSchema: res.schema }))

      if (schema !== res.schema.name) {
        const tree = getState().metalakes.metalakeTree.map(catalogItem => {
          if (catalogItem.name !== catalog) return { ...catalogItem }

          const schemas = catalogItem.children
            ? catalogItem.children.map(schemaItem => {
                if (schemaItem.name !== schema) return { ...schemaItem }

                const tables = schemaItem.children
                  ? schemaItem.children.map(table => {
                      return {
                        ...table,
                        id: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${res.schema.name}}}{{${table.name}}}`,
                        key: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${res.schema.name}}}{{${table.name}}}`,
                        path: `?${new URLSearchParams({
                          metalake,
                          catalog,
                          catalogType,
                          schema: res.schema.name,
                          table: table.name
                        }).toString()}`
                      }
                    })
                  : []

                return {
                  ...schemaItem,
                  ...res.schema,
                  id: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${res.schema.name}}}`,
                  key: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${res.schema.name}}}`,
                  path: `?${new URLSearchParams({ metalake, catalog, catalogType, schema: res.schema.name }).toString()}`,
                  name: res.schema.name,
                  title: res.schema.name,
                  tables: tables,
                  children: tables
                }
              })
            : []

          return {
            ...catalogItem,
            schemas: schemas,
            children: schemas
          }
        })

        dispatch(setMetalakeTree(tree))

        const { expanded, loaded } = remapExpandedAndLoadedNodes({
          getState,
          mapNode: node => {
            const [currentMetalake, currentCatalog, currentType, currentSchema, table] = extractPlaceholder(node)
            if (currentCatalog === catalog && currentType === catalogType && currentSchema === schema) {
              return `{{${currentMetalake}}}{{${currentCatalog}}}{{${currentType}}}{{${res.schema.name}}}${
                table ? `{{${table}}}` : ''
              }`
            }

            return node
          }
        })

        dispatch(setExpanded(expanded))
        dispatch(setLoadedNodes(loaded))
      }
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
      const tableKey = `{{${metalake}}}{{${catalog}}}{{${'relational'}}}{{${schema}}}`
      dispatch(
        setIntoTreeNodes({
          key: tableKey,
          data: mergeWithFunctionNodes({ tree: getState().metalakes.metalakeTree, key: tableKey, entities: tables })
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
  async ({ init, metalake, catalog, catalogType, schema, table, data }, { getState, dispatch }) => {
    const [err, res] = await to(updateTableApi({ metalake, catalog, schema, table, data }))
    if (err || !res) {
      return { err: true }
    }

    if (init) {
      await dispatch(getTableDetails({ init, metalake, catalog, catalogType, schema, table }))
    } else {
      dispatch(updateTableInStore({ metalake, catalog, catalogType, schema, table, newTable: res.table }))

      if (table !== res.table.name) {
        const tree = getState().metalakes.metalakeTree.map(catalogItem => {
          if (catalogItem.name !== catalog) return { ...catalogItem }

          const schemas = catalogItem.children
            ? catalogItem.children.map(schemaItem => {
                if (schemaItem.name !== schema) return { ...schemaItem }

                const tables = schemaItem.children
                  ? schemaItem.children.map(tableItem => {
                      if (tableItem.name !== table) return { ...tableItem }

                      return {
                        ...tableItem,
                        ...res.table,
                        id: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schema}}}{{${res.table.name}}}`,
                        key: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schema}}}{{${res.table.name}}}`,
                        path: `?${new URLSearchParams({
                          metalake,
                          catalog,
                          catalogType,
                          schema,
                          table: res.table.name
                        }).toString()}`,
                        name: res.table.name,
                        title: res.table.name
                      }
                    })
                  : []

                return {
                  ...schemaItem,
                  tables: tables,
                  children: tables
                }
              })
            : []

          return {
            ...catalogItem,
            schemas: schemas,
            children: schemas
          }
        })

        dispatch(setMetalakeTree(tree))

        const { expanded, loaded } = remapExpandedAndLoadedNodes({
          getState,
          mapNode: node => {
            const [currentMetalake, currentCatalog, currentType, currentSchema, currentTable] = extractPlaceholder(node)
            if (
              currentCatalog === catalog &&
              currentType === catalogType &&
              currentSchema === schema &&
              currentTable === table
            ) {
              return `{{${currentMetalake}}}{{${currentCatalog}}}{{${currentType}}}{{${currentSchema}}}{{${res.table.name}}}`
            }

            return node
          }
        })

        dispatch(setExpanded(expanded))
        dispatch(setLoadedNodes(loaded))
      }
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
      const filesetKey = `{{${metalake}}}{{${catalog}}}{{${'fileset'}}}{{${schema}}}`
      dispatch(
        setIntoTreeNodes({
          key: filesetKey,
          data: mergeWithFunctionNodes({ tree: getState().metalakes.metalakeTree, key: filesetKey, entities: filesets })
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
  async ({ init, metalake, catalog, catalogType, schema, fileset, data }, { getState, dispatch }) => {
    const [err, res] = await to(updateFilesetApi({ metalake, catalog, schema, fileset, data }))
    if (err || !res) {
      return { err: true }
    }

    if (init) {
      await dispatch(getFilesetDetails({ init, metalake, catalog, schema, fileset }))
    } else {
      dispatch(updateFilesetInStore({ metalake, catalog, catalogType, schema, fileset, newFileset: res.fileset }))

      if (fileset !== res.fileset.name) {
        const tree = getState().metalakes.metalakeTree.map(catalogItem => {
          if (catalogItem.name !== catalog) return { ...catalogItem }

          const schemas = catalogItem.children
            ? catalogItem.children.map(schemaItem => {
                if (schemaItem.name !== schema) return { ...schemaItem }

                const filesets = schemaItem.children
                  ? schemaItem.children.map(filesetItem => {
                      if (filesetItem.name !== fileset) return { ...filesetItem }

                      return {
                        ...filesetItem,
                        ...res.fileset,
                        id: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schema}}}{{${res.fileset.name}}}`,
                        key: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schema}}}{{${res.fileset.name}}}`,
                        path: `?${new URLSearchParams({
                          metalake,
                          catalog,
                          catalogType,
                          schema,
                          fileset: res.fileset.name
                        }).toString()}`,
                        name: res.fileset.name,
                        title: res.fileset.name,
                        isLeaf: true
                      }
                    })
                  : []

                return {
                  ...schemaItem,
                  children: filesets
                }
              })
            : []

          return {
            ...catalogItem,
            children: schemas
          }
        })

        dispatch(setMetalakeTree(tree))

        const { expanded, loaded } = remapExpandedAndLoadedNodes({
          getState,
          mapNode: node => {
            const [currentMetalake, currentCatalog, currentType, currentSchema, currentFileset] =
              extractPlaceholder(node)
            if (
              currentCatalog === catalog &&
              currentType === catalogType &&
              currentSchema === schema &&
              currentFileset === fileset
            ) {
              return `{{${currentMetalake}}}{{${currentCatalog}}}{{${currentType}}}{{${currentSchema}}}{{${res.fileset.name}}}`
            }

            return node
          }
        })

        dispatch(setExpanded(expanded))
        dispatch(setLoadedNodes(loaded))
      }
    }

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
      const topicKey = `{{${metalake}}}{{${catalog}}}{{${'messaging'}}}{{${schema}}}`
      await dispatch(
        setIntoTreeNodes({
          key: topicKey,
          data: mergeWithFunctionNodes({ tree: getState().metalakes.metalakeTree, key: topicKey, entities: topics })
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
  async ({ init, metalake, catalog, catalogType, schema, topic, data }, { getState, dispatch }) => {
    const [err, res] = await to(updateTopicApi({ metalake, catalog, schema, topic, data }))
    if (err || !res) {
      return { err: true }
    }

    if (init) {
      await dispatch(getTopicDetails({ init, metalake, catalog, schema, topic }))
    } else {
      dispatch(updateTopicInStore({ metalake, catalog, catalogType, schema, topic, newTopic: res.topic }))

      if (topic !== res.topic.name) {
        const tree = getState().metalakes.metalakeTree.map(catalogItem => {
          if (catalogItem.name !== catalog) return { ...catalogItem }

          const schemas = catalogItem.children
            ? catalogItem.children.map(schemaItem => {
                if (schemaItem.name !== schema) return { ...schemaItem }

                const topics = schemaItem.children
                  ? schemaItem.children.map(topicItem => {
                      if (topicItem.name !== topic) return { ...topicItem }

                      return {
                        ...topicItem,
                        ...res.topic,
                        id: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schema}}}{{${res.topic.name}}}`,
                        key: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schema}}}{{${res.topic.name}}}`,
                        path: `?${new URLSearchParams({
                          metalake,
                          catalog,
                          catalogType,
                          schema,
                          topic: res.topic.name
                        }).toString()}`,
                        name: res.topic.name,
                        title: res.topic.name,
                        isLeaf: true
                      }
                    })
                  : []

                return {
                  ...schemaItem,
                  children: topics
                }
              })
            : []

          return {
            ...catalogItem,
            children: schemas
          }
        })

        dispatch(setMetalakeTree(tree))

        const { expanded, loaded } = remapExpandedAndLoadedNodes({
          getState,
          mapNode: node => {
            const [currentMetalake, currentCatalog, currentType, currentSchema, currentTopic] = extractPlaceholder(node)
            if (
              currentCatalog === catalog &&
              currentType === catalogType &&
              currentSchema === schema &&
              currentTopic === topic
            ) {
              return `{{${currentMetalake}}}{{${currentCatalog}}}{{${currentType}}}{{${currentSchema}}}{{${res.topic.name}}}`
            }

            return node
          }
        })

        dispatch(setExpanded(expanded))
        dispatch(setLoadedNodes(loaded))
      }
    }

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

    dispatch(removeTopicFromStore({ metalake, catalog, catalogType, schema, topic }))

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
      const modelKey = `{{${metalake}}}{{${catalog}}}{{${'model'}}}{{${schema}}}`
      await dispatch(
        setIntoTreeNodes({
          key: modelKey,
          data: mergeWithFunctionNodes({ tree: getState().metalakes.metalakeTree, key: modelKey, entities: models })
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
  async ({ init, metalake, catalog, catalogType, schema, model, data }, { getState, dispatch }) => {
    const [err, res] = await to(updateModelApi({ metalake, catalog, schema, model, data }))
    if (err || !res) {
      return { err: true }
    }

    if (init) {
      await dispatch(getModelDetails({ init, metalake, catalog, schema, model }))
    } else {
      dispatch(updateModelInStore({ metalake, catalog, catalogType, schema, model, newModel: res.model }))

      if (model !== res.model.name) {
        const tree = getState().metalakes.metalakeTree.map(catalogItem => {
          if (catalogItem.name !== catalog) return { ...catalogItem }

          const schemas = catalogItem.children
            ? catalogItem.children.map(schemaItem => {
                if (schemaItem.name !== schema) return { ...schemaItem }

                const models = schemaItem.children
                  ? schemaItem.children.map(modelItem => {
                      if (modelItem.name !== model) return { ...modelItem }

                      return {
                        ...modelItem,
                        ...res.model,
                        id: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schema}}}{{${res.model.name}}}`,
                        key: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schema}}}{{${res.model.name}}}`,
                        path: `?${new URLSearchParams({
                          metalake,
                          catalog,
                          catalogType,
                          schema,
                          model: res.model.name
                        }).toString()}`,
                        name: res.model.name,
                        title: res.model.name,
                        isLeaf: true
                      }
                    })
                  : []

                return {
                  ...schemaItem,
                  children: models
                }
              })
            : []

          return {
            ...catalogItem,
            children: schemas
          }
        })

        dispatch(setMetalakeTree(tree))

        const { expanded, loaded } = remapExpandedAndLoadedNodes({
          getState,
          mapNode: node => {
            const [currentMetalake, currentCatalog, currentType, currentSchema, currentModel] = extractPlaceholder(node)
            if (
              currentCatalog === catalog &&
              currentType === catalogType &&
              currentSchema === schema &&
              currentModel === model
            ) {
              return `{{${currentMetalake}}}{{${currentCatalog}}}{{${currentType}}}{{${currentSchema}}}{{${res.model.name}}}`
            }

            return node
          }
        })

        dispatch(setExpanded(expanded))
        dispatch(setLoadedNodes(loaded))
      }
    }

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

export const fetchFunctions = createAsyncThunk(
  'appMetalakes/fetchFunctions',
  async ({ init, metalake, catalog, schema }, { dispatch }) => {
    const [err, res] = await to(getFunctionsApi({ metalake, catalog, schema, details: true }))

    if (init && (err || !res)) {
      throw new Error(err)
    }

    const { functions = [], identifiers = [] } = res || {}

    const normalized = (functions.length ? functions : identifiers).map(item => {
      return {
        ...item,
        name: item?.name || item?.identifier?.name || item?.identifier || ''
      }
    })

    return { functions: normalized, init }
  }
)

export const getFunctionDetails = createAsyncThunk(
  'appMetalakes/getFunctionDetails',
  async ({ init, metalake, catalog, schema, functionName }) => {
    const [err, res] = await to(getFunctionDetailsApi({ metalake, catalog, schema, functionName }))

    if (err || !res) {
      throw new Error(err)
    }

    return { function: res.function || res, init }
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
    functions: [],
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
      state.functions = []
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
    updateCatalogInStore(state, action) {
      const { metalake, catalog, newCatalog } = action.payload

      const updateCatalogItem = item => {
        if (!item || item.name !== catalog) return item
        const schemas = item.children || item.schemas || []

        return {
          ...item,
          ...newCatalog,
          node: 'catalog',
          id: `{{${metalake}}}{{${newCatalog.name}}}{{${newCatalog.type}}}`,
          key: `{{${metalake}}}{{${newCatalog.name}}}{{${newCatalog.type}}}`,
          path: `?${new URLSearchParams({
            metalake,
            catalog: newCatalog.name,
            catalogType: newCatalog.type
          }).toString()}`,
          catalogType: newCatalog.type,
          provider: newCatalog.provider,
          name: newCatalog.name,
          title: newCatalog.name,
          namespace: [metalake],
          schemas: schemas,
          children: schemas
        }
      }

      state.catalogs = state.catalogs.map(updateCatalogItem)
      state.tableData = state.tableData.map(updateCatalogItem)
    },
    updateSchemaInStore(state, action) {
      const { metalake, catalog, catalogType, schema, newSchema } = action.payload

      const updateSchemaItem = item => {
        if (!item || item.name !== schema) return item

        return {
          ...item,
          ...newSchema,
          node: 'schema',
          id: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${newSchema.name}}}`,
          key: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${newSchema.name}}}`,
          path: `?${new URLSearchParams({
            metalake,
            catalog,
            catalogType,
            schema: newSchema.name
          }).toString()}`,
          name: newSchema.name,
          title: newSchema.name
        }
      }

      state.schemas = state.schemas.map(updateSchemaItem)
      state.tableData = state.tableData.map(updateSchemaItem)
    },
    updateTableInStore(state, action) {
      const { metalake, catalog, catalogType, schema, table, newTable } = action.payload

      const updateTableItem = item => {
        if (!item || item.name !== table) return item

        return {
          ...item,
          ...newTable,
          node: 'table',
          id: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schema}}}{{${newTable.name}}}`,
          key: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schema}}}{{${newTable.name}}}`,
          path: `?${new URLSearchParams({
            metalake,
            catalog,
            catalogType,
            schema,
            table: newTable.name
          }).toString()}`,
          name: newTable.name,
          title: newTable.name,
          isLeaf: true
        }
      }

      state.tables = state.tables.map(updateTableItem)
      state.tableData = state.tableData.map(updateTableItem)
    },
    updateTopicInStore(state, action) {
      const { metalake, catalog, catalogType, schema, topic, newTopic } = action.payload

      const updateTopicItem = item => {
        if (!item || item.name !== topic) return item

        return {
          ...item,
          ...newTopic,
          node: 'topic',
          id: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schema}}}{{${newTopic.name}}}`,
          key: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schema}}}{{${newTopic.name}}}`,
          path: `?${new URLSearchParams({
            metalake,
            catalog,
            catalogType,
            schema,
            topic: newTopic.name
          }).toString()}`,
          name: newTopic.name,
          title: newTopic.name,
          isLeaf: true
        }
      }

      state.topics = state.topics.map(updateTopicItem)
      state.tableData = state.tableData.map(updateTopicItem)
    },
    updateFilesetInStore(state, action) {
      const { metalake, catalog, catalogType, schema, fileset, newFileset } = action.payload

      const updateFilesetItem = item => {
        if (!item || item.name !== fileset) return item

        return {
          ...item,
          ...newFileset,
          node: 'fileset',
          id: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schema}}}{{${newFileset.name}}}`,
          key: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schema}}}{{${newFileset.name}}}`,
          path: `?${new URLSearchParams({
            metalake,
            catalog,
            catalogType,
            schema,
            fileset: newFileset.name
          }).toString()}`,
          name: newFileset.name,
          title: newFileset.name,
          isLeaf: true
        }
      }

      state.filesets = state.filesets.map(updateFilesetItem)
      state.tableData = state.tableData.map(updateFilesetItem)
    },
    updateModelInStore(state, action) {
      const { metalake, catalog, catalogType, schema, model, newModel } = action.payload

      const updateModelItem = item => {
        if (!item || item.name !== model) return item

        return {
          ...item,
          ...newModel,
          node: 'model',
          id: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schema}}}{{${newModel.name}}}`,
          key: `{{${metalake}}}{{${catalog}}}{{${catalogType}}}{{${schema}}}{{${newModel.name}}}`,
          path: `?${new URLSearchParams({
            metalake,
            catalog,
            catalogType,
            schema,
            model: newModel.name
          }).toString()}`,
          name: newModel.name,
          title: newModel.name,
          isLeaf: true
        }
      }

      state.models = state.models.map(updateModelItem)
      state.tableData = state.tableData.map(updateModelItem)
    },
    updateCatalogInUse(state, action) {
      const { catalog, isInUse } = action.payload
      const inUseValue = isInUse ? 'true' : 'false'
      state.tableData = state.tableData.map(item => {
        if (item.name !== catalog) return item

        return {
          ...item,
          properties: { ...(item.properties || {}), 'in-use': inUseValue }
        }
      })
      state.catalogs = state.catalogs.map(item => {
        if (item.name !== catalog) return item

        return {
          ...item,
          properties: { ...(item.properties || {}), 'in-use': inUseValue }
        }
      })
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
    removeTopicFromStore(state, action) {
      const { metalake, catalog, catalogType, schema, topic } = action.payload
      const effectiveCatalogType = catalogType || 'messaging'
      const schemaKey = `{{${metalake}}}{{${catalog}}}{{${effectiveCatalogType}}}{{${schema}}}`
      const topicKey = `{{${metalake}}}{{${catalog}}}{{${effectiveCatalogType}}}{{${schema}}}{{${topic}}}`

      state.topics = state.topics.filter(item => item.name !== topic)
      state.tableData = state.tableData.filter(item => !(item?.node === 'topic' && item?.name === topic))
      state.selectedNodes = state.selectedNodes.filter(key => key !== topicKey)

      const schemaNode = findInTree(state.metalakeTree, 'key', schemaKey)
      if (schemaNode?.children?.length) {
        schemaNode.children = schemaNode.children.filter(item => item.key !== topicKey)
      }
    },
    resetMetalakeStore(state, action) {
      if (!action.payload?.isCacheMetalakes) {
        state.metalakes = []
      }
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
    builder.addCase(fetchFunctions.fulfilled, (state, action) => {
      state.functions = action.payload.functions
    })
    builder.addCase(getFunctionDetails.fulfilled, (state, action) => {
      if (action.payload.init) {
        state.activatedDetails = action.payload.function
      }
    })
    builder.addCase(fetchFunctions.rejected, (state, action) => {
      if (!action.error.message.includes('CanceledError')) {
        toast.error(action.error.message)
      }
    })
    builder.addCase(getFunctionDetails.rejected, (state, action) => {
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
  updateCatalogInStore,
  updateSchemaInStore,
  updateTableInStore,
  updateTopicInStore,
  updateFilesetInStore,
  updateModelInStore,
  setCatalogInUse,
  updateCatalogInUse,
  setMetalakeInUse,
  removeTopicFromStore,
  removeCatalogFromTree,
  setTableProps,
  resetActivatedDetails
} = appMetalakesSlice.actions

export default appMetalakesSlice.reducer
