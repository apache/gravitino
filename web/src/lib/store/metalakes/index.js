/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import { createSlice, createAsyncThunk } from '@reduxjs/toolkit'

import { to, extractPlaceholder, updateTreeData, findInTree } from '@/lib/utils'

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
import { getSchemasApi, getSchemaDetailsApi } from '@/lib/api/schemas'
import { getTablesApi, getTableDetailsApi } from '@/lib/api/tables'

export const fetchMetalakes = createAsyncThunk('appMetalakes/fetchMetalakes', async (params, { getState }) => {
  const [err, res] = await to(getMetalakesApi())

  if (err || !res) {
    throw new Error(err)
  }

  const { metalakes } = res

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
  async ({ key }, { getState, dispatch }) => {
    let result = {
      key,
      data: [],
      tree: getState().metalakes.metalakeTree
    }

    const pathArr = extractPlaceholder(key)
    const [metalake, catalog, schema, table] = pathArr

    if (pathArr.length === 1) {
      const [err, res] = await to(getCatalogsApi({ metalake }))

      if (err || !res) {
        throw new Error(err)
      }

      const { identifiers = [] } = res

      result.data = identifiers.map(catalogItem => {
        return {
          ...catalogItem,
          node: 'catalog',
          id: `{{${metalake}}}{{${catalogItem.name}}}`,
          key: `{{${metalake}}}{{${catalogItem.name}}}`,
          path: `?${new URLSearchParams({ metalake, catalog: catalogItem.name }).toString()}`,
          name: catalogItem.name,
          title: catalogItem.name,
          schemas: [],
          children: []
        }
      })
    } else if (pathArr.length === 2) {
      const [err, res] = await to(getSchemasApi({ metalake, catalog }))

      if (err || !res) {
        throw new Error(err)
      }

      const { identifiers = [] } = res

      result.data = identifiers.map(schemaItem => {
        return {
          ...schemaItem,
          node: 'schema',
          id: `{{${metalake}}}{{${catalog}}}{{${schemaItem.name}}}`,
          key: `{{${metalake}}}{{${catalog}}}{{${schemaItem.name}}}`,
          path: `?${new URLSearchParams({ metalake, catalog, schema: schemaItem.name }).toString()}`,
          name: schemaItem.name,
          title: schemaItem.name,
          tables: [],
          children: []
        }
      })
    } else if (pathArr.length === 3) {
      const [err, res] = await to(getTablesApi({ metalake, catalog, schema }))

      const { identifiers = [] } = res

      if (err || !res) {
        throw new Error(err)
      }

      result.data = identifiers.map(tableItem => {
        return {
          ...tableItem,
          node: 'table',
          id: `{{${metalake}}}{{${catalog}}}{{${schema}}}{{${tableItem.name}}}`,
          key: `{{${metalake}}}{{${catalog}}}{{${schema}}}{{${tableItem.name}}}`,
          path: `?${new URLSearchParams({ metalake, catalog, schema, table: tableItem.name }).toString()}`,
          name: tableItem.name,
          title: tableItem.name,
          isLeaf: true,
          columns: [],
          children: []
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
      dispatch(resetTableData())
      dispatch(setTableLoading(true))
    }
    const [err, res] = await to(getCatalogsApi({ metalake }))
    dispatch(setTableLoading(false))

    if (err || !res) {
      dispatch(resetTableData())
      throw new Error(err)
    }

    const { identifiers = [] } = res

    const catalogs = identifiers.map(catalog => {
      return {
        ...catalog,
        node: 'catalog',
        id: `{{${metalake}}}{{${catalog.name}}}`,
        key: `{{${metalake}}}{{${catalog.name}}}`,
        path: `?${new URLSearchParams({ metalake, catalog: catalog.name }).toString()}`,
        name: catalog.name,
        title: catalog.name,
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
                            id: `{{${metalake}}}{{${update.newCatalog.name}}}{{${schema.name}}}{{${table.name}}}`,
                            key: `{{${metalake}}}{{${update.newCatalog.name}}}{{${schema.name}}}{{${table.name}}}`,
                            path: `?${new URLSearchParams({
                              metalake,
                              catalog: update.newCatalog.name,
                              schema: schema.name,
                              table: table.name
                            }).toString()}`
                          }
                        })
                      : []

                    return {
                      ...schema,
                      id: `{{${metalake}}}{{${update.newCatalog.name}}}{{${schema.name}}}`,
                      key: `{{${metalake}}}{{${update.newCatalog.name}}}{{${schema.name}}}`,
                      path: `?${new URLSearchParams({
                        metalake,
                        catalog: update.newCatalog.name,
                        schema: schema.name
                      }).toString()}`,
                      tables: tables,
                      children: tables
                    }
                  })
                : []

              return {
                ...catalog,
                id: `{{${metalake}}}{{${update.newCatalog.name}}}`,
                key: `{{${metalake}}}{{${update.newCatalog.name}}}`,
                path: `?${new URLSearchParams({ metalake, catalog: update.newCatalog.name }).toString()}`,
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
              const updatedNode = `{{${metalake}}}{{${update.newCatalog.name}}}${schema ? `{{${schema}}}` : ''}${
                table ? `{{${table}}}` : ''
              }`

              return updatedNode
            }

            return node
          })

          const expanded = expandedNodes.filter(i => !i.startsWith(`{{${metalake}}}{{${update.catalog}}}`))
          dispatch(setExpanded(expanded))

          const loadedNodes = getState().metalakes.loadedNodes.map(node => {
            const [metalake, catalog, schema, table] = extractPlaceholder(node)
            if (catalog === update.catalog) {
              const updatedNode = `{{${metalake}}}{{${update.newCatalog.name}}}${schema ? `{{${schema}}}` : ''}${
                table ? `{{${table}}}` : ''
              }`

              return updatedNode
            }

            return node
          })

          const loaded = loadedNodes.filter(i => !i.startsWith(`{{${metalake}}}{{${update.catalog}}}`))
          dispatch(setLoadedNodes(loaded))
        }
      } else {
        const mergedTree = _.values(
          _.merge(_.keyBy(getState().metalakes.metalakeTree, 'key'), _.keyBy(catalogs, 'key'))
        )
        dispatch(setMetalakeTree(mergedTree))
      }
    }

    return {
      catalogs,
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
      id: `{{${metalake}}}{{${catalogItem.name}}}`,
      key: `{{${metalake}}}{{${catalogItem.name}}}`,
      path: `?${new URLSearchParams({ metalake, catalog: catalogItem.name }).toString()}`,
      name: catalogItem.name,
      title: catalogItem.name,
      schemas: [],
      children: []
    }

    dispatch(dispatch(fetchCatalogs({ metalake, init: true })))

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
  async ({ metalake, catalog }, { dispatch }) => {
    dispatch(setTableLoading(true))
    const [err, res] = await to(deleteCatalogApi({ metalake, catalog }))
    dispatch(setTableLoading(false))

    if (err || !res) {
      throw new Error(err)
    }

    dispatch(fetchCatalogs({ metalake, catalog, page: 'metalakes', init: true }))

    dispatch(removeCatalogFromTree(`{{${metalake}}}{{${catalog}}}`))

    return res
  }
)

export const fetchSchemas = createAsyncThunk(
  'appMetalakes/fetchSchemas',
  async ({ init, page, metalake, catalog }, { getState, dispatch }) => {
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
        `{{${metalake}}}{{${catalog}}}{{${schema.name}}}`
      )

      return {
        ...schema,
        node: 'schema',
        id: `{{${metalake}}}{{${catalog}}}{{${schema.name}}}`,
        key: `{{${metalake}}}{{${catalog}}}{{${schema.name}}}`,
        path: `?${new URLSearchParams({ metalake, catalog, schema: schema.name }).toString()}`,
        name: schema.name,
        title: schema.name,
        tables: schemaItem ? schemaItem.children : [],
        children: schemaItem ? schemaItem.children : []
      }
    })

    if (init && getState().metalakes.loadedNodes.includes(`{{${metalake}}}{{${catalog}}}`)) {
      dispatch(
        setIntoTreeNodes({
          key: `{{${metalake}}}{{${catalog}}}`,
          data: schemas,
          tree: getState().metalakes.metalakeTree
        })
      )
    }

    if (getState().metalakes.metalakeTree.length === 0) {
      dispatch(fetchCatalogs({ metalake }))
    }

    dispatch(setExpandedNodes([`{{${metalake}}}`, `{{${metalake}}}{{${catalog}}}`]))

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
        id: `{{${metalake}}}{{${catalog}}}{{${schema}}}{{${table.name}}}`,
        key: `{{${metalake}}}{{${catalog}}}{{${schema}}}{{${table.name}}}`,
        path: `?${new URLSearchParams({ metalake, catalog, schema, table: table.name }).toString()}`,
        name: table.name,
        title: table.name,
        isLeaf: true,
        columns: [],
        children: []
      }
    })

    if (init && getState().metalakes.loadedNodes.includes(`{{${metalake}}}{{${catalog}}}{{${schema}}}`)) {
      dispatch(
        setIntoTreeNodes({
          key: `{{${metalake}}}{{${catalog}}}{{${schema}}}`,
          data: tables,
          tree: getState().metalakes.metalakeTree
        })
      )
    }

    if (getState().metalakes.metalakeTree.length === 0) {
      dispatch(fetchCatalogs({ metalake }))
    }

    dispatch(
      setExpandedNodes([
        `{{${metalake}}}`,
        `{{${metalake}}}{{${catalog}}}`,
        `{{${metalake}}}{{${catalog}}}{{${schema}}}`
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

    if (getState().metalakes.metalakeTree.length === 0) {
      dispatch(fetchCatalogs({ metalake }))
    }

    dispatch(
      setExpandedNodes([
        `{{${metalake}}}`,
        `{{${metalake}}}{{${catalog}}}`,
        `{{${metalake}}}{{${catalog}}}{{${schema}}}`
      ])
    )

    return resTable
  }
)

export const appMetalakesSlice = createSlice({
  name: 'appMetalakes',
  initialState: {
    metalakes: [],
    filteredMetalakes: [],
    tableData: [],
    catalogs: [],
    schemas: [],
    tables: [],
    columns: [],
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
      state.expandedNodes = []
    },
    resetTableData(state, action) {
      state.tableData = []
    },
    resetTree(state, action) {
      state.metalakeTree = []
      state.expandedNodes = []
      state.selectedNodes = []
      state.loadedNodes = []

      state.tableData = []
      state.catalogs = []
      state.schemas = []
      state.tables = []
      state.columns = []
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
      state.metalakeTree.push(action.payload)
    },
    removeCatalogFromTree(state, action) {
      state.metalakeTree = state.metalakeTree.filter(i => i.key !== action.payload)
    },
    resetMetalakeStore(state, action) {}
  },
  extraReducers: builder => {
    builder.addCase(fetchMetalakes.fulfilled, (state, action) => {
      state.metalakes = action.payload.metalakes
    })
    builder.addCase(setIntoTreeNodeWithFetch.fulfilled, (state, action) => {
      const { key, data, tree } = action.payload
      state.metalakeTree = updateTreeData(tree, key, data)
    })
    builder.addCase(getMetalakeDetails.fulfilled, (state, action) => {
      state.activatedDetails = action.payload
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
    builder.addCase(getCatalogDetails.fulfilled, (state, action) => {
      state.activatedDetails = action.payload
    })
    builder.addCase(fetchSchemas.fulfilled, (state, action) => {
      state.schemas = action.payload.schemas
      if (action.payload.init) {
        state.tableData = action.payload.schemas
      }
    })
    builder.addCase(getSchemaDetails.fulfilled, (state, action) => {
      state.activatedDetails = action.payload
    })
    builder.addCase(fetchTables.fulfilled, (state, action) => {
      state.tables = action.payload.tables
      if (action.payload.init) {
        state.tableData = action.payload.tables
      }
    })
    builder.addCase(getTableDetails.fulfilled, (state, action) => {
      state.activatedDetails = action.payload
      state.tableData = action.payload.columns || []
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
  removeCatalogFromTree
} = appMetalakesSlice.actions

export default appMetalakesSlice.reducer
