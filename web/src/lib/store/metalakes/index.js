/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
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
import { getSchemasApi, getSchemaDetailsApi } from '@/lib/api/schemas'
import { getTablesApi, getTableDetailsApi } from '@/lib/api/tables'
import { getFilesetsApi, getFilesetDetailsApi } from '@/lib/api/filesets'

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
  async ({ key }, { getState, dispatch }) => {
    let result = {
      key,
      data: [],
      tree: getState().metalakes.metalakeTree
    }

    const pathArr = extractPlaceholder(key)
    const [metalake, catalog, type, schema] = pathArr

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

      result.data = identifiers.map(schemaItem => {
        return {
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
      })
    } else if (pathArr.length === 4 && type !== 'fileset') {
      const [err, res] = await to(getTablesApi({ metalake, catalog, schema }))

      const { identifiers = [] } = res

      if (err || !res) {
        throw new Error(err)
      }

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

      const { identifiers = [] } = res

      if (err || !res) {
        throw new Error(err)
      }

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

    if (getState().metalakes.metalakeTree.length === 0) {
      dispatch(fetchCatalogs({ metalake }))
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

    if (getState().metalakes.metalakeTree.length === 0) {
      dispatch(fetchCatalogs({ metalake }))
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

    if (getState().metalakes.metalakeTree.length === 0) {
      dispatch(fetchCatalogs({ metalake }))
    }

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

    if (getState().metalakes.metalakeTree.length === 0) {
      dispatch(fetchCatalogs({ metalake }))
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

    if (getState().metalakes.metalakeTree.length === 0) {
      dispatch(fetchCatalogs({ metalake }))
    }

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
    filesets: [],
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
      state.filesets = []
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
    resetMetalakeStore(state, action) {}
  },
  extraReducers: builder => {
    builder.addCase(fetchMetalakes.fulfilled, (state, action) => {
      state.metalakes = action.payload.metalakes
    })
    builder.addCase(fetchMetalakes.rejected, (state, action) => {
      toast.error(action.error.message)
    })
    builder.addCase(setIntoTreeNodeWithFetch.fulfilled, (state, action) => {
      const { key, data, tree } = action.payload
      state.metalakeTree = updateTreeData(tree, key, data)
    })
    builder.addCase(setIntoTreeNodeWithFetch.rejected, (state, action) => {
      toast.error(action.error.message)
    })
    builder.addCase(getMetalakeDetails.fulfilled, (state, action) => {
      state.activatedDetails = action.payload
    })
    builder.addCase(getMetalakeDetails.rejected, (state, action) => {
      toast.error(action.error.message)
    })
    builder.addCase(createCatalog.rejected, (state, action) => {
      toast.error(action.error.message)
    })
    builder.addCase(updateCatalog.rejected, (state, action) => {
      toast.error(action.error.message)
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
      toast.error(action.error.message)
    })
    builder.addCase(getCatalogDetails.fulfilled, (state, action) => {
      state.activatedDetails = action.payload
    })
    builder.addCase(getCatalogDetails.rejected, (state, action) => {
      toast.error(action.error.message)
    })
    builder.addCase(deleteCatalog.rejected, (state, action) => {
      toast.error(action.error.message)
    })
    builder.addCase(fetchSchemas.fulfilled, (state, action) => {
      state.schemas = action.payload.schemas
      if (action.payload.init) {
        state.tableData = action.payload.schemas
      }
    })
    builder.addCase(fetchSchemas.rejected, (state, action) => {
      toast.error(action.error.message)
    })
    builder.addCase(getSchemaDetails.fulfilled, (state, action) => {
      state.activatedDetails = action.payload
    })
    builder.addCase(getSchemaDetails.rejected, (state, action) => {
      toast.error(action.error.message)
    })
    builder.addCase(fetchTables.fulfilled, (state, action) => {
      state.tables = action.payload.tables
      if (action.payload.init) {
        state.tableData = action.payload.tables
      }
    })
    builder.addCase(fetchTables.rejected, (state, action) => {
      toast.error(action.error.message)
    })
    builder.addCase(getTableDetails.fulfilled, (state, action) => {
      state.activatedDetails = action.payload
      state.tableData = action.payload.columns || []
    })
    builder.addCase(getTableDetails.rejected, (state, action) => {
      toast.error(action.error.message)
    })
    builder.addCase(fetchFilesets.fulfilled, (state, action) => {
      state.filesets = action.payload.filesets
      if (action.payload.init) {
        state.tableData = action.payload.filesets
      }
    })
    builder.addCase(fetchFilesets.rejected, (state, action) => {
      toast.error(action.error.message)
    })
    builder.addCase(getFilesetDetails.fulfilled, (state, action) => {
      state.activatedDetails = action.payload
      state.tableData = []
    })
    builder.addCase(getFilesetDetails.rejected, (state, action) => {
      toast.error(action.error.message)
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
