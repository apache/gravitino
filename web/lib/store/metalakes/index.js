/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import { createSlice, createAsyncThunk } from '@reduxjs/toolkit'

import {
  createMetalakeApi,
  getMetalakesApi,
  deleteMetalakeApi,
  updateMetalakeApi,
  getMetalakeDetailsApi
} from '@/lib/api/metalakes'

import { getCatalogsApi, getCatalogDetailsApi, createCatalogApi } from '@/lib/api/catalogs'
import { getSchemasApi, getSchemaDetailsApi } from '@/lib/api/schemas'
import { getTablesApi, getTableDetailsApi } from '@/lib/api/tables'

export const fetchMetalakes = createAsyncThunk('appMetalakes/fetchMetalakes', async (params, { getState }) => {
  try {
    const response = await getMetalakesApi()

    const { metalakes } = response.data

    return {
      metalakes
    }
  } catch (error) {
    throw new Error(error)
  }
})

export const createMetalake = createAsyncThunk('appMetalakes/createMetalake', async (data, { getState, dispatch }) => {
  try {
    const response = await createMetalakeApi(data)

    dispatch(fetchMetalakes())

    return response.metalake
  } catch (error) {
    throw new Error(error)
  }
})

export const deleteMetalake = createAsyncThunk('appMetalakes/deleteMetalake', async (name, { dispatch }) => {
  try {
    const response = await deleteMetalakeApi(name)

    dispatch(fetchMetalakes())

    return response.data
  } catch (error) {
    throw new Error(error)
  }
})

export const updateMetalake = createAsyncThunk('appMetalakes/updateMetalake', async ({ name, data }, { dispatch }) => {
  try {
    const response = await updateMetalakeApi({ name, data })

    dispatch(fetchMetalakes())

    return response.data
  } catch (error) {
    throw new Error(error)
  }
})

export const initMetalakeTree = createAsyncThunk(
  'appMetalakes/fetchMetalakeTree',
  async ({ metalake, catalog, schema, table }, { dispatch }) => {
    try {
      const tree = []

      const catalogsData = await getCatalogsApi({ metalake })

      const { identifiers: catalogs = [] } = catalogsData.data

      for (const catalogItem of catalogs) {
        const catalogNode = {
          ...catalogItem,
          node: 'catalog',
          id: `${metalake}____${catalogItem.name}`,
          path: `?${new URLSearchParams({ metalake, catalog: catalogItem.name }).toString()}`,
          name: catalogItem.name,
          schemas: []
        }

        if (catalog) {
          if (catalog === catalogNode.name) {
            dispatch(setExpandedTreeNode([catalogNode.id]))

            const schemasData = await getSchemasApi({ metalake, catalog })
            const { identifiers: schemas = [] } = schemasData.data

            for (const schemaItem of schemas) {
              const schemaNode = {
                ...schemaItem,
                node: 'schema',
                id: `${metalake}____${catalogItem.name}____${schemaItem.name}`,
                path: `?${new URLSearchParams({
                  metalake,
                  catalog: catalogItem.name,
                  schema: schemaItem.name
                }).toString()}`,
                name: schemaItem.name,
                tables: []
              }

              if (schema) {
                if (schema === schemaNode.name) {
                  const tablesData = await getTablesApi({ metalake, catalog, schema })
                  const { identifiers: tables = [] } = tablesData.data

                  for (const tableItem of tables) {
                    const tableNode = {
                      ...tableItem,
                      node: 'table',
                      id: `${metalake}____${catalogItem.name}____${schemaItem.name}____${tableItem.name}`,
                      path: `?${new URLSearchParams({
                        metalake,
                        catalog: catalogItem.name,
                        schema: schemaItem.name,
                        table: tableItem.name
                      }).toString()}`,
                      name: tableItem.name
                    }

                    schemaNode.tables.push(tableNode)
                  }
                }
              }

              catalogNode.schemas.push(schemaNode)
            }
          }
        }

        tree.push(catalogNode)
      }

      dispatch(setIsLoadedTree(true))

      return tree
    } catch (error) {
      throw new Error(error)
    }
  }
)

export const setIntoTreeAction = createAsyncThunk(
  'appMetalakes/setIntoTreeAction',
  async ({ catalogItem, nodeIds }, { getState, dispatch }) => {
    const nodeArr = nodeIds[0].split('____')
    const [metalake, catalog, schema, table] = nodeArr

    const data = {
      updated: '',
      id: nodeIds[0],
      metalake,
      catalogs: [],
      catalogItem,
      catalog,
      schemas: [],
      schema,
      tables: [],
      table
    }

    if (nodeArr.length === 1) {
      const catalogs = await dispatch(fetchCatalogs({ metalake }))

      data.updated = 'metalake'
      data.catalogs = catalogs.payload.catalogs
      data.catalogItem = catalogItem

      return data
    } else if (nodeArr.length === 2) {
      const schemas = await dispatch(fetchSchemas({ metalake, catalog }))

      data.updated = 'catalog'
      data.schemas = schemas.payload.schemas

      return data
    } else if (nodeArr.length === 3) {
      const tables = await dispatch(fetchTables({ metalake, catalog, schema }))

      data.updated = 'schema'
      data.tables = tables.payload.tables

      return data
    } else {
      return null
    }
  }
)

export const getMetalakeDetails = createAsyncThunk('appMetalakes/getMetalakeDetails', async ({ metalake }) => {
  const response = await getMetalakeDetailsApi(metalake)

  const { metalake: resMetalake } = response.data

  return resMetalake
})

export const fetchCatalogs = createAsyncThunk(
  'appMetalakes/fetchCatalogs',
  async ({ init, page, metalake }, { dispatch }) => {
    try {
      const response = await getCatalogsApi({ metalake })

      const { identifiers = [] } = response.data

      const catalogs = identifiers.map(catalog => {
        return {
          ...catalog,
          node: 'catalog',
          id: `${metalake}____${catalog.name}`,
          path: `?${new URLSearchParams({ metalake, catalog: catalog.name }).toString()}`,
          name: catalog.name,
          schemas: []
        }
      })

      return {
        catalogs,
        page,
        init
      }
    } catch (error) {
      throw new Error(error)
    }
  }
)

export const getCatalogDetails = createAsyncThunk('appMetalakes/getCatalogDetails', async ({ metalake, catalog }) => {
  const response = await getCatalogDetailsApi({ metalake, catalog })

  const { catalog: resCatalog } = response.data

  return resCatalog
})

export const createCatalog = createAsyncThunk(
  'appMetalakes/createCatalog',
  async ({ data, metalake }, { dispatch }) => {
    try {
      const response = await createCatalogApi({ data, metalake })

      const { catalog: catalogItem } = response.data

      dispatch(setIntoTreeAction({ catalogItem, nodeIds: [metalake] }))

      return response.catalog
    } catch (error) {
      throw new Error(error)
    }
  }
)

export const fetchSchemas = createAsyncThunk('appMetalakes/fetchSchemas', async ({ init, page, metalake, catalog }) => {
  try {
    const response = await getSchemasApi({ metalake, catalog })

    const { identifiers = [] } = response.data

    const schemas = identifiers.map(schema => {
      return {
        ...schema,
        node: 'schema',
        id: `${metalake}____${catalog}____${schema.name}`,
        path: `?${new URLSearchParams({ metalake, catalog, schema: schema.name }).toString()}`,
        name: schema.name,
        tables: []
      }
    })

    return { schemas, page, init }
  } catch (error) {
    throw new Error(error)
  }
})

export const getSchemaDetails = createAsyncThunk(
  'appMetalakes/getSchemaDetails',
  async ({ metalake, catalog, schema }) => {
    const response = await getSchemaDetailsApi({ metalake, catalog, schema })

    const { schema: resSchema } = response.data

    return resSchema
  }
)

export const fetchTables = createAsyncThunk(
  'appMetalakes/fetchTables',
  async ({ init, page, metalake, catalog, schema }) => {
    console.log(schema)
    try {
      const response = await getTablesApi({ metalake, catalog, schema })

      const { identifiers = [] } = response.data

      const tables = identifiers.map(table => {
        return {
          ...table,
          node: 'table',
          id: `${metalake}____${catalog}____${schema}____${table.name}`,
          path: `?${new URLSearchParams({ metalake, catalog, schema, table: table.name }).toString()}`,
          name: table.name,
          columns: []
        }
      })

      return { tables, page, init }
    } catch (error) {
      throw new Error(error)
    }
  }
)

export const getTableDetails = createAsyncThunk(
  'appMetalakes/getTableDetails',
  async ({ metalake, catalog, schema, table }) => {
    const response = await getTableDetailsApi({ metalake, catalog, schema, table })

    const { table: resTable } = response.data

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
    isLoadedTree: false,
    selectedTreeNode: null,
    expendedTreeNode: [],
    activatedDetails: null,
    clickedExpandNode: null
  },
  reducers: {
    setFilteredMetalakes(state, action) {
      state.filteredMetalakes = action.payload
    },
    setIsLoadedTree(state, action) {
      state.isLoadedTree = action.payload
    },
    setSelectedTreeNode(state, action) {
      state.selectedTreeNode = action.payload
    },
    setExpandedTreeNode(state, action) {
      state.expendedTreeNode = action.payload
    },
    resetMetalakeStore(state, action) {}
  },
  extraReducers: builder => {
    builder.addCase(fetchMetalakes.fulfilled, (state, action) => {
      state.metalakes = action.payload.metalakes
    })
    builder.addCase(initMetalakeTree.fulfilled, (state, action) => {
      state.metalakeTree = action.payload
    })
    builder.addCase(setIntoTreeAction.fulfilled, (state, action) => {
      if (action.payload.updated === 'metalake') {
        const catalogItem = {
          namespace: [action.payload.metalake],
          node: 'catalog',
          id: `${action.payload.metalake}____${action.payload.catalogItem.name}`,
          path: `?${new URLSearchParams({
            metalake: action.payload.metalake,
            catalog: action.payload.catalogItem.name
          }).toString()}`,
          name: action.payload.catalogItem.name,
          schemas: []
        }
        state.metalakeTree.push(catalogItem)
        state.tableData = action.payload.catalogs
      }
      if (action.payload.updated === 'catalog') {
        const catalogIndex = state.metalakeTree.findIndex(i => i.id === action.payload.id)
        state.metalakeTree[catalogIndex].schemas = action.payload.schemas
      }

      if (action.payload.updated === 'schema') {
        const catalogIndex = state.metalakeTree.findIndex(i => i.name === action.payload.catalog)
        const schemaIndex = state.metalakeTree[catalogIndex].schemas.findIndex(i => i.id === action.payload.id)
        state.metalakeTree[catalogIndex].schemas[schemaIndex].tables = action.payload.tables
      }
    })
    builder.addCase(getMetalakeDetails.fulfilled, (state, action) => {
      state.activatedDetails = action.payload
    })
    builder.addCase(fetchCatalogs.fulfilled, (state, action) => {
      state.catalogs = action.payload.catalogs
      if (action.payload.init) {
        state.tableData = action.payload.catalogs
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
    })
  }
})

export const { setFilteredMetalakes, setIsLoadedTree, setSelectedTreeNode, setExpandedTreeNode, resetMetalakeStore } =
  appMetalakesSlice.actions

export default appMetalakesSlice.reducer
