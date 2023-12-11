/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { useEffect } from 'react'

import { Box } from '@mui/material'

import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'

import LeftContent from './LeftContent'
import RightContent from './RightContent'

import {
  initMetalakeTree,
  fetchCatalogs,
  fetchSchemas,
  fetchTables,
  getMetalakeDetails,
  getCatalogDetails,
  getSchemaDetails,
  getTableDetails,
  setSelectedTreeNode
} from '@/lib/store/metalakes'

const MetalakeView = props => {
  const { page, tableTitle, routeParams } = props

  const dispatch = useAppDispatch()
  const store = useAppSelector(state => state.metalakes)

  useEffect(() => {
    const { metalake, catalog, schema, table } = routeParams

    if (store.metalakeTree.length === 0) {
      dispatch(initMetalakeTree({ metalake, catalog, schema, table }))
    }

    switch (page) {
      case 'metalakes':
        dispatch(fetchCatalogs({ init: true, page, metalake }))
        dispatch(getMetalakeDetails({ metalake }))

        break
      case 'catalogs':
        dispatch(fetchSchemas({ init: true, page, metalake, catalog }))
        dispatch(getCatalogDetails({ metalake, catalog }))

        break
      case 'schemas':
        dispatch(fetchTables({ init: true, page, metalake, catalog, schema }))
        dispatch(getSchemaDetails({ metalake, catalog, schema }))

        break
      case 'tables':
        dispatch(fetchColumns({ init: true, page, metalake, catalog, schema, table }))
        dispatch(getTableDetails({ metalake, catalog, schema, table }))
        break
      default:
        break
    }

    dispatch(
      setSelectedTreeNode(
        routeParams.catalog
          ? `${routeParams.metalake}____${routeParams.catalog}${routeParams.schema ? `____${routeParams.schema}` : ''}${
              routeParams.table ? `____${routeParams.table}` : ''
            }`
          : null
      )
    )

    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [page, dispatch, routeParams])

  return (
    <Box className='app-metalake twc-w-full twc-h-full twc-flex twc-rounded-lg twc-overflow-auto twc-relative shadow-md'>
      <LeftContent routeParams={routeParams} />
      <RightContent tableTitle={tableTitle} routeParams={routeParams} page={page} />
    </Box>
  )
}

export default MetalakeView
