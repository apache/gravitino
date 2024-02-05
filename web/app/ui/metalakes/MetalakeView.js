/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { useEffect } from 'react'

import { Box } from '@mui/material'

import { useAppDispatch } from '@/lib/hooks/useStore'

import LeftContent from './LeftContent'
import RightContent from './RightContent'

import {
  fetchCatalogs,
  fetchSchemas,
  fetchTables,
  getMetalakeDetails,
  getCatalogDetails,
  getSchemaDetails,
  getTableDetails,
  setSelectedNodes
} from '@/lib/store/metalakes'

const MetalakeView = props => {
  const { page, tableTitle, routeParams } = props

  const dispatch = useAppDispatch()

  useEffect(() => {
    if (JSON.stringify(routeParams) !== '{}') {
      const { metalake, catalog, schema, table } = routeParams

      switch (page) {
        case 'metalakes':
          dispatch(fetchCatalogs({ init: true, page, metalake }))
          dispatch(getMetalakeDetails({ metalake }))
          break

        case 'catalogs':
          if (catalog !== null) {
            dispatch(fetchSchemas({ init: true, page, metalake, catalog }))
            dispatch(getCatalogDetails({ metalake, catalog }))
          }
          break

        case 'schemas':
          if (catalog !== null && schema !== null) {
            dispatch(fetchTables({ init: true, page, metalake, catalog, schema }))
            dispatch(getSchemaDetails({ metalake, catalog, schema }))
          }
          break

        case 'tables':
          if (catalog !== null && schema !== null && table !== null) {
            dispatch(getTableDetails({ init: true, metalake, catalog, schema, table }))
          }
          break

        default:
          break
      }
    }

    dispatch(
      setSelectedNodes(
        routeParams.catalog
          ? [
              `{{${routeParams.metalake}}}{{${routeParams.catalog}}}${
                routeParams.schema ? `{{${routeParams.schema}}}` : ''
              }${routeParams.table ? `{{${routeParams.table}}}` : ''}`
            ]
          : []
      )
    )

    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [routeParams])

  return (
    <Box className='app-metalake twc-w-full twc-h-full twc-flex twc-rounded-lg twc-overflow-auto twc-relative shadow-md'>
      <LeftContent routeParams={routeParams} />
      <RightContent tableTitle={tableTitle} routeParams={routeParams} page={page} />
    </Box>
  )
}

export default MetalakeView
