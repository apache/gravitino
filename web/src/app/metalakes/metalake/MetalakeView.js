/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { useEffect } from 'react'

import { Box } from '@mui/material'

import { useAppDispatch } from '@/lib/hooks/useStore'
import { useSearchParams } from 'next/navigation'
import MetalakePageLeftBar from './MetalakePageLeftBar'
import RightContent from './rightContent/RightContent'

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

const MetalakeView = () => {
  const dispatch = useAppDispatch()
  const searchParams = useSearchParams()

  const paramsSize = [...searchParams.keys()].length

  useEffect(() => {
    const routeParams = {
      metalake: searchParams.get('metalake'),
      catalog: searchParams.get('catalog'),
      schema: searchParams.get('schema'),
      table: searchParams.get('table')
    }
    if ([...searchParams.keys()].length) {
      const { metalake, catalog, schema, table } = routeParams

      if (paramsSize === 1 && metalake) {
        dispatch(fetchCatalogs({ init: true, page: 'metalakes', metalake }))
        dispatch(getMetalakeDetails({ metalake }))
      }

      if (paramsSize === 2 && catalog) {
        dispatch(fetchSchemas({ init: true, page: 'catalogs', metalake, catalog }))
        dispatch(getCatalogDetails({ metalake, catalog }))
      }

      if (paramsSize === 3 && catalog && schema) {
        dispatch(fetchTables({ init: true, page: 'schemas', metalake, catalog, schema }))
        dispatch(getSchemaDetails({ metalake, catalog, schema }))
      }

      if (paramsSize === 4 && catalog && schema && table) {
        dispatch(getTableDetails({ init: true, metalake, catalog, schema, table }))
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
  }, [searchParams])

  return (
    <Box className={'metalake-template'} style={{ height: 'calc(100vh - 11rem)' }}>
      <Box className='app-metalake twc-w-full twc-h-full twc-flex twc-rounded-lg twc-overflow-auto twc-relative shadow-md'>
        <Box className={`twc-bg-customs-white`} sx={{ borderRight: theme => `1px solid ${theme.palette.divider}` }}>
          <Box className={`twc-w-[340px] twc-h-full round-tl-md round-bl-md twc-overflow-hidden`}>
            <Box className={'twc-px-5 twc-py-3 twc-flex twc-items-center'}></Box>
            <MetalakePageLeftBar />
          </Box>
        </Box>
        <RightContent />
      </Box>
    </Box>
  )
}

export default MetalakeView
