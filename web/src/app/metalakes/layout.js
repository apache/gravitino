/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { Grid, Typography } from '@mui/material'
import { useSearchParams } from 'next/navigation'
import { useEffect, useState } from 'react'
import Metalake from './metalake/MetalakeView.js'

const MetalakesView = ({ children }) => {
  const searchParams = useSearchParams()

  const [, setRouteParams] = useState({})

  const metalakeName = searchParams.get('metalake')
  const catalogName = searchParams.get('catalog')
  const schemaName = searchParams.get('schema')
  const tableName = searchParams.get('table')

  useEffect(() => {
    const takeParams = {
      metalake: metalakeName,
      catalog: catalogName,
      schema: schemaName,
      table: tableName
    }
    setRouteParams(takeParams)
  }, [metalakeName, catalogName, schemaName, tableName])

  return (
    <>
      {metalakeName ? (
        <Metalake />
      ) : (
        <Grid container spacing={6}>
          <Grid item xs={12}>
            <Typography className={'twc-mb-4 twc-text-[1.375rem] twc-font-bold'}>Metalakes</Typography>
            <Typography sx={{ color: 'text.secondary' }}>
              A metalake is the top-level container for data in Gravitino. Within a metalake, Gravitino provides a
              3-level namespace for organizing data: catalog, schemas/databases, and tables/views.
            </Typography>
          </Grid>
          <Grid item xs={12}>
            {children}
          </Grid>
        </Grid>
      )}
    </>
  )
}

export default MetalakesView
