/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { Grid, Typography } from '@mui/material'

import MetalakeList from './MetalakeList'

const MetalakesView = () => {
  return (
    <Grid container spacing={6}>
      <Grid item xs={12}>
        <Typography className={'twc-mb-4 twc-text-[1.375rem] twc-font-bold'}>Metalakes</Typography>
        <Typography sx={{ color: 'text.secondary' }}>
          A metalake is the top-level container for data in Gravitino. Within a metalake, Gravitino provides a 3-level
          namespace for organizing data: catalog, schemas/databases, and tables/views.
        </Typography>
      </Grid>
      <Grid item xs={12}>
        <MetalakeList />
      </Grid>
    </Grid>
  )
}

export default MetalakesView
