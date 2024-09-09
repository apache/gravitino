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

'use client'

import { Grid, Typography } from '@mui/material'
import { useSearchParams } from 'next/navigation'
import Metalake from './metalake/MetalakeView.js'

const MetalakesView = ({ children }) => {
  const searchParams = useSearchParams()
  const metalakeName = searchParams.get('metalake')

  return (
    <>
      {metalakeName ? (
        <Metalake />
      ) : (
        <Grid container spacing={6}>
          <Grid item xs={12}>
            <Typography className={'twc-mb-4 twc-text-[1.375rem] twc-font-bold'} data-refer='metalake-page-title'>
              Metalakes
            </Typography>
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
