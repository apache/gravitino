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

import { useCallback, useState } from 'react'
import { Grid, Card } from '@mui/material'
import TableHeader from './TableHeader'
import DetailsDrawer from '@/components/DetailsDrawer'
import CreateMetalakeDialog from './CreateMetalakeDialog'
import dynamic from 'next/dynamic'
import Loading from '@/app/rootLayout/Loading'

const DynamicTableBody = dynamic(() => import('./TableBody'), {
  loading: () => <Loading height={'200px'} />,
  ssr: false
})

const MetalakeList = () => {
  const [value, setValue] = useState('')
  const [openDrawer, setOpenDrawer] = useState(false)
  const [drawerData, setDrawerData] = useState()
  const [openDialog, setOpenDialog] = useState(false)
  const [dialogData, setDialogData] = useState({})
  const [dialogType, setDialogType] = useState('create')

  const handleFilter = useCallback(val => {
    setValue(val)
  }, [])

  return (
    <Grid container spacing={6}>
      <Grid item xs={12}>
        <Card>
          <TableHeader
            value={value}
            handleFilter={handleFilter}
            setOpenDialog={setOpenDialog}
            setDialogData={setDialogData}
            setDialogType={setDialogType}
          />
          <DynamicTableBody
            value={value}
            setOpenDialog={setOpenDialog}
            setDialogData={setDialogData}
            setDialogType={setDialogType}
            setOpenDrawer={setOpenDrawer}
            setDrawerData={setDrawerData}
          />
          <DetailsDrawer openDrawer={openDrawer} setOpenDrawer={setOpenDrawer} drawerData={drawerData} />
        </Card>
      </Grid>
      <CreateMetalakeDialog open={openDialog} setOpen={setOpenDialog} data={dialogData} type={dialogType} />
    </Grid>
  )
}

export default MetalakeList
