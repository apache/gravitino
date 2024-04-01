/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
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
