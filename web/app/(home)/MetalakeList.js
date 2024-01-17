/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import { useEffect, useCallback, useState } from 'react'

import Link from 'next/link'

import { Box, Grid, Card, IconButton, Typography, Tooltip } from '@mui/material'
import { DataGrid } from '@mui/x-data-grid'

import Icon from '@/components/Icon'

import { useAppDispatch, useAppSelector } from '@/lib/hooks/useStore'
import { fetchMetalakes, setFilteredMetalakes, deleteMetalake, updateMetalake } from '@/lib/store/metalakes'

import { formatToDateTime } from '@/lib/utils/date'
import TableHeader from './TableHeader'
import DetailsDrawer from './DetailsDrawer'
import CreateMetalakeDialog from './CreateMetalakeDialog'
import ConfirmDeleteDialog from '@/components/ConfirmDeleteDialog'

const MetalakeList = () => {
  const dispatch = useAppDispatch()
  const store = useAppSelector(state => state.metalakes)

  const [value, setValue] = useState('')
  const [paginationModel, setPaginationModel] = useState({ page: 0, pageSize: 10 })
  const [openDrawer, setOpenDrawer] = useState(false)
  const [drawerData, setDrawerData] = useState()
  const [openDialog, setOpenDialog] = useState(false)
  const [dialogData, setDialogData] = useState({})
  const [dialogType, setDialogType] = useState('create')
  const [openConfirmDelete, setOpenConfirmDelete] = useState(false)
  const [confirmCacheData, setConfirmCacheData] = useState(null)

  const handleDeleteMetalake = name => {
    setOpenConfirmDelete(true)
    setConfirmCacheData(name)
  }

  const handleConfirmDeleteSubmit = () => {
    if (confirmCacheData) {
      dispatch(deleteMetalake(confirmCacheData))
      setOpenConfirmDelete(false)
    }
  }

  const handleCloseConfirm = () => {
    setOpenConfirmDelete(false)
    setConfirmCacheData(null)
  }

  const handleShowEditDialog = data => {
    setDialogType('update')
    setOpenDialog(true)
    setDialogData(data)
  }

  const handleFilter = useCallback(val => {
    setValue(val)
  }, [])

  const handleShowDetails = row => {
    setDrawerData(row)
    setOpenDrawer(true)
  }

  useEffect(() => {
    dispatch(fetchMetalakes())
  }, [dispatch])

  useEffect(() => {
    const filteredData = store.metalakes.filter(i => i.name.toLowerCase().includes(value.toLowerCase()))

    dispatch(setFilteredMetalakes(filteredData))
  }, [dispatch, store.metalakes, value])

  const columns = [
    {
      flex: 0.2,
      minWidth: 230,
      field: 'name',
      headerName: 'Name',
      renderCell: ({ row }) => {
        const { name } = row

        return (
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            <Typography
              noWrap
              component={Link}
              href={`/metalakes/?metalake=${name}`}
              sx={{
                fontWeight: 500,
                color: 'primary.main',
                textDecoration: 'none',
                '&:hover': { color: 'primary.main', textDecoration: 'underline' }
              }}
            >
              {name}
            </Typography>
          </Box>
        )
      }
    },
    {
      flex: 0.15,
      minWidth: 150,
      field: 'createdBy',
      headerName: 'Created By',
      renderCell: ({ row }) => {
        return (
          <Typography noWrap sx={{ color: 'text.secondary' }}>
            {row.audit?.creator}
          </Typography>
        )
      }
    },
    {
      flex: 0.15,
      field: 'createdAt',
      minWidth: 150,
      headerName: 'Created At',
      renderCell: ({ row }) => {
        return (
          <Typography noWrap sx={{ color: 'text.secondary' }}>
            {formatToDateTime(row.audit?.createTime)}
          </Typography>
        )
      }
    },
    {
      flex: 0.1,
      minWidth: 90,
      sortable: false,
      field: 'actions',
      headerName: 'Actions',
      renderCell: ({ row }) => (
        <>
          <Tooltip title='Details' placement='top'>
            <IconButton
              size='small'
              sx={{ color: theme => theme.palette.text.secondary }}
              onClick={() => handleShowDetails(row)}
            >
              <Icon icon='bx:show-alt' />
            </IconButton>
          </Tooltip>

          <Tooltip title='Edit' placement='top'>
            <IconButton
              size='small'
              sx={{ color: theme => theme.palette.text.secondary }}
              onClick={() => handleShowEditDialog(row)}
            >
              <Icon icon='mdi:square-edit-outline' />
            </IconButton>
          </Tooltip>

          <Tooltip title='Delete' placement='top'>
            <IconButton
              size='small'
              sx={{ color: theme => theme.palette.error.light }}
              onClick={() => handleDeleteMetalake(row.name)}
            >
              <Icon icon='mdi:delete-outline' />
            </IconButton>
          </Tooltip>
        </>
      )
    }
  ]

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
          <DataGrid
            style={{ maxHeight: 'calc(100vh - 23.2rem)' }}
            getRowId={row => row?.name}
            rows={store.filteredMetalakes}
            columns={columns}
            disableRowSelectionOnClick
            pageSizeOptions={[10, 25, 50]}
            paginationModel={paginationModel}
            onPaginationModelChange={setPaginationModel}
          />
          <DetailsDrawer openDrawer={openDrawer} setOpenDrawer={setOpenDrawer} drawerData={drawerData} />
        </Card>
      </Grid>
      <CreateMetalakeDialog
        open={openDialog}
        setOpen={setOpenDialog}
        updateMetalake={updateMetalake}
        data={dialogData}
        type={dialogType}
      />
      <ConfirmDeleteDialog
        open={openConfirmDelete}
        setOpen={setOpenConfirmDelete}
        confirmCacheData={confirmCacheData}
        handleClose={handleCloseConfirm}
        handleConfirmDeleteSubmit={handleConfirmDeleteSubmit}
      />
    </Grid>
  )
}

export default MetalakeList
