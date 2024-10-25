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

import { useState, useEffect, Fragment } from 'react'

import Link from 'next/link'

import { Box, Typography, Portal, Tooltip, IconButton, Switch } from '@mui/material'
import { DataGrid, GridToolbar } from '@mui/x-data-grid'
import {
  VisibilityOutlined as ViewIcon,
  EditOutlined as EditIcon,
  DeleteOutlined as DeleteIcon
} from '@mui/icons-material'

import { formatToDateTime } from '@/lib/utils/date'
import { useAppDispatch, useAppSelector } from '@/lib/hooks/useStore'
import {
  fetchMetalakes,
  setFilteredMetalakes,
  deleteMetalake,
  resetTree,
  setMetalakeInUse
} from '@/lib/store/metalakes'
import { switchInUseApi } from '@/lib/api/metalakes'
import { to } from '@/lib/utils'
import ConfirmDeleteDialog from '@/components/ConfirmDeleteDialog'

const TableBody = props => {
  const { value, setOpenDialog, setDialogData, setDialogType, setDrawerData, setOpenDrawer } = props

  const dispatch = useAppDispatch()
  const store = useAppSelector(state => state.metalakes)
  const [paginationModel, setPaginationModel] = useState({ page: 0, pageSize: 10 })

  const [openConfirmDelete, setOpenConfirmDelete] = useState(false)
  const [confirmCacheData, setConfirmCacheData] = useState(null)

  const handleDeleteMetalake = name => () => {
    setOpenConfirmDelete(true)
    setConfirmCacheData({ name, type: 'metalake' })
  }

  const handleConfirmDeleteSubmit = () => {
    if (confirmCacheData) {
      dispatch(deleteMetalake(confirmCacheData.name))
      setOpenConfirmDelete(false)
    }
  }

  const handleCloseConfirm = () => {
    setOpenConfirmDelete(false)
    setConfirmCacheData(null)
  }

  const handleShowEditDialog = data => () => {
    setDialogType('update')
    setOpenDialog(true)
    setDialogData(data)
  }

  const handleShowDetails = row => () => {
    setDrawerData(row)
    setOpenDrawer(true)
  }

  const handleClickLink = () => {
    dispatch(resetTree())
  }

  const handleChangeInUse = async (name, isInUse) => {
    const [err, res] = await to(switchInUseApi({ name, isInUse }))
    if (err || !res) {
      throw new Error(err)
    }
    dispatch(setMetalakeInUse({ name, isInUse }))
  }

  useEffect(() => {
    dispatch(fetchMetalakes())
  }, [dispatch])

  useEffect(() => {
    const filteredData = store.metalakes
      .filter(i => i.name.toLowerCase().includes(value.toLowerCase()))
      .sort((a, b) => {
        if (a.name.toLowerCase() === value.toLowerCase()) return -1
        if (b.name.toLowerCase() === value.toLowerCase()) return 1

        return 0
      })

    dispatch(setFilteredMetalakes(filteredData))
  }, [dispatch, store.metalakes, value])

  /** @type {import('@mui/x-data-grid').GridColDef[]} */
  const columns = [
    {
      flex: 0.2,
      minWidth: 230,
      disableColumnMenu: true,
      filterable: true,
      type: 'string',
      field: 'name',
      headerName: 'Name',
      renderCell: ({ row }) => {
        const { name } = row

        return (
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            <Tooltip title={row.comment} placement='right'>
              {row.properties['in-use'] === 'true' ? (
                <Typography
                  noWrap
                  component={Link}
                  href={`/metalakes?metalake=${name}`}
                  onClick={() => handleClickLink()}
                  sx={{
                    fontWeight: 500,
                    color: 'primary.main',
                    textDecoration: 'none',
                    maxWidth: 240,
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    '&:hover': { color: 'primary.main', textDecoration: 'underline' }
                  }}
                  data-refer={`metalake-link-${name}`}
                >
                  {name}
                </Typography>
              ) : (
                <Typography>{name}</Typography>
              )}
            </Tooltip>
            <Tooltip title={row.properties['in-use'] === 'true' ? 'In-use' : 'Not In-use'} placement='right'>
              <Switch
                data-refer={`metalake-in-use-${name}`}
                checked={row.properties['in-use'] === 'true'}
                onChange={(e, value) => handleChangeInUse(name, value)}
                size='small'
              />
            </Tooltip>
          </Box>
        )
      }
    },
    {
      flex: 0.15,
      minWidth: 150,
      disableColumnMenu: true,
      type: 'string',
      field: 'createdBy',
      valueGetter: params => `${params.row.audit?.creator}`,
      headerName: 'Created by',
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
      minWidth: 150,
      disableColumnMenu: true,
      type: 'dateTime',
      field: 'createdAt',
      valueGetter: params => new Date(params.row.audit?.createTime),
      headerName: 'Created at',
      renderCell: ({ row }) => {
        return (
          <Typography title={row.audit?.createTime} noWrap sx={{ color: 'text.secondary' }}>
            {formatToDateTime(row.audit?.createTime)}
          </Typography>
        )
      }
    },
    {
      flex: 0.1,
      minWidth: 90,
      type: 'actions',
      headerName: 'Actions',
      field: 'actions',
      renderCell: ({ id, row }) => (
        <>
          <IconButton
            title='Details'
            size='small'
            sx={{ color: theme => theme.palette.text.secondary }}
            onClick={handleShowDetails(row)}
            data-refer={`view-metalake-${row.name}`}
          >
            <ViewIcon viewBox='0 0 24 22' />
          </IconButton>

          <IconButton
            title='Edit'
            size='small'
            sx={{ color: theme => theme.palette.text.secondary }}
            onClick={handleShowEditDialog(row)}
            data-refer={`edit-metalake-${row.name}`}
          >
            <EditIcon />
          </IconButton>

          <IconButton
            title='Delete'
            size='small'
            sx={{ color: theme => theme.palette.error.light }}
            onClick={handleDeleteMetalake(row.name)}
            data-refer={`delete-metalake-${row.name}`}
          >
            <DeleteIcon />
          </IconButton>
        </>
      )
    }
  ]

  function TableToolbar(props) {
    return (
      <>
        <Fragment>
          <Portal container={() => document.getElementById('filter-panel')}>
            <Box className={`twc-flex twc-w-full twc-justify-between`}>
              <GridToolbar {...props} />
            </Box>
          </Portal>
        </Fragment>
      </>
    )
  }

  return (
    <>
      <DataGrid
        disableColumnSelector
        disableDensitySelector
        slots={{ toolbar: TableToolbar }}
        slotProps={{
          toolbar: {
            printOptions: { disableToolbarButton: true },
            csvOptions: { disableToolbarButton: true }
          }
        }}
        sx={{
          '& .MuiDataGrid-virtualScroller': {
            minHeight: 36
          },
          maxHeight: 'calc(100vh - 23.2rem)'
        }}
        data-refer='metalake-table-grid'
        getRowId={row => row?.name}
        rows={store.filteredMetalakes}
        columns={columns}
        disableRowSelectionOnClick
        onCellClick={(params, event) => event.stopPropagation()}
        onRowClick={(params, event) => event.stopPropagation()}
        pageSizeOptions={[10, 25, 50]}
        paginationModel={paginationModel}
        onPaginationModelChange={setPaginationModel}
      />
      <ConfirmDeleteDialog
        open={openConfirmDelete}
        setOpen={setOpenConfirmDelete}
        confirmCacheData={confirmCacheData}
        handleClose={handleCloseConfirm}
        handleConfirmDeleteSubmit={handleConfirmDeleteSubmit}
      />
    </>
  )
}

export default TableBody
