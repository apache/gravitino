/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import { useState, useEffect } from 'react'

import Link from 'next/link'

import { Box, Typography, IconButton } from '@mui/material'
import { DataGrid } from '@mui/x-data-grid'
import {
  VisibilityOutlined as ViewIcon,
  EditOutlined as EditIcon,
  DeleteOutlined as DeleteIcon
} from '@mui/icons-material'

import Icon from '@/components/Icon'

import ColumnTypeChip from '@/components/ColumnTypeChip'
import DetailsDrawer from '@/components/DetailsDrawer'
import ConfirmDeleteDialog from '@/components/ConfirmDeleteDialog'
import CreateCatalogDialog from './CreateCatalogDialog'

import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'
import { updateCatalog, deleteCatalog } from '@/lib/store/metalakes'

import { to } from '@/lib/utils'
import { getCatalogDetailsApi } from '@/lib/api/catalogs'

const EmptyText = () => {
  return (
    <Typography variant='caption' color={theme => theme.palette.text.disabled}>
      N/A
    </Typography>
  )
}

const TableView = props => {
  const { page, routeParams } = props
  const { metalake, catalog } = routeParams

  const defaultPaginationConfig = { pageSize: 10, page: 0 }
  const pageSizeOptions = [10, 25, 50]

  const dispatch = useAppDispatch()

  const [paginationModel, setPaginationModel] = useState(defaultPaginationConfig)
  const store = useAppSelector(state => state.metalakes)

  const [openDrawer, setOpenDrawer] = useState(false)
  const [drawerData, setDrawerData] = useState()
  const [confirmCacheData, setConfirmCacheData] = useState(null)
  const [openConfirmDelete, setOpenConfirmDelete] = useState(false)
  const [openDialog, setOpenDialog] = useState(false)
  const [dialogData, setDialogData] = useState({})
  const [dialogType, setDialogType] = useState('create')

  const handleClickUrl = path => {
    if (!path) {
      return
    }
  }

  const columns = [
    {
      flex: 0.1,
      minWidth: 60,
      disableColumnMenu: true,
      type: 'string',
      field: 'name',
      headerName: 'Name',
      renderCell: ({ row }) => {
        const { name, path } = row

        return (
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            <Typography
              noWrap
              {...(path
                ? {
                    component: Link,
                    href: path
                  }
                : {})}
              onClick={() => handleClickUrl(path)}
              sx={{
                fontWeight: 400,
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
    }
  ]

  const catalogsColumns = [
    {
      flex: 0.1,
      minWidth: 60,
      disableColumnMenu: true,
      type: 'string',
      field: 'name',
      headerName: 'Name',
      renderCell: ({ row }) => {
        const { name, path } = row

        return (
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            <Typography
              noWrap
              {...(path
                ? {
                    component: Link,
                    href: path
                  }
                : {})}
              onClick={() => handleClickUrl(path)}
              sx={{
                fontWeight: 400,
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
      flex: 0.1,
      minWidth: 90,
      sortable: false,
      disableColumnMenu: true,
      type: 'actions',
      field: 'actions',
      headerName: 'Actions',
      renderCell: ({ row }) => (
        <>
          <IconButton
            title='Details'
            size='small'
            sx={{ color: theme => theme.palette.text.secondary }}
            onClick={() => handleShowDetails({ row, type: 'catalog' })}
          >
            <ViewIcon viewBox='0 0 24 22' />
          </IconButton>

          <IconButton
            title='Edit'
            size='small'
            sx={{ color: theme => theme.palette.text.secondary }}
            onClick={() => handleShowEditDialog({ row, type: 'catalog' })}
          >
            <EditIcon />
          </IconButton>

          <IconButton
            title='Delete'
            size='small'
            sx={{ color: theme => theme.palette.error.light }}
            onClick={() => handleDelete({ name: row.name, type: 'catalog' })}
          >
            <DeleteIcon />
          </IconButton>
        </>
      )
    }
  ]

  const tableColumns = [
    {
      flex: 0.1,
      minWidth: 60,
      disableColumnMenu: true,
      type: 'string',
      field: 'name',
      headerName: 'Name',
      renderCell: ({ row }) => {
        const { name } = row

        return (
          <Box sx={{ width: '100%', display: 'flex', alignItems: 'center' }}>
            <Typography
              title={name}
              noWrap
              sx={{
                fontWeight: 400,
                color: 'text.main',
                textDecoration: 'none'
              }}
            >
              {name}
            </Typography>
          </Box>
        )
      }
    },
    {
      flex: 0.1,
      minWidth: 60,
      disableColumnMenu: true,
      type: 'string',
      field: 'type',
      headerName: 'Type',
      renderCell: ({ row }) => {
        const { type } = row

        return (
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            <ColumnTypeChip type={type} />
          </Box>
        )
      }
    },
    {
      flex: 0.1,
      minWidth: 60,
      disableColumnMenu: true,
      type: 'boolean',
      field: 'nullable',
      headerName: 'Nullable',
      renderCell: ({ row }) => {
        const { nullable } = row

        return (
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            <Typography
              noWrap
              variant='body2'
              sx={{
                fontWeight: 400,
                color: 'text.secondary',
                textDecoration: 'none'
              }}
            >
              {typeof nullable !== 'undefined' && `${nullable}`}
            </Typography>
          </Box>
        )
      }
    },
    {
      flex: 0.1,
      minWidth: 60,
      disableColumnMenu: true,
      type: 'boolean',
      field: 'autoIncrement',
      headerName: 'AutoIncrement',
      renderCell: ({ row }) => {
        const { autoIncrement } = row

        return typeof autoIncrement !== 'undefined' ? (
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            <Typography
              noWrap
              variant='body2'
              sx={{
                fontWeight: 400,
                color: 'text.secondary',
                textDecoration: 'none'
              }}
            >
              {`${autoIncrement}`}
            </Typography>
          </Box>
        ) : (
          <EmptyText />
        )
      }
    },
    {
      flex: 0.1,
      minWidth: 60,
      disableColumnMenu: true,
      type: 'string',
      field: 'comment',
      headerName: 'Comment',
      renderCell: ({ row }) => {
        const { comment } = row

        return typeof comment !== 'undefined' ? (
          <Box sx={{ width: '100%', display: 'flex', alignItems: 'center' }}>
            <Typography
              noWrap
              title={comment}
              variant='body2'
              sx={{
                fontWeight: 400,
                color: 'text.secondary',
                textDecoration: 'none'
              }}
            >
              {comment}
            </Typography>
          </Box>
        ) : (
          <EmptyText />
        )
      }
    }
  ]

  const handleShowDetails = async ({ row, type }) => {
    if (type === 'catalog') {
      const [err, res] = await to(getCatalogDetailsApi({ metalake, catalog: row.name }))

      if (err || !res) {
        throw new Error(err)
      }

      setDrawerData(res.catalog)
      setOpenDrawer(true)
    }
  }

  const handleShowEditDialog = async data => {
    const metalake = data.row.namespace[0] || null
    const catalog = data.row.name || null

    if (metalake && catalog) {
      const [err, res] = await to(getCatalogDetailsApi({ metalake, catalog }))

      if (err || !res) {
        throw new Error(err)
      }

      const { catalog: resCatalog } = res

      setDialogType('update')
      setDialogData(resCatalog)
      setOpenDialog(true)
    }
  }

  const handleDelete = ({ name, type }) => {
    setOpenConfirmDelete(true)
    setConfirmCacheData({ name, type })
  }

  const handleCloseConfirm = () => {
    setOpenConfirmDelete(false)
    setConfirmCacheData(null)
  }

  const handleConfirmDeleteSubmit = () => {
    if (confirmCacheData) {
      if (confirmCacheData.type === 'catalog') {
        dispatch(deleteCatalog({ metalake, catalog: confirmCacheData.name }))
      }

      setOpenConfirmDelete(false)
    }
  }

  const checkColumns = () => {
    if (page === 'metalakes') {
      return catalogsColumns
    } else if (page === 'tables') {
      return tableColumns
    } else {
      return columns
    }
  }

  useEffect(() => {
    setPaginationModel({ ...paginationModel, page: 0 })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [store.tableLoading])

  return (
    <Box className={`twc-h-full`}>
      <DataGrid
        sx={{
          '& .MuiDataGrid-columnHeaders': {
            borderTopLeftRadius: 0,
            borderTopRightRadius: 0,
            borderTop: 0
          }
        }}
        loading={store.tableLoading}
        rows={store.tableData}
        getRowId={row => row?.name}
        columns={checkColumns()}
        disableRowSelectionOnClick
        pageSizeOptions={pageSizeOptions}
        paginationModel={paginationModel}
        onPaginationModelChange={setPaginationModel}
      />
      <DetailsDrawer openDrawer={openDrawer} setOpenDrawer={setOpenDrawer} drawerData={drawerData} page={page} />
      <ConfirmDeleteDialog
        open={openConfirmDelete}
        setOpen={setOpenConfirmDelete}
        confirmCacheData={confirmCacheData}
        handleClose={handleCloseConfirm}
        handleConfirmDeleteSubmit={handleConfirmDeleteSubmit}
      />

      <CreateCatalogDialog
        open={openDialog}
        setOpen={setOpenDialog}
        updateCatalog={updateCatalog}
        data={dialogData}
        type={dialogType}
        routeParams={routeParams}
      />
    </Box>
  )
}

export default TableView
