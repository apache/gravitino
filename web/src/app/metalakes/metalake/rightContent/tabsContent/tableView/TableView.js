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

import { Inconsolata } from 'next/font/google'

import { useState, useEffect, Fragment } from 'react'

import Link from 'next/link'

import { styled, Box, Typography, IconButton, Stack } from '@mui/material'
import Tooltip, { tooltipClasses } from '@mui/material/Tooltip'
import { DataGrid } from '@mui/x-data-grid'
import {
  VisibilityOutlined as ViewIcon,
  EditOutlined as EditIcon,
  DeleteOutlined as DeleteIcon,
} from '@mui/icons-material'

import Icon from '@/components/Icon'

import ColumnTypeChip from '@/components/ColumnTypeChip'
import DetailsDrawer from '@/components/DetailsDrawer'
import ConfirmDeleteDialog from '@/components/ConfirmDeleteDialog'
import CreateCatalogDialog from '../../CreateCatalogDialog'

import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'
import { updateCatalog, deleteCatalog } from '@/lib/store/metalakes'

import { to } from '@/lib/utils'
import { getCatalogDetailsApi } from '@/lib/api/catalogs'
import { useSearchParams } from 'next/navigation'

const fonts = Inconsolata({ subsets: ['latin'] })

const EmptyText = () => {
  return (
    <Typography variant='caption' color={theme => theme.palette.text.disabled}>
      N/A
    </Typography>
  )
}

const CustomTooltip = styled(({ className, ...props }) => <Tooltip {...props} classes={{ popper: className }} />)(
  ({ theme }) => ({
    [`& .${tooltipClasses.tooltip}`]: {
      backgroundColor: '#23282a',
      padding: 0,
      border: '1px solid #dadde9',
    },
  })
)

const TableView = () => {
  const searchParams = useSearchParams()
  const paramsSize = [...searchParams.keys()].length
  const metalake = searchParams.get('metalake') || ''

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

  const renderIconTooltip = (type, name) => {
    const propsItem = store.tableProps.find(i => i.type === type)
    const items = propsItem?.items || []

    const isCond = propsItem?.items.find(i => i.fields.find(v => (Array.isArray(v) ? v.includes(name) : v === name)))

    const icon = propsItem?.icon

    return (
      <>
        {isCond && (
          <CustomTooltip
            placement='right'
            title={
              <>
                <Box
                  sx={{
                    backgroundColor: '#525c61',
                    p: 1.5,
                    px: 4,
                    borderTopLeftRadius: 4,
                    borderTopRightRadius: 4,
                  }}
                >
                  <Typography color='white' fontWeight={700} fontSize={14} sx={{ textTransform: 'capitalize' }}>
                    {type}:{' '}
                  </Typography>
                </Box>

                <Box sx={{ p: 1.5, px: 4 }}>
                  {items.map((it, idx) => {
                    return (
                      <Fragment key={idx}>
                        <Typography
                          variant='caption'
                          color='white'
                          className={fonts.className}
                          sx={{ display: 'flex', flexDirection: 'column' }}
                          data-refer={`tip-${type}-item-${name}`}
                        >
                          {it.text || it.fields}
                        </Typography>
                        {idx < items.length - 1 && (
                          <Box
                            component={'span'}
                            sx={{
                              display: 'block',
                              my: 1,
                              borderTop: theme => `1px solid ${theme.palette.grey[800]}`,
                            }}
                          ></Box>
                        )}
                      </Fragment>
                    )
                  })}
                </Box>
              </>
            }
          >
            <Box sx={{ display: 'flex', alignItems: 'center' }} data-refer={`col-icon-${type}-${name}`}>
              <Icon icon={icon} />
            </Box>
          </CustomTooltip>
        )}
      </>
    )
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
                    href: path,
                  }
                : {})}
              onClick={() => handleClickUrl(path)}
              sx={{
                fontWeight: 400,
                color: 'primary.main',
                textDecoration: 'none',
                '&:hover': { color: 'primary.main', textDecoration: 'underline' },
              }}
            >
              {name}
            </Typography>
          </Box>
        )
      },
    },
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
            <Tooltip title={row.comment} placement='right'>
              <Typography
                noWrap
                {...(path
                  ? {
                      component: Link,
                      href: path,
                    }
                  : {})}
                onClick={() => handleClickUrl(path)}
                sx={{
                  fontWeight: 400,
                  color: 'primary.main',
                  textDecoration: 'none',
                  '&:hover': { color: 'primary.main', textDecoration: 'underline' },
                }}
              >
                {name}
              </Typography>
            </Tooltip>
          </Box>
        )
      },
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
            data-refer={`view-catalog-${row.name}`}
          >
            <ViewIcon viewBox='0 0 24 22' />
          </IconButton>

          <IconButton
            title='Edit'
            size='small'
            sx={{ color: theme => theme.palette.text.secondary }}
            onClick={() => handleShowEditDialog({ row, type: 'catalog' })}
            data-refer={`edit-catalog-${row.name}`}
          >
            <EditIcon />
          </IconButton>

          <IconButton
            title='Delete'
            size='small'
            sx={{ color: theme => theme.palette.error.light }}
            onClick={() => handleDelete({ name: row.name, type: 'catalog', catalogType: row.type })}
            data-refer={`delete-catalog-${row.name}`}
          >
            <DeleteIcon />
          </IconButton>
        </>
      ),
    },
  ]

  const tableColumns = [
    {
      flex: 0.15,
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
                pr: 4,
                fontWeight: 400,
                color: 'text.main',
                textDecoration: 'none',
              }}
            >
              {name}
            </Typography>
            <Stack spacing={0} direction={'row'}>
              {renderIconTooltip('partitioning', name)}
              {renderIconTooltip('sortOrders', name)}
              {renderIconTooltip('distribution', name)}
              {renderIconTooltip('indexes', name)}
            </Stack>
          </Box>
        )
      },
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
      },
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
                textDecoration: 'none',
              }}
            >
              {typeof nullable !== 'undefined' && `${nullable}`}
            </Typography>
          </Box>
        )
      },
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
                textDecoration: 'none',
              }}
            >
              {`${autoIncrement}`}
            </Typography>
          </Box>
        ) : (
          <EmptyText />
        )
      },
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
                textDecoration: 'none',
              }}
            >
              {comment}
            </Typography>
          </Box>
        ) : (
          <EmptyText />
        )
      },
    },
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

  const handleDelete = ({ name, type, catalogType }) => {
    setOpenConfirmDelete(true)
    setConfirmCacheData({ name, type, catalogType })
  }

  const handleCloseConfirm = () => {
    setOpenConfirmDelete(false)
    setConfirmCacheData(null)
  }

  const handleConfirmDeleteSubmit = () => {
    if (confirmCacheData) {
      if (confirmCacheData.type === 'catalog') {
        dispatch(deleteCatalog({ metalake, catalog: confirmCacheData.name, type: confirmCacheData.catalogType }))
      }

      setOpenConfirmDelete(false)
    }
  }

  const checkColumns = () => {
    if (paramsSize == 1 && searchParams.has('metalake')) {
      return catalogsColumns
    } else if (paramsSize == 5 && searchParams.has('table')) {
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
            borderTop: 0,
          },
        }}
        data-refer='table-grid'
        loading={store.tableLoading}
        rows={store.tableData}
        getRowId={row => row?.name}
        columns={checkColumns()}
        disableRowSelectionOnClick
        pageSizeOptions={pageSizeOptions}
        paginationModel={paginationModel}
        onPaginationModelChange={setPaginationModel}
      />
      <DetailsDrawer
        openDrawer={openDrawer}
        setOpenDrawer={setOpenDrawer}
        drawerData={drawerData}
        isMetalakePage={paramsSize == 1 && searchParams.hasOwnProperty('metalake')}
      />
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
      />
    </Box>
  )
}

export default TableView
