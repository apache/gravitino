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

import { styled, Box, Typography, IconButton, Stack, Switch } from '@mui/material'
import Tooltip, { tooltipClasses } from '@mui/material/Tooltip'
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
import CreateCatalogDialog from '../../CreateCatalogDialog'
import CreateSchemaDialog from '../../CreateSchemaDialog'
import CreateFilesetDialog from '../../CreateFilesetDialog'
import CreateTopicDialog from '../../CreateTopicDialog'
import CreateTableDialog from '../../CreateTableDialog'

import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'
import {
  deleteCatalog,
  deleteFileset,
  deleteTopic,
  deleteSchema,
  deleteTable,
  deleteModel,
  deleteVersion,
  setCatalogInUse
} from '@/lib/store/metalakes'

import { to } from '@/lib/utils'
import { getCatalogDetailsApi, switchInUseApi } from '@/lib/api/catalogs'
import { getSchemaDetailsApi } from '@/lib/api/schemas'
import { useSearchParams } from 'next/navigation'
import { getFilesetDetailsApi } from '@/lib/api/filesets'
import { getTopicDetailsApi } from '@/lib/api/topics'
import { getTableDetailsApi } from '@/lib/api/tables'
import { getModelDetailsApi, getVersionDetailsApi } from '@/lib/api/models'

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
      border: '1px solid #dadde9'
    }
  })
)

const TableView = () => {
  const searchParams = useSearchParams()
  const paramsSize = [...searchParams.keys()].length
  const metalake = searchParams.get('metalake') || ''
  const catalog = searchParams.get('catalog') || ''
  const type = searchParams.get('type') || ''
  const schema = searchParams.get('schema') || ''
  const model = searchParams.get('model') || ''

  const isCatalogList = paramsSize == 1 && searchParams.has('metalake')

  const isKafkaSchema =
    paramsSize == 3 &&
    searchParams.has('metalake') &&
    searchParams.has('catalog') &&
    searchParams.get('type') === 'messaging'

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
  const [openSchemaDialog, setOpenSchemaDialog] = useState(false)
  const [openFilesetDialog, setOpenFilesetDialog] = useState(false)
  const [openTopicDialog, setOpenTopicDialog] = useState(false)
  const [openTableDialog, setOpenTableDialog] = useState(false)
  const [dialogData, setDialogData] = useState({})
  const [dialogType, setDialogType] = useState('create')
  const [isHideEdit, setIsHideEdit] = useState(true)
  const [isHideDrop, setIsHideDrop] = useState(true)

  useEffect(() => {
    if (store.catalogs.length) {
      const currentCatalog = store.catalogs.filter(ca => ca.name === catalog)[0]

      const isHideAction =
        (['lakehouse-hudi', 'kafka'].includes(currentCatalog?.provider) && paramsSize == 3) ||
        (currentCatalog?.provider === 'lakehouse-hudi' && paramsSize == 4)
      setIsHideEdit(isHideAction || type === 'model')
      setIsHideDrop(isHideAction)
    }
  }, [store.catalogs, store.catalogs.length, paramsSize, catalog, type])

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
                    borderTopRightRadius: 4
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
                              borderTop: theme => `1px solid ${theme.palette.grey[800]}`
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

  const actionsColumns = [
    {
      flex: 0.1,
      minWidth: 60,
      disableColumnMenu: true,
      type: 'string',
      field: 'name',
      headerName: model ? 'Version' : 'Name',
      renderCell: ({ row }) => {
        const { name, path } = row

        return (
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            <Tooltip title={row.comment} placement='left'>
              {(isCatalogList && row.inUse === 'true') || !isCatalogList ? (
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
              ) : (
                <Typography>{name}</Typography>
              )}
            </Tooltip>
            {isCatalogList && (
              <Tooltip title={row.inUse === 'true' ? 'In-use' : 'Not In-use'} placement='right'>
                <Switch
                  data-refer={`catalog-in-use-${name}`}
                  checked={row.inUse === 'true'}
                  onChange={(e, value) => handleChangeInUse(name, row.type, value)}
                  size='small'
                />
              </Tooltip>
            )}
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
            onClick={() => handleShowDetails({ row, type: row.node })}
            data-refer={`view-entity-${row.name}`}
          >
            <ViewIcon viewBox='0 0 24 22' />
          </IconButton>

          {!isHideEdit && (
            <IconButton
              title='Edit'
              size='small'
              sx={{ color: theme => theme.palette.text.secondary }}
              onClick={() => handleShowEditDialog({ row, type: row.node })}
              data-refer={`edit-entity-${row.name}`}
            >
              <EditIcon />
            </IconButton>
          )}

          {!isHideDrop && (
            <IconButton
              title='Delete'
              size='small'
              sx={{ color: theme => theme.palette.error.light }}
              onClick={() =>
                handleDelete({ name: row.name, type: row.node, catalogType: row.type, inUse: row.inUse === 'true' })
              }
              data-refer={`delete-entity-${row.name}`}
            >
              <DeleteIcon />
            </IconButton>
          )}
        </>
      )
    }
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
                textDecoration: 'none'
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
    switch (type) {
      case 'catalog': {
        const [err, res] = await to(getCatalogDetailsApi({ metalake, catalog: row.name }))

        if (err || !res) {
          throw new Error(err)
        }

        setDrawerData(res.catalog)
        setOpenDrawer(true)
        break
      }
      case 'schema': {
        const [err, res] = await to(getSchemaDetailsApi({ metalake, catalog, schema: row.name }))

        if (err || !res) {
          throw new Error(err)
        }

        setDrawerData(res.schema)
        setOpenDrawer(true)
        break
      }
      case 'fileset': {
        const [err, res] = await to(getFilesetDetailsApi({ metalake, catalog, schema, fileset: row.name }))
        if (err || !res) {
          throw new Error(err)
        }

        setDrawerData(res.fileset)
        setOpenDrawer(true)
        break
      }
      case 'topic': {
        const [err, res] = await to(getTopicDetailsApi({ metalake, catalog, schema, topic: row.name }))
        if (err || !res) {
          throw new Error(err)
        }

        setDrawerData(res.topic)
        setOpenDrawer(true)
        break
      }
      case 'table': {
        const [err, res] = await to(getTableDetailsApi({ metalake, catalog, schema, table: row.name }))
        if (err || !res) {
          throw new Error(err)
        }

        setDrawerData(res.table)
        setOpenDrawer(true)
        break
      }
      case 'model': {
        const [err, res] = await to(getModelDetailsApi({ metalake, catalog, schema, model: row.name }))
        if (err || !res) {
          throw new Error(err)
        }

        setDrawerData(res.model)
        setOpenDrawer(true)
        break
      }
      case 'version': {
        const [err, res] = await to(getVersionDetailsApi({ metalake, catalog, schema, model, version: row.name }))
        if (err || !res) {
          throw new Error(err)
        }

        setDrawerData(res.modelVersion)
        setOpenDrawer(true)
        break
      }
      default:
        return
    }
  }

  const handleShowEditDialog = async data => {
    switch (data.type) {
      case 'catalog': {
        const [err, res] = await to(getCatalogDetailsApi({ metalake, catalog: data.row?.name }))

        if (err || !res) {
          throw new Error(err)
        }

        const { catalog: resCatalog } = res

        setDialogType('update')
        setDialogData(resCatalog)
        setOpenDialog(true)
        break
      }
      case 'schema': {
        if (metalake && catalog) {
          const [err, res] = await to(getSchemaDetailsApi({ metalake, catalog, schema: data.row?.name }))

          if (err || !res) {
            throw new Error(err)
          }

          setDialogType('update')
          setDialogData(res.schema)
          setOpenSchemaDialog(true)
        }
        break
      }
      case 'fileset': {
        if (metalake && catalog && schema) {
          const [err, res] = await to(getFilesetDetailsApi({ metalake, catalog, schema, fileset: data.row?.name }))
          if (err || !res) {
            throw new Error(err)
          }

          setDialogType('update')
          setDialogData(res.fileset)
          setOpenFilesetDialog(true)
        }
        break
      }
      case 'topic': {
        if (metalake && catalog && schema) {
          const [err, res] = await to(getTopicDetailsApi({ metalake, catalog, schema, topic: data.row?.name }))
          if (err || !res) {
            throw new Error(err)
          }

          setDialogType('update')
          setDialogData(res.topic)
          setOpenTopicDialog(true)
        }
        break
      }
      case 'table': {
        if (metalake && catalog && schema) {
          const [err, res] = await to(getTableDetailsApi({ metalake, catalog, schema, table: data.row?.name }))
          if (err || !res) {
            throw new Error(err)
          }

          setDialogType('update')
          setDialogData(res.table)
          setOpenTableDialog(true)
        }
        break
      }
      default:
        return
    }
  }

  const handleDelete = ({ name, type, catalogType, inUse }) => {
    setOpenConfirmDelete(true)
    setConfirmCacheData({ name, type, catalogType, inUse })
  }

  const handleCloseConfirm = () => {
    setOpenConfirmDelete(false)
    setConfirmCacheData(null)
  }

  const handleConfirmDeleteSubmit = () => {
    if (confirmCacheData) {
      switch (confirmCacheData.type) {
        case 'catalog':
          dispatch(deleteCatalog({ metalake, catalog: confirmCacheData.name, type: confirmCacheData.catalogType }))
          break
        case 'schema':
          dispatch(deleteSchema({ metalake, catalog, type, schema: confirmCacheData.name }))
          break
        case 'fileset':
          dispatch(deleteFileset({ metalake, catalog, type, schema, fileset: confirmCacheData.name }))
          break
        case 'topic':
          dispatch(deleteTopic({ metalake, catalog, type, schema, topic: confirmCacheData.name }))
          break
        case 'table':
          dispatch(deleteTable({ metalake, catalog, type, schema, table: confirmCacheData.name }))
          break
        case 'model':
          dispatch(deleteModel({ metalake, catalog, type, schema, model: confirmCacheData.name }))
          break
        case 'version':
          dispatch(deleteVersion({ metalake, catalog, type, schema, model, version: confirmCacheData.name }))
          break
        default:
          break
      }

      setOpenConfirmDelete(false)
    }
  }

  const handleChangeInUse = async (name, catalogType, isInUse) => {
    const [err, res] = await to(switchInUseApi({ metalake, catalog: name, isInUse }))
    if (err || !res) {
      throw new Error(err)
    }
    dispatch(setCatalogInUse({ name, catalogType, metalake, isInUse }))
  }

  const checkColumns = () => {
    if (
      (paramsSize == 1 && searchParams.has('metalake')) ||
      (paramsSize == 3 && searchParams.has('metalake') && searchParams.has('catalog') && searchParams.has('type')) ||
      (paramsSize == 4 &&
        searchParams.has('metalake') &&
        searchParams.has('catalog') &&
        searchParams.get('type') === 'fileset' &&
        searchParams.has('schema')) ||
      (paramsSize == 4 &&
        searchParams.has('metalake') &&
        searchParams.has('catalog') &&
        searchParams.get('type') === 'messaging' &&
        searchParams.has('schema')) ||
      (paramsSize == 4 &&
        searchParams.has('metalake') &&
        searchParams.has('catalog') &&
        searchParams.get('type') === 'relational' &&
        searchParams.has('schema')) ||
      (paramsSize == 4 &&
        searchParams.has('metalake') &&
        searchParams.has('catalog') &&
        searchParams.get('type') === 'model' &&
        searchParams.has('schema')) ||
      (paramsSize == 5 &&
        searchParams.has('metalake') &&
        searchParams.has('catalog') &&
        searchParams.get('type') === 'model' &&
        searchParams.has('schema') &&
        searchParams.has('model'))
    ) {
      return actionsColumns
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
            borderTop: 0
          }
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
      <DetailsDrawer openDrawer={openDrawer} setOpenDrawer={setOpenDrawer} drawerData={drawerData} />
      <ConfirmDeleteDialog
        open={openConfirmDelete}
        setOpen={setOpenConfirmDelete}
        confirmCacheData={confirmCacheData}
        handleClose={handleCloseConfirm}
        handleConfirmDeleteSubmit={handleConfirmDeleteSubmit}
      />

      <CreateCatalogDialog open={openDialog} setOpen={setOpenDialog} data={dialogData} type={dialogType} />

      <CreateSchemaDialog open={openSchemaDialog} setOpen={setOpenSchemaDialog} data={dialogData} type={dialogType} />

      <CreateFilesetDialog
        open={openFilesetDialog}
        setOpen={setOpenFilesetDialog}
        data={dialogData}
        type={dialogType}
      />

      <CreateTopicDialog open={openTopicDialog} setOpen={setOpenTopicDialog} data={dialogData} type={dialogType} />

      <CreateTableDialog open={openTableDialog} setOpen={setOpenTableDialog} data={dialogData} type={dialogType} />
    </Box>
  )
}

export default TableView
