/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import { useState } from 'react'

import Link from 'next/link'

import { Box, Typography } from '@mui/material'
import ColumnTypeChip from '@/components/ColumnTypeChip'
import { DataGrid } from '@mui/x-data-grid'
import { useAppSelector, useAppDispatch } from '@/lib/hooks/useStore'
import { resetTableData } from '@/lib/store/metalakes'

const TableView = props => {
  const { page } = props
  const defaultPaginationConfig = { pageSize: 10, page: 0 }
  const pageSizeOptions = [10, 25, 50]

  const dispatch = useAppDispatch()

  const [paginationModel, setPaginationModel] = useState(defaultPaginationConfig)
  const store = useAppSelector(state => state.metalakes)

  const columns = [
    {
      flex: 0.1,
      minWidth: 60,
      field: 'id',
      headerName: 'Name',
      renderCell: ({ row }) => {
        const { name, path } = row

        const handleClickUrl = () => {
          dispatch(resetTableData())
        }

        return (
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            <Typography
              noWrap
              component={Link}
              href={path ?? '/'}
              onClick={() => handleClickUrl()}
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

  const tableColumns = [
    {
      flex: 0.1,
      minWidth: 60,
      field: 'name',
      headerName: 'Name',
      renderCell: ({ row }) => {
        const { name } = row

        return (
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            <Typography
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
              {`${nullable}`}
            </Typography>
          </Box>
        )
      }
    }
  ]

  return (
    <DataGrid
      sx={{
        '& .MuiDataGrid-columnHeaders': {
          borderTopLeftRadius: 0,
          borderTopRightRadius: 0,
          borderTop: 0
        }
      }}
      autoHeight
      rows={store.tableData}
      getRowId={row => row?.name}
      columns={page === 'tables' ? tableColumns : columns}
      disableRowSelectionOnClick
      pageSizeOptions={pageSizeOptions}
      paginationModel={paginationModel}
      onPaginationModelChange={setPaginationModel}
    />
  )
}

export default TableView
