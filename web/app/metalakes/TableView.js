/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import { useState } from 'react'

import Link from 'next/link'

import { Box, Typography } from '@mui/material'
import { DataGrid } from '@mui/x-data-grid'
import { useAppSelector } from '@/lib/hooks/useStore'

const columns = [
  {
    flex: 0.1,
    minWidth: 60,
    field: 'id',
    headerName: 'Name',
    renderCell: ({ row }) => {
      const { name, path } = row

      return (
        <Box sx={{ display: 'flex', alignItems: 'center' }}>
          <Typography
            noWrap
            component={Link}
            href={path}
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
  }
]

const TableView = props => {
  const defaultPaginationConfig = { pageSize: 10, page: 0 }
  const pageSizeOptions = [10, 25, 50]

  const [paginationModel, setPaginationModel] = useState(defaultPaginationConfig)
  const store = useAppSelector(state => state.metalakes)

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
      columns={columns}
      disableRowSelectionOnClick
      pageSizeOptions={pageSizeOptions}
      paginationModel={paginationModel}
      onPaginationModelChange={setPaginationModel}
    />
  )
}

export default TableView
