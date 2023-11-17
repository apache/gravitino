import { useEffect, useCallback, useState } from 'react'

import Link from 'next/link'

import Box from '@mui/material/Box'
import Card from '@mui/material/Card'
import Grid from '@mui/material/Grid'
import IconButton from '@mui/material/IconButton'
import Typography from '@mui/material/Typography'
import { DataGrid } from '@mui/x-data-grid'
import Icon from 'src/@core/components/icon'
import TableHeader from './TableHeader'

import { formatToDateTime } from 'src/@core/utils/dateUtil'
import { useDispatch, useSelector } from 'react-redux'
import { fetchMetalakes, createMetalake } from 'src/store/metalake'

import DetailsDrawer from './DetailsDrawer'

const MetalakeList = () => {
  const [value, setValue] = useState('')
  const [paginationModel, setPaginationModel] = useState({ page: 0, pageSize: 10 })
  const [openDrawer, setOpenDrawer] = useState(false)

  const [drawerData, setDrawerData] = useState()

  const dispatch = useDispatch()
  const store = useSelector(state => state.metalake)

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
              href={'/metalake'}
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
            {row.createdBy}
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
            {formatToDateTime(row.createdAt)}
          </Typography>
        )
      }
    },
    {
      flex: 0.1,
      minWidth: 100,
      sortable: false,
      field: 'actions',
      headerName: 'Actions',
      renderCell: ({ row }) => (
        <IconButton onClick={() => handleShowDetails(row)}>
          <Icon icon='bx:show-alt' />
        </IconButton>
      )
    }
  ]

  useEffect(() => {
    dispatch(
      fetchMetalakes({
        q: value
      })
    )
  }, [dispatch, value])

  const handleFilter = useCallback(val => {
    setValue(val)
  }, [])

  const handleShowDetails = row => {
    setDrawerData(row)
    setOpenDrawer(true)
  }

  return (
    <Grid container spacing={6}>
      <Grid item xs={12}>
        <Card>
          <TableHeader value={value} dispatch={dispatch} handleFilter={handleFilter} createMetalake={createMetalake} />
          <DataGrid
            autoHeight
            rows={store.data}
            columns={columns}
            disableRowSelectionOnClick
            pageSizeOptions={[10, 25, 50]}
            paginationModel={paginationModel}
            onPaginationModelChange={setPaginationModel}
          />
          <DetailsDrawer openDrawer={openDrawer} setOpenDrawer={setOpenDrawer} drawerData={drawerData} />
        </Card>
      </Grid>
    </Grid>
  )
}

export default MetalakeList
