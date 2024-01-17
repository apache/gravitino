/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import Box from '@mui/material/Box'
import Grid from '@mui/material/Grid'
import Typography from '@mui/material/Typography'
import Table from '@mui/material/Table'
import TableHead from '@mui/material/TableHead'
import TableBody from '@mui/material/TableBody'
import TableRow from '@mui/material/TableRow'
import TableCell from '@mui/material/TableCell'
import TableContainer from '@mui/material/TableContainer'

import { formatToDateTime } from '@/lib/utils/date'

const DetailsView = props => {
  const { store, data = {}, page } = props

  const activatedItem = store.activatedDetails

  const audit = activatedItem?.audit || {}

  const properties = Object.keys(activatedItem?.properties || []).map(item => {
    return {
      key: item,
      value: activatedItem?.properties[item]
    }
  })

  console.log(activatedItem)

  return (
    <Box sx={{ p: 4 }}>
      <Grid container spacing={6}>
        {page && page === 'catalogs' ? (
          <>
            <Grid item xs={12} md={6} sx={{ mb: [0, 5] }}>
              <Typography variant='body2' sx={{ mb: 2 }}>
                Type
              </Typography>
              <Typography sx={{ fontWeight: 500 }}>{activatedItem?.type || ''}</Typography>
            </Grid>
            <Grid item xs={12} md={6} sx={{ mb: [0, 5] }}>
              <Typography variant='body2' sx={{ mb: 2 }}>
                Provider
              </Typography>
              <Typography sx={{ fontWeight: 500 }}>{activatedItem?.provider || ''}</Typography>
            </Grid>
          </>
        ) : null}
        <Grid item xs={12} sx={{ mb: [0, 5] }}>
          <Typography variant='body2' sx={{ mb: 2 }}>
            Comment
          </Typography>
          <Typography sx={{ fontWeight: 500, whiteSpace: 'pre' }}>{activatedItem?.comment || ''}</Typography>
        </Grid>

        <Grid item xs={12} md={6} sx={{ mb: [0, 5] }}>
          <Typography variant='body2' sx={{ mb: 2 }}>
            Created by
          </Typography>
          <Typography sx={{ fontWeight: 500 }}>{audit?.creator ? audit.creator : ''}</Typography>
        </Grid>

        <Grid item xs={12} md={6} sx={{ mb: [0, 5] }}>
          <Typography variant='body2' sx={{ mb: 2 }}>
            Created at
          </Typography>
          <Typography sx={{ fontWeight: 500 }}>
            {audit?.createTime ? formatToDateTime(audit?.createTime) : ''}
          </Typography>
        </Grid>

        <Grid item xs={12} md={6} sx={{ mb: [0, 5] }}>
          <Typography variant='body2' sx={{ mb: 2 }}>
            Last modified by
          </Typography>
          <Typography sx={{ fontWeight: 500 }}>{audit?.lastModifier ? audit.lastModifier : ''}</Typography>
        </Grid>

        <Grid item xs={12} md={6} sx={{ mb: [0, 5] }}>
          <Typography variant='body2' sx={{ mb: 2 }}>
            Last modified at
          </Typography>
          <Typography sx={{ fontWeight: 500 }}>
            {audit?.lastModifiedTime ? formatToDateTime(audit?.lastModifiedTime) : ''}
          </Typography>
        </Grid>

        <Grid item xs={12} sx={{ mb: [0, 5] }}>
          <Typography variant='body2' sx={{ mb: 2 }}>
            Properties
          </Typography>

          <TableContainer>
            <Table>
              <TableHead
                sx={{
                  backgroundColor: theme => theme.palette.action.hover
                }}
              >
                <TableRow>
                  <TableCell sx={{ py: 2 }}>Key</TableCell>
                  <TableCell sx={{ py: 2 }}>Value</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {properties.map((item, index) => {
                  return (
                    <TableRow key={index}>
                      <TableCell sx={{ py: theme => `${theme.spacing(2.75)} !important` }}>{item.key}</TableCell>
                      <TableCell sx={{ py: theme => `${theme.spacing(2.75)} !important` }}>{item.value}</TableCell>
                    </TableRow>
                  )
                })}
              </TableBody>
            </Table>
          </TableContainer>
        </Grid>
      </Grid>
    </Box>
  )
}

export default DetailsView
