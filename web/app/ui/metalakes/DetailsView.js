/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import { Box, Grid, Typography, Table, TableHead, TableBody, TableRow, TableCell, TableContainer } from '@mui/material'

import EmptyText from '@/components/EmptyText'

import { formatToDateTime, isValidDate } from '@/lib/utils/date'

const DetailsView = props => {
  const { store, data = {}, page } = props

  const activatedItem = store.activatedDetails

  const audit = activatedItem?.audit || {}

  const properties = Object.keys(activatedItem?.properties || []).map(item => {
    return {
      key: item,
      value: JSON.stringify(activatedItem?.properties[item]).replace(/^"|"$/g, '')
    }
  })

  const renderFieldText = ({ value, linkBreak = false, isDate = false }) => {
    if (!value) {
      return <EmptyText />
    }

    return (
      <Typography sx={{ fontWeight: 500, wordBreak: 'break-all', whiteSpace: linkBreak ? 'pre-wrap' : 'normal' }}>
        {isDate && isValidDate(value) ? formatToDateTime(value) : value}
      </Typography>
    )
  }

  return (
    <Box sx={{ p: 4, height: '100%', overflow: 'auto' }}>
      <Grid container spacing={6}>
        {page && page === 'catalogs' ? (
          <>
            <Grid item xs={12} md={6} sx={{ mb: [0, 5] }}>
              <Typography variant='body2' sx={{ mb: 2 }}>
                Type
              </Typography>
              {renderFieldText({ value: activatedItem?.type })}
            </Grid>
            <Grid item xs={12} md={6} sx={{ mb: [0, 5] }}>
              <Typography variant='body2' sx={{ mb: 2 }}>
                Provider
              </Typography>
              {renderFieldText({ value: activatedItem?.provider })}
            </Grid>
          </>
        ) : null}
        <Grid item xs={12} sx={{ mb: [0, 5] }}>
          <Typography variant='body2' sx={{ mb: 2 }}>
            Comment
          </Typography>
          {renderFieldText({ value: activatedItem?.comment, linkBreak: true })}
        </Grid>

        <Grid item xs={12} md={6} sx={{ mb: [0, 5] }}>
          <Typography variant='body2' sx={{ mb: 2 }}>
            Created by
          </Typography>
          {renderFieldText({ value: audit.creator })}
        </Grid>

        <Grid item xs={12} md={6} sx={{ mb: [0, 5] }}>
          <Typography variant='body2' sx={{ mb: 2 }}>
            Created at
          </Typography>
          {renderFieldText({ value: audit.createTime, isDate: true })}
        </Grid>

        <Grid item xs={12} md={6} sx={{ mb: [0, 5] }}>
          <Typography variant='body2' sx={{ mb: 2 }}>
            Last modified by
          </Typography>
          {renderFieldText({ value: audit.lastModifier })}
        </Grid>

        <Grid item xs={12} md={6} sx={{ mb: [0, 5] }}>
          <Typography variant='body2' sx={{ mb: 2 }}>
            Last modified at
          </Typography>
          {renderFieldText({ value: audit.lastModifiedTime, isDate: true })}
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
