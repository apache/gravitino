/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

'use client'

import { Box, Grid, Typography, Table, TableHead, TableBody, TableRow, TableCell, TableContainer } from '@mui/material'

import EmptyText from '@/components/EmptyText'

import { formatToDateTime, isValidDate } from '@/lib/utils/date'
import { useAppSelector } from '@/lib/hooks/useStore'
import { useSearchParams } from 'next/navigation'

const DetailsView = () => {
  const searchParams = useSearchParams()
  const paramsSize = [...searchParams.keys()].length

  const store = useAppSelector(state => state.metalakes)

  const activatedItem = store.activatedDetails

  const audit = activatedItem?.audit || {}

  const properties = Object.keys(activatedItem?.properties || [])
    .filter(key => !['partition-count', 'replication-factor'].includes(key))
    .map(item => {
      return {
        key: item,
        value: JSON.stringify(activatedItem?.properties[item]).replace(/^"|"$/g, '')
      }
    })
  if (paramsSize === 5 && searchParams.get('topic')) {
    properties.unshift({
      key: 'replication-factor',
      value: JSON.stringify(activatedItem?.properties['replication-factor'])?.replace(/^"|"$/g, '')
    })
    properties.unshift({
      key: 'partition-count',
      value: JSON.stringify(activatedItem?.properties['partition-count'])?.replace(/^"|"$/g, '')
    })
  }

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
        {paramsSize == 3 && searchParams.get('catalog') && searchParams.get('type') ? (
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
                      <TableCell sx={{ py: theme => `${theme.spacing(2.75)} !important` }}>
                        <Typography
                          sx={{
                            fontWeight:
                              searchParams.get('topic') && ['partition-count', 'replication-factor'].includes(item.key)
                                ? 500
                                : 400
                          }}
                        >
                          {item.key}
                        </Typography>
                      </TableCell>
                      <TableCell sx={{ py: theme => `${theme.spacing(2.75)} !important` }}>
                        <Typography
                          sx={{
                            fontWeight:
                              searchParams.get('topic') && ['partition-count', 'replication-factor'].includes(item.key)
                                ? 500
                                : 400
                          }}
                        >
                          {item.value}
                        </Typography>
                      </TableCell>
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
