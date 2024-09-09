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

  let properties = Object.keys(activatedItem?.properties || [])
    .filter(key => !['partition-count', 'replication-factor'].includes(key))
    .map(item => {
      return {
        key: item,
        value: JSON.stringify(activatedItem?.properties[item]).replace(/^"|"$/g, '')
      }
    })
  if (paramsSize === 5 && searchParams.get('topic')) {
    const topicPros = Object.keys(activatedItem?.properties || [])
      .filter(key => ['partition-count', 'replication-factor'].includes(key))
      .map(item => {
        return {
          key: item,
          value: JSON.stringify(activatedItem?.properties[item]).replace(/^"|"$/g, '')
        }
      })
    properties = [...topicPros, ...properties]
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
        {paramsSize === 5 && searchParams.get('fileset') ? (
          <>
            <Grid item xs={12} sx={{ mb: [0, 5] }}>
              <Typography variant='body2' sx={{ mb: 2 }}>
                Type
              </Typography>
              {renderFieldText({ value: activatedItem?.type })}
            </Grid>
            <Grid item xs={12} sx={{ mb: [0, 5] }}>
              <Typography variant='body2' sx={{ mb: 2 }}>
                Storage location
              </Typography>
              {renderFieldText({ value: activatedItem?.storageLocation })}
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

          <TableContainer data-refer='details-table-grid'>
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
                          <div
                            data-refer={
                              searchParams.get('topic') && ['partition-count', 'replication-factor'].includes(item.key)
                                ? `props-key-${item.key}-highlight`
                                : `props-key-${item.key}`
                            }
                          >
                            {item.key}
                          </div>
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
                          <div
                            data-refer={
                              searchParams.get('topic') && ['partition-count', 'replication-factor'].includes(item.key)
                                ? `props-value-${item.key}-highlight`
                                : `props-value-${item.key}`
                            }
                          >
                            {item.key === 'jdbc-password' ? '[HIDDEN]' : item.value}
                          </div>
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
