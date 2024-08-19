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

import { useEffect, useState } from 'react'

import {
  Box,
  Grid,
  Drawer,
  IconButton,
  Typography,
  Table,
  TableHead,
  TableBody,
  TableRow,
  TableCell,
  TableContainer,
  Tooltip
} from '@mui/material'

import Icon from '@/components/Icon'

import EmptyText from '@/components/EmptyText'

import { formatToDateTime, isValidDate } from '@/lib/utils/date'

const DetailsDrawer = props => {
  const { openDrawer, setOpenDrawer, drawerData = {}, isMetalakePage } = props

  const { audit = {} } = drawerData

  const [properties, setProperties] = useState([])

  const handleClose = () => {
    setOpenDrawer(false)
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

  useEffect(() => {
    if (JSON.stringify(drawerData) !== '{}') {
      if (drawerData.properties) {
        const propsData = Object.keys(drawerData.properties).map(item => {
          return {
            key: item,
            value: JSON.stringify(drawerData.properties[item]).replace(/^"|"$/g, '')
          }
        })

        setProperties(propsData)
      } else {
        setProperties([])
      }
    }
  }, [drawerData])

  return (
    <Drawer
      data-refer='details-drawer'
      open={openDrawer}
      anchor='right'
      variant='temporary'
      onClose={handleClose}
      ModalProps={{ keepMounted: true }}
      PaperProps={{
        sx: {
          width: {
            xs: 300,
            sm: 400
          }
        }
      }}
    >
      <Box
        className={'drawer-header twc-flex twc-items-center twc-justify-between'}
        sx={{
          p: theme => theme.spacing(3, 4),
          borderBottom: theme => `1px solid ${theme.palette.divider}`,
          backgroundColor: theme => theme.palette.background.default
        }}
      >
        <Typography variant='h6'>Details</Typography>
        <IconButton size='small' data-refer='close-details-btn' onClick={handleClose} sx={{ color: 'text.primary' }}>
          <Icon icon='bx:x' fontSize={20} />
        </IconButton>
      </Box>
      <Box sx={{ p: 4 }}>
        <Grid item xs={12} sx={{ mb: [0, 5] }}>
          <Typography
            variant='subtitle1'
            className={'twc-py-2 twc-font-semibold twc-text-[1.2rem] twc-w-full twc-overflow-hidden twc-text-ellipsis'}
            sx={{
              borderBottom: theme => `1px solid ${theme.palette.divider}`,
              whiteSpace: 'nowrap'
            }}
            data-refer='details-title'
          >
            {drawerData.name}
          </Typography>
        </Grid>

        {isMetalakePage ? (
          <>
            <Grid item xs={12} md={6} sx={{ mb: [0, 5] }}>
              <Typography variant='body2' sx={{ mb: 2 }}>
                Type
              </Typography>
              {renderFieldText({ value: drawerData.type })}
            </Grid>
            <Grid item xs={12} md={6} sx={{ mb: [0, 5] }}>
              <Typography variant='body2' sx={{ mb: 2 }}>
                Provider
              </Typography>
              {renderFieldText({ value: drawerData.provider })}
            </Grid>
          </>
        ) : null}

        <Grid item xs={12} sx={{ mb: [0, 5] }}>
          <Typography variant='body2' sx={{ mb: 2 }}>
            Comment
          </Typography>
          {renderFieldText({ value: drawerData.comment, linkBreak: true })}
        </Grid>

        <Grid item xs={12} sx={{ mb: [0, 5] }}>
          <Typography variant='body2' sx={{ mb: 2 }}>
            Created by
          </Typography>
          {renderFieldText({ value: audit.creator })}
        </Grid>

        <Grid item xs={12} sx={{ mb: [0, 5] }}>
          <Typography variant='body2' sx={{ mb: 2 }}>
            Created at
          </Typography>
          {renderFieldText({ value: audit.createTime, isDate: true })}
        </Grid>

        <Grid item xs={12} sx={{ mb: [0, 5] }}>
          <Typography variant='body2' sx={{ mb: 2 }}>
            Last modified by
          </Typography>
          {renderFieldText({ value: audit.lastModifier })}
        </Grid>

        <Grid item xs={12} sx={{ mb: [0, 5] }}>
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
              <TableBody data-refer='details-props-table'>
                {properties.map((item, index) => {
                  return (
                    <TableRow key={index} data-refer={`details-props-index-${index}`}>
                      <TableCell className={'twc-py-[0.7rem]'} data-refer={`details-props-key-${item.key}`}>
                        <Tooltip title={item.key} placement='bottom'>
                          {item.key.length > 22 ? `${item.key.substring(0, 22)}...` : item.key}
                        </Tooltip>
                      </TableCell>
                      <TableCell
                        className={'twc-py-[0.7rem]'}
                        data-refer={`details-props-value-${item.value}`}
                        data-prev-refer={`details-props-key-${item.key}`}
                      >
                        <Tooltip title={item.value} placement='bottom'>
                          {item.value.length > 22 ? `${item.value.substring(0, 22)}...` : item.value}
                        </Tooltip>
                      </TableCell>
                    </TableRow>
                  )
                })}
              </TableBody>
            </Table>
          </TableContainer>
        </Grid>
      </Box>
    </Drawer>
  )
}

export default DetailsDrawer
