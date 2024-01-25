/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
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
  TableContainer
} from '@mui/material'

import Icon from '@/components/Icon'

import EmptyText from '@/components/EmptyText'

import { formatToDateTime, isValidDate } from '@/lib/utils/date'

const DetailsDrawer = props => {
  const { openDrawer, setOpenDrawer, drawerData = {} } = props

  const { audit = {} } = drawerData

  const [properties, setProperties] = useState([])

  const handleClose = () => {
    setOpenDrawer(false)
  }

  const renderFieldText = (value, linkBreak = false) => {
    if (!value) {
      return <EmptyText />
    }

    return (
      <Typography sx={{ fontWeight: 500, whiteSpace: linkBreak ? 'pre-wrap' : 'normal' }}>
        {isValidDate(value) ? formatToDateTime(value) : value}
      </Typography>
    )
  }

  useEffect(() => {
    if (drawerData.properties) {
      const propsData = Object.keys(drawerData.properties).map(item => {
        return {
          key: item,
          value: drawerData.properties[item]
        }
      })

      setProperties(propsData)
    }
  }, [drawerData])

  return (
    <Drawer
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
        <IconButton size='small' onClick={handleClose} sx={{ color: 'text.primary' }}>
          <Icon icon='bx:x' fontSize={20} />
        </IconButton>
      </Box>
      <Box sx={{ p: 4 }}>
        <Grid item xs={12} sx={{ mb: [0, 5] }}>
          <Typography
            variant='subtitle1'
            className={'twc-py-2 twc-font-semibold twc-text-[1.2rem]'}
            sx={{
              borderBottom: theme => `1px solid ${theme.palette.divider}`
            }}
          >
            {drawerData.name}
          </Typography>
        </Grid>

        <Grid item xs={12} sx={{ mb: [0, 5] }}>
          <Typography variant='body2' sx={{ mb: 2 }}>
            Comment
          </Typography>
          {renderFieldText(drawerData.comment, true)}
        </Grid>

        <Grid item xs={12} sx={{ mb: [0, 5] }}>
          <Typography variant='body2' sx={{ mb: 2 }}>
            Created by
          </Typography>
          {renderFieldText(audit.creator)}
        </Grid>

        <Grid item xs={12} sx={{ mb: [0, 5] }}>
          <Typography variant='body2' sx={{ mb: 2 }}>
            Created at
          </Typography>
          {renderFieldText(audit.createTime)}
        </Grid>

        <Grid item xs={12} sx={{ mb: [0, 5] }}>
          <Typography variant='body2' sx={{ mb: 2 }}>
            Last modified by
          </Typography>
          {renderFieldText(audit.lastModifier)}
        </Grid>

        <Grid item xs={12} sx={{ mb: [0, 5] }}>
          <Typography variant='body2' sx={{ mb: 2 }}>
            Last modified at
          </Typography>
          {renderFieldText(audit.lastModifiedTime)}
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
                      <TableCell className={'twc-py-[0.7rem]'}>{item.key}</TableCell>
                      <TableCell className={'twc-py-[0.7rem]'}>{item.value}</TableCell>
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
