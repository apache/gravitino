/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import Drawer from '@mui/material/Drawer'
import { styled } from '@mui/material/styles'
import IconButton from '@mui/material/IconButton'
import Typography from '@mui/material/Typography'
import Box from '@mui/material/Box'
import Grid from '@mui/material/Grid'
import Table from '@mui/material/Table'
import TableHead from '@mui/material/TableHead'
import TableBody from '@mui/material/TableBody'
import TableRow from '@mui/material/TableRow'
import TableCell from '@mui/material/TableCell'
import TableContainer from '@mui/material/TableContainer'

import Icon from 'src/@core/components/icon'

import { formatToDateTime } from 'src/@core/utils/dateUtil'

const Header = styled(Box)(({ theme }) => ({
  display: 'flex',
  alignItems: 'center',
  padding: theme.spacing(3, 4),
  justifyContent: 'space-between',
  borderBottom: `1px solid ${theme.palette.divider}`,
  backgroundColor: theme.palette.background.default
}))

const DetailsDrawer = props => {
  const { openDrawer, setOpenDrawer, drawerData = {} } = props

  const handleClose = () => {
    setOpenDrawer(false)
  }

  return (
    <Drawer
      open={openDrawer}
      anchor='right'
      variant='temporary'
      onClose={handleClose}
      ModalProps={{ keepMounted: true }}
      sx={{ '& .MuiDrawer-paper': { width: { xs: 300, sm: 400 } } }}
    >
      <Header>
        <Typography variant='h6'>Details</Typography>
        <IconButton size='small' onClick={handleClose} sx={{ color: 'text.primary' }}>
          <Icon icon='bx:x' fontSize={20} />
        </IconButton>
      </Header>
      <Box sx={{ p: 4 }}>
        <Grid item xs={12} sx={{ mb: [0, 5] }}>
          <Typography
            variant='subtitle1'
            sx={{
              py: 2,
              fontWeight: 600,
              fontSize: '1.2rem',
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
          <Typography sx={{ fontWeight: 500 }}>{drawerData.comment}</Typography>
        </Grid>

        <Grid item xs={12} sx={{ mb: [0, 5] }}>
          <Typography variant='body2' sx={{ mb: 2 }}>
            Last modified by
          </Typography>
          <Typography sx={{ fontWeight: 500 }}>{drawerData.lastModifiedBy}</Typography>
        </Grid>

        <Grid item xs={12} sx={{ mb: [0, 5] }}>
          <Typography variant='body2' sx={{ mb: 2 }}>
            Last modified at
          </Typography>
          <Typography sx={{ fontWeight: 500 }}>{formatToDateTime(drawerData.lastModifiedAt)}</Typography>
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
                {(drawerData.properties || []).map((item, index) => {
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
      </Box>
    </Drawer>
  )
}

export default DetailsDrawer
