/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Dialog from '@mui/material/Dialog'
import Typography from '@mui/material/Typography'
import DialogContent from '@mui/material/DialogContent'
import DialogActions from '@mui/material/DialogActions'

import Icon from 'src/@core/components/icon'

const ConfirmDeleteDialog = props => {
  const { open, setOpen, handleConfirmDeleteSubmit } = props

  const handleClose = () => setOpen(false)

  return (
    <Dialog fullWidth open={open} onClose={handleClose} sx={{ '& .MuiPaper-root': { width: '100%', maxWidth: 512 } }}>
      <DialogContent
        sx={{
          px: theme => [`${theme.spacing(5)} !important`, `${theme.spacing(15)} !important`],
          pt: theme => [`${theme.spacing(8)} !important`, `${theme.spacing(12.5)} !important`]
        }}
      >
        <Box
          sx={{
            display: 'flex',
            textAlign: 'center',
            alignItems: 'center',
            flexDirection: 'column',
            justifyContent: 'center',
            '& svg': { mb: 8, color: 'warning.main' }
          }}
        >
          <Icon icon='tabler:alert-circle' fontSize='5.5rem' />
          <Typography variant='h4' sx={{ mb: 5, color: 'text.secondary' }}>
            Confirm Delete ?
          </Typography>
          <Typography>This action can not reserve!</Typography>
        </Box>
      </DialogContent>
      <DialogActions
        sx={{
          justifyContent: 'center',
          px: theme => [`${theme.spacing(5)} !important`, `${theme.spacing(15)} !important`],
          pb: theme => [`${theme.spacing(8)} !important`, `${theme.spacing(12.5)} !important`]
        }}
      >
        <Button variant='contained' sx={{ mr: 2 }} onClick={() => handleConfirmDeleteSubmit()}>
          Submit
        </Button>
        <Button variant='outlined' color='secondary' onClick={() => handleClose()}>
          Cancel
        </Button>
      </DialogActions>
    </Dialog>
  )
}

export default ConfirmDeleteDialog
