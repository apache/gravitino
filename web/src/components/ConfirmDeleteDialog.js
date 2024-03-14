/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import { Box, Button, Typography, Dialog, DialogContent, DialogActions } from '@mui/material'

import Icon from '@/components/Icon'

const ConfirmDeleteDialog = props => {
  const { open, setOpen, handleConfirmDeleteSubmit } = props

  const handleClose = () => setOpen(false)

  return (
    <Dialog fullWidth open={open} onClose={handleClose} PaperProps={{ sx: { width: '100%', maxWidth: 480 } }}>
      <DialogContent className={'twc-px-[1.25rem] sm:twc-px-[3.75rem] twc-pt-[2rem] sm:twc-pt-[3.125rem]'}>
        <Box
          className={'twc-flex twc-flex-col twc-text-center twc-items-center twc-justify-center'}
          sx={{ '& svg': { mb: 8, color: 'warning.main' } }}
        >
          <Icon icon='tabler:alert-circle' fontSize='6rem' />
          <Typography variant='h4' className={'twc-mb-5 '} sx={{ color: 'text.secondary' }}>
            Confirm Delete?
          </Typography>
          <Typography>This action can not be reversed!</Typography>
        </Box>
      </DialogContent>
      <DialogActions className={'twc-justify-center twc-px-5 twc-pb-8'}>
        <Button
          variant='contained'
          data-refer='confirm-delete'
          className={'twc-mr-2'}
          onClick={() => handleConfirmDeleteSubmit()}
        >
          Delete
        </Button>
        <Button variant='outlined' color='secondary' onClick={() => handleClose()}>
          Cancel
        </Button>
      </DialogActions>
    </Dialog>
  )
}

export default ConfirmDeleteDialog
