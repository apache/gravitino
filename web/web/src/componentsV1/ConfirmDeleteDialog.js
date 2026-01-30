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

import { Box, Button, Typography, Dialog, DialogContent, DialogActions } from '@mui/material'

import Icon from '@/components/Icon'

const ConfirmDeleteDialog = props => {
  const { open, setOpen, confirmCacheData, handleConfirmDeleteSubmit } = props

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
            Confirm Drop?
          </Typography>
          {confirmCacheData?.type === 'catalog' && confirmCacheData?.inUse ? (
            <Typography>Make sure the {confirmCacheData.type} is not in-use.</Typography>
          ) : (
            <Typography>
              {confirmCacheData?.type === 'metalake' && (
                <span>
                  Make sure the {confirmCacheData.type} is not in-use, and all sub-entities in it are dropped.{' '}
                </span>
              )}
              This action can not be reversed!
            </Typography>
          )}
        </Box>
      </DialogContent>
      <DialogActions className={'twc-justify-center twc-px-5 twc-pb-8'}>
        {!(confirmCacheData?.type === 'catalog' && confirmCacheData?.inUse) && (
          <Button
            variant='contained'
            data-refer='confirm-delete'
            className={'twc-mr-2'}
            onClick={() => handleConfirmDeleteSubmit()}
          >
            Drop
          </Button>
        )}
        <Button variant='outlined' color='secondary' onClick={() => handleClose()}>
          Cancel
        </Button>
      </DialogActions>
    </Dialog>
  )
}

export default ConfirmDeleteDialog
