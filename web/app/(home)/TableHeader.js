/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import { Box, Button, TextField } from '@mui/material'

import Icon from '@/components/Icon'

const TableHeader = props => {
  const { handleFilter, value, setOpenDialog, setDialogData } = props

  const handleCreate = () => {
    setDialogData({})
    setOpenDialog(true)
  }

  return (
    <Box className={'twc-px-5 twc-pb-4 twc-pt-4 twc-flex twc-flex-wrap twc-items-center twc-justify-end'}>
      <Box className={'twc-flex twc-items-center twc-h-full'}>
        <TextField size='small' value={value} placeholder='Filter' onChange={e => handleFilter(e.target.value)} />
      </Box>
      <Button
        className={'twc-ml-2'}
        variant='contained'
        color='primary'
        startIcon={<Icon icon='bx:bxs-plus-square' fontSize={20} />}
        onClick={handleCreate}
      >
        create metalake
      </Button>
    </Box>
  )
}

export default TableHeader
