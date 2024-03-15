/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import { Box, Button, TextField } from '@mui/material'

import Icon from '@/components/Icon'

const TableHeader = props => {
  const { handleFilter, value, setOpenDialog, setDialogData, setDialogType } = props

  const handleCreate = () => {
    setDialogData({})
    setOpenDialog(true)
    setDialogType('create')
  }

  return (
    <Box className={'twc-pr-5 twc-pb-4 twc-pt-4 twc-flex twc-flex-wrap twc-items-center twc-justify-end'}>
      <Box className={'twc-flex twc-items-center twc-flex-1 twc-h-full'} id='filter-panel' />
      <TextField
        id='query-metalake'
        size='small'
        value={value}
        placeholder='Query Name'
        onChange={e => handleFilter(e.target.value)}
      />
      <Button
        id='createMetalakeBtn'
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
