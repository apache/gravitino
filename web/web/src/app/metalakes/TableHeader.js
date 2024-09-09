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
        data-refer='query-metalake'
        size='small'
        value={value}
        placeholder='Query Name'
        onChange={e => handleFilter(e.target.value)}
      />
      <Button
        data-refer='create-metalake-btn'
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
