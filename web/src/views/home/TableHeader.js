import { useState } from 'react'

import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import TextField from '@mui/material/TextField'

import Icon from 'src/@core/components/icon'

import CreateMetalakeDialog from './CreateMetalakeDialog'

const TableHeader = props => {
  const { handleFilter, value, dispatch, createMetalake } = props

  const [openDialog, setOpenDialog] = useState(false)

  const handleCreate = () => {
    setOpenDialog(true)
  }

  return (
    <Box
      sx={{
        px: 5,
        pb: 4,
        pt: 4,
        display: 'flex',
        flexWrap: 'wrap',
        alignItems: 'center',
        justifyContent: 'flex-end'
      }}
    >
      <Box sx={{ display: 'flex', alignItems: 'center' }}>
        <TextField
          size='small'
          value={value}
          placeholder='Filter'
          sx={{ mb: 2 }}
          onChange={e => handleFilter(e.target.value)}
        />
      </Box>
      <Button
        variant='contained'
        sx={{ ml: 2, mb: 2 }}
        startIcon={<Icon icon='bx:bxs-plus-square' fontSize={20} />}
        onClick={handleCreate}
      >
        create metalake
      </Button>
      <CreateMetalakeDialog
        open={openDialog}
        setOpen={setOpenDialog}
        dispatch={dispatch}
        createMetalake={createMetalake}
      />
    </Box>
  )
}

export default TableHeader
