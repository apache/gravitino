/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import { useState, forwardRef, useEffect } from 'react'

import Box from '@mui/material/Box'
import Button from '@mui/material/Button'
import Dialog from '@mui/material/Dialog'
import Typography from '@mui/material/Typography'
import DialogContent from '@mui/material/DialogContent'
import DialogActions from '@mui/material/DialogActions'
import IconButton from '@mui/material/IconButton'
import Fade from '@mui/material/Fade'
import Grid from '@mui/material/Grid'
import TextField from '@mui/material/TextField'
import FormControl from '@mui/material/FormControl'
import FormHelperText from '@mui/material/FormHelperText'

import Icon from 'src/@core/components/icon'

import * as yup from 'yup'
import { useForm, Controller } from 'react-hook-form'
import { yupResolver } from '@hookform/resolvers/yup'

import { genUpdateMetalakeUpdates } from 'src/@core/utils/func'

const defaultValues = {
  name: '',
  comment: ''
}

const schema = yup.object().shape({
  name: yup.string().required()
})

const Transition = forwardRef(function Transition(props, ref) {
  return <Fade ref={ref} {...props} />
})

const CreateMetalakeDialog = props => {
  const { open, setOpen, type = 'create', data = {}, store, dispatch, createMetalake, updateMetalake } = props

  const typeText = type === 'create' ? 'Create' : 'Update'

  const [innerProps, setInnerProps] = useState([])

  const [cacheData, setCacheData] = useState()

  const {
    control,
    reset,
    setValue,
    handleSubmit,
    formState: { errors }
  } = useForm({
    defaultValues,
    mode: 'onChange',
    resolver: yupResolver(schema)
  })

  const handleFormChange = (index, event) => {
    let data = [...innerProps]
    data[index][event.target.name] = event.target.value
    setInnerProps(data)
  }

  const addFields = () => {
    let newField = { key: '', value: '' }

    setInnerProps([...innerProps, newField])
  }

  const removeFields = index => {
    let data = [...innerProps]
    data.splice(index, 1)
    setInnerProps(data)
  }

  const handleClose = () => {
    reset()
    setInnerProps([])
    setOpen(false)
  }

  const onSubmit = data => {
    const properties = innerProps.reduce((acc, item) => {
      acc[item.key] = item.value

      return acc
    }, {})

    const metalakeData = {
      ...data,
      properties
    }

    if (type === 'create') {
      dispatch(createMetalake({ ...metalakeData }))
    } else {
      const reqData = { updates: genUpdateMetalakeUpdates(cacheData, metalakeData) }

      if (reqData.updates.length !== 0) {
        dispatch(updateMetalake({ name: cacheData.name, data: reqData }))
      }
    }

    handleClose()
    reset()
  }

  useEffect(() => {
    if (open && JSON.stringify(data) !== '{}') {
      setCacheData(data)
      const { properties } = data

      const propsArr = Object.keys(properties).map(item => {
        return {
          key: item,
          value: properties[item]
        }
      })

      setInnerProps(propsArr)

      setValue('name', data.name)
      setValue('comment', data.comment)
    }
  }, [open, data, setValue])

  return (
    <Dialog fullWidth maxWidth='sm' scroll='body' TransitionComponent={Transition} open={open} onClose={handleClose}>
      <form onSubmit={handleSubmit(onSubmit)}>
        <DialogContent
          sx={{
            position: 'relative',
            pb: theme => `${theme.spacing(8)} !important`,
            px: theme => [`${theme.spacing(5)} !important`, `${theme.spacing(15)} !important`],
            pt: theme => [`${theme.spacing(8)} !important`, `${theme.spacing(12.5)} !important`]
          }}
        >
          <IconButton
            size='small'
            onClick={() => setOpen(false)}
            sx={{ position: 'absolute', right: '1rem', top: '1rem' }}
          >
            <Icon icon='bx:x' />
          </IconButton>
          <Box sx={{ mb: 8, textAlign: 'center' }}>
            <Typography variant='h5' sx={{ mb: 3 }}>
              {typeText} Metalake
            </Typography>
          </Box>

          <Grid container spacing={6}>
            <Grid item xs={12}>
              <FormControl fullWidth>
                <Controller
                  name='name'
                  control={control}
                  rules={{ required: true }}
                  render={({ field: { value, onChange } }) => (
                    <TextField
                      value={value}
                      label='Name'
                      onChange={onChange}
                      placeholder=''
                      error={Boolean(errors.name)}
                    />
                  )}
                />
                {errors.name && <FormHelperText sx={{ color: 'error.main' }}>{errors.name.message}</FormHelperText>}
              </FormControl>
            </Grid>

            <Grid item xs={12}>
              <FormControl fullWidth>
                <Controller
                  name='comment'
                  control={control}
                  rules={{ required: false }}
                  render={({ field: { value, onChange } }) => (
                    <TextField
                      value={value}
                      label='Comment'
                      multiline
                      rows={2}
                      onChange={onChange}
                      placeholder=''
                      error={Boolean(errors.comment)}
                    />
                  )}
                />
              </FormControl>
            </Grid>

            <Grid item xs={12}>
              <Typography sx={{ mb: 2 }} variant='body2'>
                Properties
              </Typography>
              {innerProps.map((item, index) => {
                return (
                  <Grid item xs={12} key={index} sx={{ '& + &': { mt: 2 } }}>
                    <FormControl fullWidth>
                      <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                        <TextField
                          size='small'
                          name='key'
                          label='Key'
                          value={item.key}
                          onChange={event => handleFormChange(index, event)}
                        />
                        <TextField
                          size='small'
                          name='value'
                          label='Value'
                          value={item.value}
                          onChange={event => handleFormChange(index, event)}
                        />
                        <Box sx={{ minWidth: 40 }}>
                          <IconButton onClick={() => removeFields(index)}>
                            <Icon icon='mdi:minus-circle-outline' />
                          </IconButton>
                        </Box>
                      </Box>
                    </FormControl>
                  </Grid>
                )
              })}
            </Grid>

            <Grid item xs={12}>
              <Button
                size='small'
                onClick={addFields}
                variant='outlined'
                startIcon={<Icon icon='mdi:plus-circle-outline' />}
              >
                Add Property
              </Button>
            </Grid>
          </Grid>
        </DialogContent>
        <DialogActions
          sx={{
            justifyContent: 'center',
            px: theme => [`${theme.spacing(5)} !important`, `${theme.spacing(15)} !important`],
            pb: theme => [`${theme.spacing(5)} !important`, `${theme.spacing(12.5)} !important`]
          }}
        >
          <Button variant='contained' sx={{ mr: 1 }} type='submit'>
            {typeText}
          </Button>
          <Button variant='outlined' onClick={handleClose}>
            Cancel
          </Button>
        </DialogActions>
      </form>
    </Dialog>
  )
}

export default CreateMetalakeDialog
