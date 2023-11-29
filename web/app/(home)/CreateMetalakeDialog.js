/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import { useState, forwardRef, useEffect } from 'react'

import {
  Box,
  Button,
  Dialog,
  Typography,
  DialogContent,
  DialogActions,
  IconButton,
  Fade,
  Grid,
  TextField,
  FormControl,
  FormHelperText
} from '@mui/material'

import Icon from '@/components/Icon'

import * as yup from 'yup'
import { useForm, Controller } from 'react-hook-form'
import { yupResolver } from '@hookform/resolvers/yup'

import { genUpdateMetalakeUpdates } from '@/lib/utils'

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
  const { open, setOpen, data = {}, store, dispatch, createMetalake, updateMetalake, type = 'create' } = props

  // const [type, setType] = useState('create')
  const [innerProps, setInnerProps] = useState([])
  const [cacheData, setCacheData] = useState()

  const typeText = type === 'create' ? 'Create' : 'Update'

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
    reset({ name: '', comment: '' })
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
          className={'twc-relative twc-pb-8'}
          sx={{
            px: theme => [`${theme.spacing(5)} !important`, `${theme.spacing(15)} !important`],
            pt: theme => [`${theme.spacing(8)} !important`, `${theme.spacing(12.5)} !important`]
          }}
        >
          <IconButton
            className={'twc-absolute twc-right-[1rem] twc-top-[1rem]'}
            size='small'
            onClick={() => setOpen(false)}
          >
            <Icon icon='bx:x' />
          </IconButton>
          <Box className={'twc-mb-8 twc-text-center'}>
            <Typography className={'twc-mb-3'} variant='h5'>
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
                {errors.name && (
                  <FormHelperText className={'twc-text-error-main'}>{errors.name.message}</FormHelperText>
                )}
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
              <Typography className={'twc-mb-2'} variant='body2'>
                Properties
              </Typography>
              {innerProps.map((item, index) => {
                return (
                  <Grid item xs={12} key={index} className={'[&+&]:twc-mt-2'}>
                    <FormControl fullWidth>
                      <Box className={'twc-flex twc-items-center twc-justify-between'}>
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
                        <Box className={'twc-min-w-[40px]'}>
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
        <DialogActions className={' twc-px-5 md:twc-px-15 twc-pb-5 md:twc-pb-[12.5 0.25 rem]'}>
          <Button variant='contained' type='submit'>
            {typeText}
          </Button>
          <Button variant='outlined' className={'twc-ml-1'} onClick={handleClose} type='reset'>
            Cancel
          </Button>
        </DialogActions>
      </form>
    </Dialog>
  )
}

export default CreateMetalakeDialog
