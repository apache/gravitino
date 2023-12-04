/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import { createSlice, createAsyncThunk } from '@reduxjs/toolkit'

import { createMetalakeApi, getMetalakesApi, deleteMetalakeApi, updateMetalakeApi } from '@/lib/api/metalakes'

export const fetchMetalakes = createAsyncThunk('appMetalakes/fetchMetalakes', async (params, { getState }) => {
  try {
    const response = await getMetalakesApi()

    const { metalakes } = response.data

    return {
      metalakes
    }
  } catch (error) {
    throw new Error(error)
  }
})

export const createMetalake = createAsyncThunk('appMetalakes/createMetalake', async (data, { getState, dispatch }) => {
  try {
    const response = await createMetalakeApi(data)

    dispatch(fetchMetalakes())

    return response.metalake
  } catch (error) {
    throw new Error(error)
  }
})

export const deleteMetalake = createAsyncThunk('appMetalakes/deleteMetalake', async (name, { dispatch }) => {
  try {
    const response = await deleteMetalakeApi(name)

    dispatch(fetchMetalakes())

    return response.data
  } catch (error) {
    throw new Error(error)
  }
})

export const updateMetalake = createAsyncThunk('appMetalakes/updateMetalake', async ({ name, data }, { dispatch }) => {
  try {
    const response = await updateMetalakeApi({ name, data })

    dispatch(fetchMetalakes())

    return response.data
  } catch (error) {
    throw new Error(error)
  }
})

export const appMetalakesSlice = createSlice({
  name: 'appMetalakes',
  initialState: {
    metalakes: [],
    filteredMetalakes: []
  },
  reducers: {
    setFilteredMetalakes(state, action) {
      state.filteredMetalakes = action.payload
    }
  },
  extraReducers: builder => {
    builder.addCase(fetchMetalakes.fulfilled, (state, action) => {
      state.metalakes = action.payload.metalakes
    })
  }
})

export const { setFilteredMetalakes } = appMetalakesSlice.actions

export default appMetalakesSlice.reducer
