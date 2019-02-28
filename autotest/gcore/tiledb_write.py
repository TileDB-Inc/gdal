#!/usr/bin/env pytest
# -*- coding: utf-8 -*-
###############################################################################
# $Id$
#
# Project:  GDAL/OGR Test Suite
# Purpose:  Test read/write functionality for GeoTIFF format.
# Author:   TileDB, Inc
#
###############################################################################
# Copyright (c) 2019, TileDB, Inc
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Library General Public
# License as published by the Free Software Foundation; either
# version 2 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Library General Public License for more details.
#
# You should have received a copy of the GNU Library General Public
# License along with this library; if not, write to the
# Free Software Foundation, Inc., 59 Temple Place - Suite 330,
# Boston, MA 02111-1307, USA.
###############################################################################

from osgeo import gdal
import pytest

import gdaltest

@pytest.mark.require_driver('TileDB')
def test_tiledb_write_complex():
    gdaltest.tiledb_drv = gdal.GetDriverByName('TileDB')

    src_ds = gdal.Open('data/cfloat64.tif')

    new_ds = gdaltest.tiledb_drv.CreateCopy('tmp/tiledb_complex64', src_ds)

    bnd = new_ds.GetRasterBand(1)
    assert bnd.Checksum() == 5028, 'Didnt get expected checksum on still-open file'

    bnd = None
    new_ds = None

    gdaltest.tiledb_drv.Delete('tmp/tiledb_complex64')

def test_tiff_write_custom_blocksize():
    gdaltest.tiledb_drv = gdal.GetDriverByName('TileDB')

    src_ds = gdal.Open('data/utmsmall.tif')

    options = ['BLOCKXSIZE=32', 'BLOCKYSIZE=32']

    new_ds = gdaltest.tiledb_drv.CreateCopy('tmp/tiledb_custom', src_ds,
                                          options=options)

    bnd = new_ds.GetRasterBand(1)
    assert bnd.Checksum() == 50054, 'Didnt get expected checksum on still-open file'
    assert bnd.GetBlockSize() == [32, 32]

    bnd = None
    new_ds = None

    gdaltest.tiledb_drv.Delete('tmp/tiledb_custom')

def test_tiff_write_rgb():
    gdaltest.tiledb_drv = gdal.GetDriverByName('TileDB')

    src_ds = gdal.Open('data/rgbsmall.tif')

    new_ds = gdaltest.tiledb_drv.CreateCopy('tmp/tiledb_rgb', src_ds)

    assert new_ds.RasterCount == 3, 'Didnt get expected band count'
    bnd = new_ds.GetRasterBand(2)
    assert bnd.Checksum() == 21053, 'Didnt get expected checksum on still-open file'

    new_ds = None

    gdaltest.tiledb_drv.Delete('tmp/tiledb_rgb')    
