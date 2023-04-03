/******************************************************************************
 *
 * Project:  GDAL TileDB Driver
 * Purpose:  Implement GDAL TileDB Support based on https://www.tiledb.io
 * Author:   TileDB, Inc
 *
 ******************************************************************************
 * Copyright (c) 2023, TileDB, Inc
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 ****************************************************************************/

#include "tiledbheaders.h"

#include "tiledb/tiledb"

/************************************************************************/
/*                      VSI_to_tiledb_uri()                             */
/************************************************************************/

CPLString TileDBDataset::VSI_to_tiledb_uri(const char *pszUri)
{
    CPLString osUri;

    if (STARTS_WITH_CI(pszUri, "/VSIS3/"))
        osUri.Printf("s3://%s", pszUri + 7);
    else if (STARTS_WITH_CI(pszUri, "/VSIGS/"))
        osUri.Printf("gcs://%s", pszUri + 7);
    else
    {
        osUri = pszUri;
        // tiledb (at least at 2.4.2 on Conda) wrongly interprets relative
        // directories on Windows as absolute ones.
        if (CPLIsFilenameRelative(pszUri))
        {
            char *pszCurDir = CPLGetCurrentDir();
            if (pszCurDir)
                osUri = CPLFormFilename(pszCurDir, pszUri, nullptr);
            CPLFree(pszCurDir);
        }
    }

    return osUri;
}

/************************************************************************/
/*                           AddFilter()                                */
/************************************************************************/

CPLErr TileDBDataset::AddFilter(const char *pszFilterName, const int level)

{
    try
    {
        if (pszFilterName == nullptr)
            m_filterList->add_filter(
                tiledb::Filter(*m_ctx, TILEDB_FILTER_NONE)
                    .set_option(TILEDB_COMPRESSION_LEVEL, level));
        else if EQUAL (pszFilterName, "GZIP")
            m_filterList->add_filter(
                tiledb::Filter(*m_ctx, TILEDB_FILTER_GZIP)
                    .set_option(TILEDB_COMPRESSION_LEVEL, level));
        else if EQUAL (pszFilterName, "ZSTD")
            m_filterList->add_filter(
                tiledb::Filter(*m_ctx, TILEDB_FILTER_ZSTD)
                    .set_option(TILEDB_COMPRESSION_LEVEL, level));
        else if EQUAL (pszFilterName, "LZ4")
            m_filterList->add_filter(
                tiledb::Filter(*m_ctx, TILEDB_FILTER_LZ4)
                    .set_option(TILEDB_COMPRESSION_LEVEL, level));
        else if EQUAL (pszFilterName, "RLE")
            m_filterList->add_filter(
                tiledb::Filter(*m_ctx, TILEDB_FILTER_RLE)
                    .set_option(TILEDB_COMPRESSION_LEVEL, level));
        else if EQUAL (pszFilterName, "BZIP2")
            m_filterList->add_filter(
                tiledb::Filter(*m_ctx, TILEDB_FILTER_BZIP2)
                    .set_option(TILEDB_COMPRESSION_LEVEL, level));
        else if EQUAL (pszFilterName, "DOUBLE-DELTA")
            m_filterList->add_filter(
                tiledb::Filter(*m_ctx, TILEDB_FILTER_DOUBLE_DELTA));
        else if EQUAL (pszFilterName, "POSITIVE-DELTA")
            m_filterList->add_filter(
                tiledb::Filter(*m_ctx, TILEDB_FILTER_POSITIVE_DELTA));
        else
            return CE_Failure;

        return CE_None;
    }
    catch (const tiledb::TileDBError &e)
    {
        CPLError(CE_Failure, CPLE_AppDefined, "%s", e.what());
        return CE_Failure;
    }
}

/************************************************************************/
/*                              Identify()                              */
/************************************************************************/

int TileDBDataset::Identify(GDALOpenInfo *poOpenInfo)

{
    if (STARTS_WITH_CI(poOpenInfo->pszFilename, "TILEDB:"))
    {
        return TRUE;
    }

    try
    {
        const char *pszConfig =
            CSLFetchNameValue(poOpenInfo->papszOpenOptions, "TILEDB_CONFIG");

        if (pszConfig != nullptr)
        {
            return TRUE;
        }

        if (poOpenInfo->bIsDirectory ||
            ((STARTS_WITH_CI(poOpenInfo->pszFilename, "/VSIS3/") ||
              STARTS_WITH_CI(poOpenInfo->pszFilename, "/VSIGS/")) &&
             !EQUAL(CPLGetExtension(poOpenInfo->pszFilename), "tif")))
        {
            tiledb::Context ctx;
            CPLString osArrayPath =
                TileDBDataset::VSI_to_tiledb_uri(poOpenInfo->pszFilename);
            return tiledb::Object::object(ctx, osArrayPath).type() ==
                   tiledb::Object::Type::Array;
        }

        return FALSE;
    }
    catch (...)
    {
        return FALSE;
    }
}

/************************************************************************/
/*                              Delete()                                */
/************************************************************************/

CPLErr TileDBDataset::Delete(const char *pszFilename)

{
    try
    {
        tiledb::Context ctx;
        tiledb::VFS vfs(ctx);
        CPLString osArrayPath = TileDBDataset::VSI_to_tiledb_uri(pszFilename);

        if (vfs.is_dir(osArrayPath))
        {
            vfs.remove_dir(osArrayPath);
            return CE_None;
        }
        else
            return CE_Failure;
    }
    catch (const tiledb::TileDBError &e)
    {
        CPLError(CE_Failure, CPLE_AppDefined, "%s", e.what());
        return CE_Failure;
    }
}

/************************************************************************/
/*                                Open()                                */
/************************************************************************/

GDALDataset *TileDBDataset::Open(GDALOpenInfo *poOpenInfo)

{
    try
    {
        if (!TileDBDataset::Identify(poOpenInfo))
            return nullptr;

        if (STARTS_WITH_CI(poOpenInfo->pszFilename, "TILEDB:") &&
            !STARTS_WITH_CI(poOpenInfo->pszFilename, "TILEDB://"))
        {
            // subdataset URI so this is a raster
            return TileDBRasterDataset::Open(poOpenInfo);
        }
        else
        {
            const char *pszConfig = CSLFetchNameValue(
                poOpenInfo->papszOpenOptions, "TILEDB_CONFIG");
            std::unique_ptr<tiledb::Context> poCtx;

            if (pszConfig != nullptr)
            {
                tiledb::Config cfg(pszConfig);
                poCtx.reset(new tiledb::Context(cfg));
            }
            else
                poCtx.reset(new tiledb::Context());

            tiledb::ArraySchema schema(*poCtx, poOpenInfo->pszFilename);

            if (schema.array_type() == TILEDB_SPARSE)
                return OGRTileDBDataset::Open(poOpenInfo);
            else
                return TileDBRasterDataset::Open(poOpenInfo);
        }
    }
    catch (const tiledb::TileDBError &e)
    {
        CPLError(CE_Failure, CPLE_AppDefined, "%s", e.what());
        return nullptr;
    }
}

/************************************************************************/
/*                              Create()                                */
/************************************************************************/

GDALDataset *TileDBDataset::Create(const char *pszFilename, int nXSize,
                                   int nYSize, int nBandsIn, GDALDataType eType,
                                   char **papszOptions)

{
    try
    {
        if (nBandsIn > 0)
            return TileDBRasterDataset::Create(pszFilename, nXSize, nYSize,
                                               nBandsIn, eType, papszOptions);
        else
            return OGRTileDBDataset::Create(pszFilename, papszOptions);
    }
    catch (const tiledb::TileDBError &e)
    {
        CPLError(CE_Failure, CPLE_AppDefined, "%s", e.what());
        return nullptr;
    }

    return nullptr;
}

/************************************************************************/
/*                              CreateCopy()                            */
/************************************************************************/

GDALDataset *TileDBDataset::CreateCopy(const char *pszFilename,
                                       GDALDataset *poSrcDS, int bStrict,
                                       char **papszOptions,
                                       GDALProgressFunc pfnProgress,
                                       void *pProgressData)

{
    try
    {
        if (poSrcDS->GetRasterCount() > 0)
            return TileDBRasterDataset::CreateCopy(pszFilename, poSrcDS,
                                                   bStrict, papszOptions,
                                                   pfnProgress, pProgressData);
        else
            return OGRTileDBDataset::CreateCopy(pszFilename, poSrcDS, bStrict,
                                                papszOptions, pfnProgress,
                                                pProgressData);
    }
    catch (const tiledb::TileDBError &e)
    {
        CPLError(CE_Failure, CPLE_AppDefined, "%s", e.what());
        return nullptr;
    }

    return nullptr;
}

/************************************************************************/
/*                         GDALRegister_TILEDB()                        */
/************************************************************************/

void GDALRegister_TileDB()

{
    if (GDALGetDriverByName("TileDB") != nullptr)
        return;

    GDALDriver *poDriver = new GDALDriver();

    poDriver->SetDescription("TileDB");
    poDriver->SetMetadataItem(GDAL_DCAP_RASTER, "YES");
    poDriver->SetMetadataItem(GDAL_DCAP_VECTOR, "YES");
    poDriver->SetMetadataItem(GDAL_DCAP_SUBCREATECOPY, "YES");
    poDriver->SetMetadataItem(GDAL_DMD_LONGNAME, "TileDB");
    poDriver->SetMetadataItem(GDAL_DMD_HELPTOPIC, "drivers/raster/tiledb.html");
    poDriver->SetMetadataItem(GDAL_DMD_CREATIONDATATYPES,
                              "Byte UInt16 Int16 UInt32 Int32 Float32 "
                              "Float64 CInt16 CInt32 CFloat32 CFloat64");
    poDriver->SetMetadataItem(GDAL_DCAP_VIRTUALIO, "YES");
    poDriver->SetMetadataItem(GDAL_DMD_SUBDATASETS, "YES");
    poDriver->SetMetadataItem(
        GDAL_DMD_CREATIONOPTIONLIST,
        "<CreationOptionList>\n"
        "   <Option name='COMPRESSION' type='string-select' description='image "
        "compression to use' default='NONE'>\n"
        "       <Value>NONE</Value>\n"
        "       <Value>GZIP</Value>\n"
        "       <Value>ZSTD</Value>\n"
        "       <Value>LZ4</Value>\n"
        "       <Value>RLE</Value>\n"
        "       <Value>BZIP2</Value>\n"
        "       <Value>DOUBLE-DELTA</Value>\n"
        "       <Value>POSITIVE-DELTA</Value>\n"
        "   </Option>\n"
        "   <Option name='COMPRESSION_LEVEL' type='int' "
        "description='Compression level'/>\n"
        "   <Option name='BLOCKXSIZE' type='int' description='Tile Width'/>"
        "   <Option name='BLOCKYSIZE' type='int' description='Tile Height'/>"
        "   <Option name='STATS' type='boolean' description='Dump TileDB "
        "stats'/>"
        "   <Option name='TILEDB_CONFIG' type='string' description='location "
        "of configuration file for TileDB'/>"
        "   <Option name='TILEDB_ATTRIBUTE' type='string' "
        "description='co-registered file to add as TileDB attributes, only "
        "applicable for interleave types of band or pixel'/>"
        "   <Option name='INTERLEAVE' type='string-select' "
        "description='Indexing order' default='BAND'>\n"
        "        <Value>BAND</Value>\n"
        "        <Value>PIXEL</Value>\n"
        "        <Value>ATTRIBUTES</Value>\n"
        "   </Option>\n"
        "   <Option name='TILEDB_TIMESTAMP' type='int' description='Create "
        "array at this timestamp, the timestamp should be > 0'/>\n"
        "   <Option name='BOUNDS' type='string' description='Specify "
        "bounds for sparse array, minx, miny, maxx, maxy'/>\n"
        "</CreationOptionList>\n");

    poDriver->SetMetadataItem(
        GDAL_DMD_OPENOPTIONLIST,
        "<OpenOptionList>"
        "   <Option name='STATS' type='boolean' description='Dump TileDB "
        "stats'/>"
        "   <Option name='TILEDB_ATTRIBUTE' type='string' "
        "description='Attribute to read from each band'/>"
        "   <Option name='TILEDB_CONFIG' type='string' description='location "
        "of configuration file for TileDB'/>"
        "   <Option name='TILEDB_TIMESTAMP' type='int' description='Open array "
        "at this timestamp, the timestamp should be > 0'/>"
        "</OpenOptionList>");

    poDriver->pfnIdentify = TileDBDataset::Identify;
    poDriver->pfnOpen = TileDBDataset::Open;
    poDriver->pfnCreate = TileDBDataset::Create;
    poDriver->pfnCreateCopy = TileDBDataset::CreateCopy;
    poDriver->pfnDelete = TileDBDataset::Delete;

    GetGDALDriverManager()->RegisterDriver(poDriver);
}
