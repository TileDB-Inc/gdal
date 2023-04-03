/******************************************************************************
 *
 * Project:  GDAL TileDB Driver
 * Purpose:  Include tiledb headers
 * Author:   TileDB, Inc
 *
 ******************************************************************************
 * Copyright (c) 2019, TileDB, Inc
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

#ifndef TILEDB_HEADERS_H
#define TILEDB_HEADERS_H

#include <list>

#include "cpl_port.h"
#include "cpl_string.h"
#include "gdal_frmts.h"
#include "gdal_pam.h"
#include "ogrsf_frmts.h"

#ifdef HAVE_GCC_SYSTEM_HEADER
#pragma GCC system_header
#endif

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4996) /* XXXX was deprecated */
#endif

#include "tiledb/tiledb"

#ifdef _MSC_VER
#pragma warning(pop)
#endif

typedef enum
{
    BAND = 0,
    PIXEL = 1,
    ATTRIBUTES = 2
} TILEDB_INTERLEAVE_MODE;

const CPLString TILEDB_VALUES("TDB_VALUES");

/************************************************************************/
/* ==================================================================== */
/*                               TileRasterBand                         */
/* ==================================================================== */
/************************************************************************/

class TileDBRasterBand;

/************************************************************************/
/* ==================================================================== */
/*                               TileDBDataset                          */
/* ==================================================================== */
/************************************************************************/

class TileDBDataset : public GDALPamDataset
{
  protected:
    uint64_t nTimestamp = 0;
    std::unique_ptr<tiledb::Context> m_ctx;
    std::unique_ptr<tiledb::Context> m_roCtx;
    std::unique_ptr<tiledb::Array> m_array;
    std::unique_ptr<tiledb::Array> m_roArray;
    std::unique_ptr<tiledb::ArraySchema> m_schema;
    std::unique_ptr<tiledb::FilterList> m_filterList;

  public:
    CPLErr AddFilter(const char *pszFilterName, const int level);
    static int Identify(GDALOpenInfo *);
    static CPLErr Delete(const char *pszFilename);
    static CPLString VSI_to_tiledb_uri(const char *pszUri);

    static GDALDataset *Open(GDALOpenInfo *);
    static GDALDataset *Create(const char *pszFilename, int nXSize, int nYSize,
                               int nBands, GDALDataType eType,
                               char **papszOptions);
    static GDALDataset *CreateCopy(const char *pszFilename,
                                   GDALDataset *poSrcDS, int bStrict,
                                   char **papszOptions,
                                   GDALProgressFunc pfnProgress,
                                   void *pProgressData);
};

/************************************************************************/
/* ==================================================================== */
/*                            TileDRasterDataset                        */
/* ==================================================================== */
/************************************************************************/

class TileDBRasterDataset final : public TileDBDataset
{
    friend class TileDBRasterBand;

  protected:
    CPLString osMetaDoc;
    TILEDB_INTERLEAVE_MODE eIndexMode = BAND;
    int nBitsPerSample = 8;
    GDALDataType eDataType = GDT_Unknown;
    int nBlockXSize = -1;
    int nBlockYSize = -1;
    int nBlocksX = 0;
    int nBlocksY = 0;
    int nBandStart = 1;
    bool bHasSubDatasets = false;
    int nSubDataCount = 0;
    char **papszSubDatasets = nullptr;
    CPLStringList m_osSubdatasetMD{};
    CPLXMLNode *psSubDatasetsTree = nullptr;
    char **papszAttributes = nullptr;
    std::list<std::unique_ptr<GDALDataset>> lpoAttributeDS = {};

    bool bStats = FALSE;
    CPLErr IRasterIO(GDALRWFlag, int, int, int, int, void *, int, int,
                     GDALDataType, int, int *, GSpacing, GSpacing, GSpacing,
                     GDALRasterIOExtraArg *psExtraArg) override;
    CPLErr CreateAttribute(GDALDataType eType, const CPLString &osAttrName,
                           const int nSubRasterCount = 1);

    CPLErr AddDimensions(tiledb::Domain &domain, tiledb::Dimension &y,
                         tiledb::Dimension &x,
                         tiledb::Dimension *poBands = nullptr);

  public:
    ~TileDBRasterDataset();
    CPLErr TryLoadCachedXML(char **papszSiblingFiles = nullptr,
                            bool bReload = true);
    CPLErr TryLoadXML(char **papszSiblingFiles = nullptr) override;
    CPLErr TrySaveXML() override;
    char **GetMetadata(const char *pszDomain) override;
    virtual CPLErr FlushCache(bool bAtClosing) override;
    static CPLErr CopySubDatasets(GDALDataset *poSrcDS,
                                  TileDBRasterDataset *poDstDS,
                                  GDALProgressFunc pfnProgress,
                                  void *pProgressData);
    static TileDBRasterDataset *CreateLL(const char *pszFilename, int nXSize,
                                         int nYSize, int nBands,
                                         GDALDataType eType,
                                         char **papszOptions);
    static void SetBlockSize(GDALRasterBand *poBand, char **&papszOptions);

    static GDALDataset *Open(GDALOpenInfo *);
    static GDALDataset *Create(const char *pszFilename, int nXSize, int nYSize,
                               int nBands, GDALDataType eType,
                               char **papszOptions);
    static GDALDataset *CreateCopy(const char *pszFilename,
                                   GDALDataset *poSrcDS, int bStrict,
                                   char **papszOptions,
                                   GDALProgressFunc pfnProgress,
                                   void *pProgressData);
};

/************************************************************************/
/*                        OGRTileDBLayer                                */
/************************************************************************/

class OGRTileDBDataset;

class OGRTileDBLayer final : public OGRLayer
{
    friend OGRTileDBDataset;
    std::unique_ptr<tiledb::Context> m_ctx;
    std::unique_ptr<tiledb::Array> m_array;
    std::unique_ptr<tiledb::ArraySchema> m_schema;
    bool bInitialized = false;
    OGRFeatureDefn *poFeatureDefn;
    const CPLString osConfig;
    int nNextFID;
    double nXStart;
    double nYStart;
    double nXEnd;
    double nYEnd;
    std::vector<double> aXs;
    std::vector<double> aYs;
    std::vector<char> aNames;
    std::vector<uint64_t> aNameOffsets;
    std::vector<unsigned char> aGeometries;
    std::vector<uint64_t> aGeometryOffsets;
    uint64_t nBufferCount;
    OGREnvelope sMaxEnvelope;

    void FlushArrays();

  public:
    OGRTileDBLayer(const char *pszFilename, const OGRwkbGeometryType eGType);
    ~OGRTileDBLayer();
    OGRFeature *GetNextFeature() override;
    void ResetReading() override;
    OGRErr ICreateFeature(OGRFeature *poFeature) override;
    OGRErr CreateField(OGRFieldDefn *poField, int bApproxOK) override;
    int TestCapability(const char *) override;
    OGRErr CreateGeomField(OGRGeomFieldDefn *poField, int bApproxOK);
    OGRFeatureDefn *GetLayerDefn() override
    {
        return poFeatureDefn;
    }
};

/************************************************************************/
/*                         OGRTileDBDataset                             */
/************************************************************************/

class OGRTileDBDataset final : public TileDBDataset
{
    friend OGRTileDBLayer;
    std::vector<std::unique_ptr<OGRLayer>> m_apoLayers{};

  public:
    OGRTileDBDataset();
    ~OGRTileDBDataset();
    OGRLayer *ExecuteSQL(const char *pszSQLCommand,
                         OGRGeometry *poSpatialFilter,
                         const char *pszDialect) override;
    int TestCapability(const char *) override;
    OGRLayer *ICreateLayer(const char *pszName,
                           OGRSpatialReference *poSpatialRef = nullptr,
                           OGRwkbGeometryType eGType = wkbUnknown,
                           char **papszOptions = nullptr) override;
    static GDALDataset *Open(GDALOpenInfo *);
    static GDALDataset *Create(const char *pszFilename,
                               CSLConstList papszOptions);
};

#endif  // TILEDB_HEADERS_H
