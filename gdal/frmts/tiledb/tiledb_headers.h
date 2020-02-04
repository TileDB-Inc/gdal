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

#ifdef HAVE_GCC_SYSTEM_HEADER
#pragma GCC system_header
#endif

#include "tiledb/tiledb"

const CPLString TILEDB_VALUES( "TDB_VALUES" );

/************************************************************************/
/* ==================================================================== */
/*                               TileDBDataset                          */
/* ==================================================================== */
/************************************************************************/

class TileDBRasterBand;

class TileDBDataset final: public GDALPamDataset
{
    friend class TileDBRasterBand;
    friend class TileDBGroup;

    protected:
        int           nBitsPerSample = 8;
        GDALDataType  eDataType = GDT_Unknown;
        int           nBlockXSize = -1;
        int           nBlockYSize = -1;
        int           nBlocksX = 0;
        int           nBlocksY = 0;
        int           nBandStart = 1;
        bool          bGlobalOrder = false;
        bool          bHasSubDatasets = false;
        int           nSubDataCount = 0;
        char          **papszSubDatasets = nullptr;
        CPLStringList m_osSubdatasetMD{};
        CPLXMLNode*   psSubDatasetsTree = nullptr;
        CPLString     osMetaDoc;
        CPLString     osRootGroup;

        char          **papszCreationOptions;

        std::shared_ptr<GDALGroup> m_poRootGroup{};
        std::unique_ptr<tiledb::Context> m_ctx;
        std::unique_ptr<tiledb::Context> m_roCtx;
        std::unique_ptr<tiledb::Array> m_array;
        std::unique_ptr<tiledb::Array> m_roArray;
        std::unique_ptr<tiledb::ArraySchema> m_schema;
        std::unique_ptr<tiledb::FilterList> m_filterList;

        char          **papszAttributes = nullptr;
        std::list<std::unique_ptr<GDALDataset>> lpoAttributeDS = {};

        bool bStats = FALSE;
        CPLErr AddFilter( const char* pszFilterName, const int level );
        CPLErr CreateAttribute( GDALDataType eType, const CPLString& osAttrName,
                                const int nSubRasterCount=1 );
    public:
        TileDBDataset(): papszCreationOptions(nullptr) {};
        virtual ~TileDBDataset();

        CPLErr TryLoadCachedXML(char **papszSiblingFiles = nullptr, bool bReload=true);
        CPLErr TryLoadXML(char **papszSiblingFiles = nullptr) override;
        CPLErr TrySaveXML() override;
        char** GetMetadata(const char *pszDomain) override;
        static GDALDataset      *Open( GDALOpenInfo * );
        static int              Identify( GDALOpenInfo * );
        static CPLErr           Delete( const char * pszFilename );
        static CPLErr           CopySubDatasets( GDALDataset* poSrcDS,
                                                TileDBDataset* poDstDS,
                                                GDALProgressFunc pfnProgress,
                                                void *pProgressData );
        static TileDBDataset    *CreateLL( const char * pszFilename,
                                    int nXSize, int nYSize, int nBands,
                                    char ** papszOptions );
        static GDALDataset      *Create( const char * pszFilename,
                                    int nXSize, int nYSize, int nBands,
                                    GDALDataType eType, char ** papszOptions );
        static GDALDataset      *CreateCopy( const char * pszFilename,
                                    GDALDataset *poSrcDS,
                                    int bStrict, char ** papszOptions,
                                    GDALProgressFunc pfnProgress,
                                    void *pProgressData);
        static void             ErrorHandler( const std::string& msg );
        static void             SetBlockSize( GDALRasterBand* poBand,
                                                char ** &papszOptions );

#if TILEDB_VERSION_MAJOR >= 1 && TILEDB_VERSION_MINOR >= 7
        // multidim api
        std::shared_ptr<GDALGroup> GetRootGroup() const override;
        static GDALDataset *CreateMultiDimensional( const char * pszFilename,
                                                CSLConstList papszRootGroupOptions,
                                                CSLConstList papszOptions );
#endif
};

#endif // TILEDB_HEADERS_H
