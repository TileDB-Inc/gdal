/******************************************************************************
 *
 * Project:  GDAL TileDB Driver
 * Purpose:  Implement GDAL TileDB Support based on https://www.tiledb.io
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

#include "cpl_string.h"
#include "gdal_frmts.h"
#include "gdal_pam.h"

#include "tiff.h"

#include "tiledb/tiledb"

CPL_CVSID("$Id$")


const CPLString TILEDB_VALUES( "VALUES" );

/************************************************************************/
/* ==================================================================== */
/*                               TileDBDataset                          */
/* ==================================================================== */
/************************************************************************/

class TileDBRasterBand;

class TileDBDataset : public GDALPamDataset
{
    friend class TileDBRasterBand;

    protected:
        uint16      nBitsPerSample = 8;
        GDALDataType eDataType = GDT_Byte;
        int         nBlockXSize = -1;
        int         nBlockYSize = -1;

        std::unique_ptr<tiledb::Context> m_ctx;
        std::unique_ptr<tiledb::Array> m_array;
        std::unique_ptr<tiledb::FilterList> m_filterList;

        bool bStats = FALSE;
        CPLErr AddFilter( const char* pszFilterName, const int level );
        CPLErr CreateAttribute(tiledb::ArraySchema& schema);
    public:
        virtual ~TileDBDataset();

        static GDALDataset *Open( GDALOpenInfo * );
        static int          Identify( GDALOpenInfo * );
        static CPLErr       Delete( const char * pszFilename );	
        static GDALDataset *Create( const char * pszFilename,
                                int nXSize, int nYSize, int nBands,
                                GDALDataType eType, char ** papszParmList );
};

/************************************************************************/
/* ==================================================================== */
/*                            TileDBRasterBand                          */
/* ==================================================================== */
/************************************************************************/

class TileDBRasterBand : public GDALPamRasterBand
{
    friend class TileDBDataset;
    protected:
        TileDBDataset  *poGDS;
        CPLErr SetBuffer( tiledb::Query& query, GDALDataType eType, void * pImage, int nSize );
    public:
        TileDBRasterBand( TileDBDataset *, int );
        virtual ~TileDBRasterBand();
        virtual CPLErr IReadBlock( int, int, void * ) override;
        virtual CPLErr IWriteBlock( int, int, void * ) override;
        virtual GDALColorInterp GetColorInterpretation() override;
};

/************************************************************************/
/*                          TileDBRasterBand()                          */
/************************************************************************/

TileDBRasterBand::TileDBRasterBand( TileDBDataset *poDSIn, int nBandIn ) :
    poGDS(poDSIn)
{
    poDS = poDSIn;
    nBand = nBandIn;
    eDataType = poGDS->eDataType;
    eAccess = poGDS->eAccess;
    nRasterXSize = poGDS->nRasterXSize;
    nRasterYSize = poGDS->nRasterYSize;
    nBlockXSize = poGDS->nBlockXSize;
    nBlockYSize = poGDS->nBlockYSize;
}

/************************************************************************/
/*                          ~TileDBRasterBand()                         */
/************************************************************************/
TileDBRasterBand::~TileDBRasterBand()

{
    FlushCache();
}


/************************************************************************/
/*                             SetBuffer()                              */
/************************************************************************/

CPLErr TileDBRasterBand::SetBuffer( tiledb::Query& query, GDALDataType eType, void * pImage, int nSize )
{
 switch (eDataType)
    {
        case GDT_Byte:
            query.set_buffer( TILEDB_VALUES, reinterpret_cast<unsigned char*>( pImage ), nSize );
            break;
        case GDT_UInt16:
            query.set_buffer( TILEDB_VALUES, reinterpret_cast<unsigned short*>( pImage ), nSize );
            break;
        case GDT_UInt32:
            query.set_buffer( TILEDB_VALUES, reinterpret_cast<unsigned int*>( pImage ), nSize );
            break;
        case GDT_Int16:
            query.set_buffer( TILEDB_VALUES, reinterpret_cast<short*>( pImage ), nSize );
            break;
        case GDT_Int32:
            query.set_buffer( TILEDB_VALUES, reinterpret_cast<int*>( pImage ), nSize );
            break;
        case GDT_Float32:
            query.set_buffer( TILEDB_VALUES, reinterpret_cast<float*>( pImage ), nSize );
            break;
        case GDT_Float64:
            query.set_buffer( TILEDB_VALUES, reinterpret_cast<double*>( pImage ), nSize );
            break;
        case GDT_CInt16:
            query.set_buffer( TILEDB_VALUES, reinterpret_cast<short*>( pImage ), nSize );
            break;
        case GDT_CInt32:
            query.set_buffer( TILEDB_VALUES, reinterpret_cast<int*>( pImage ), nSize * 2);
            break;
        case GDT_CFloat32:
            query.set_buffer( TILEDB_VALUES, reinterpret_cast<float*>( pImage ), nSize * 2);
            break;
        case GDT_CFloat64:
            query.set_buffer( TILEDB_VALUES, reinterpret_cast<double*>( pImage ), nSize * 2);
            break;
        default:
            return CE_Failure;
    }
    return CE_None;
}

/************************************************************************/
/*                             IReadBlock()                             */
/************************************************************************/

CPLErr TileDBRasterBand::IReadBlock( int nBlockXOff, int nBlockYOff,
                                   void * pImage )
{
    tiledb::Query query( *poGDS->m_ctx, *poGDS->m_array );

    size_t nStartX = nBlockXOff * nBlockXSize;
    size_t nStartY = nBlockYOff * nBlockYSize;

    query.set_layout( TILEDB_ROW_MAJOR );   
    std::vector<size_t> subarray = { nStartX, nStartX + nBlockXSize - 1, 
                                     nStartY, nStartY + nBlockYSize - 1,
                                     nBand, nBand };

    SetBuffer(query, eDataType, pImage, nBlockXSize * nBlockYSize);
    query.set_subarray(subarray);

    if ( poGDS->bStats )
        tiledb::Stats::enable();

    auto status = query.submit();

    if ( poGDS->bStats )
    {
        tiledb::Stats::dump(stdout);
        tiledb::Stats::disable();
    }

    if (status != tiledb::Query::Status::COMPLETE)
        return CE_Failure;
    else
        return CE_None;
}

/************************************************************************/
/*                             IWriteBlock()                            */
/************************************************************************/
CPLErr TileDBRasterBand::IWriteBlock( int nBlockXOff, int nBlockYOff,
                                     void * pImage )

{ 
    if( eAccess == GA_ReadOnly )
    {
        CPLError( CE_Failure, CPLE_NoWriteAccess,
                  "Unable to write block, dataset opened read only.\n" );
        return CE_Failure;
    }

    CPLAssert( poGDS != nullptr
               && nBlockXOff >= 0
               && nBlockYOff >= 0
               && pImage != nullptr );

    tiledb::Query query( *poGDS->m_ctx, *poGDS->m_array );

    size_t nStartX = nBlockXOff * nBlockXSize;
    size_t nStartY = nBlockYOff * nBlockYSize;

    query.set_layout( TILEDB_GLOBAL_ORDER );
    std::vector<size_t> subarray = { nStartX, nStartX + nBlockXSize - 1, 
                                     nStartY, nStartY + nBlockYSize - 1,
                                     nBand, nBand };
    query.set_subarray(subarray);

    SetBuffer(query, eDataType, pImage, nBlockXSize * nBlockYSize);

    if ( poGDS->bStats )
        tiledb::Stats::enable();

    auto status = query.submit();

    if ( poGDS->bStats )
    {
        tiledb::Stats::dump(stdout);
        tiledb::Stats::disable();
    }

    query.finalize();

    if (status != tiledb::Query::Status::COMPLETE)
        return CE_Failure;
    else
        return CE_None;
}

/************************************************************************/
/*                       GetColorInterpretation()                       */
/************************************************************************/

GDALColorInterp TileDBRasterBand::GetColorInterpretation()

{
    if (poGDS->nBands == 1)
        return GCI_GrayIndex;

    if ( nBand == 1 )
        return GCI_RedBand;

    else if( nBand == 2 )
        return GCI_GreenBand;

    else if ( nBand == 3 )
        return GCI_BlueBand;

    return GCI_AlphaBand;
}

/************************************************************************/
/* ==================================================================== */
/*                             TileDBDataset                            */
/* ==================================================================== */
/************************************************************************/

/************************************************************************/
/*                           ~TileDBDataset()                           */
/************************************************************************/

TileDBDataset::~TileDBDataset()

{
    FlushCache();
    m_array->close();
}

/************************************************************************/
/*                           AddFilter()                                */
/************************************************************************/

CPLErr TileDBDataset::AddFilter( const char* pszFilterName, const int level )

{
   if (pszFilterName == nullptr)
        m_filterList->add_filter( tiledb::Filter( *m_ctx, TILEDB_FILTER_NONE )
            .set_option( TILEDB_COMPRESSION_LEVEL, level ) );
    else if EQUAL(pszFilterName, "GZIP")
        m_filterList->add_filter( tiledb::Filter( *m_ctx, TILEDB_FILTER_GZIP )
            .set_option( TILEDB_COMPRESSION_LEVEL, level ) );
    else if EQUAL(pszFilterName, "ZSTD")
        m_filterList->add_filter( tiledb::Filter( *m_ctx, TILEDB_FILTER_ZSTD )
            .set_option( TILEDB_COMPRESSION_LEVEL, level ) );
    else if EQUAL(pszFilterName, "LZ4")
        m_filterList->add_filter( tiledb::Filter( *m_ctx, TILEDB_FILTER_LZ4 )
            .set_option( TILEDB_COMPRESSION_LEVEL, level ) );
    else if EQUAL(pszFilterName, "RLE")
        m_filterList->add_filter( tiledb::Filter( *m_ctx, TILEDB_FILTER_RLE )
            .set_option( TILEDB_COMPRESSION_LEVEL, level ) );
    else if EQUAL(pszFilterName, "BZIP2")
        m_filterList->add_filter( tiledb::Filter( *m_ctx, TILEDB_FILTER_BZIP2 )
            .set_option( TILEDB_COMPRESSION_LEVEL, level ) );
    else if EQUAL(pszFilterName, "DOUBLE-DELTA")
        m_filterList->add_filter( tiledb::Filter( *m_ctx, TILEDB_FILTER_DOUBLE_DELTA )
            .set_option( TILEDB_COMPRESSION_LEVEL, level ) );
    else if EQUAL(pszFilterName, "POSITIVE-DELTA")
        m_filterList->add_filter( tiledb::Filter( *m_ctx, TILEDB_FILTER_POSITIVE_DELTA )
            .set_option( TILEDB_COMPRESSION_LEVEL, level ) );
    else
        return CE_Failure;
    
    return CE_None;
}

/************************************************************************/
/*                              Delete()                                */
/************************************************************************/

CPLErr TileDBDataset::Delete( const char * pszFilename )

{
    tiledb::Context ctx;
    tiledb::VFS vfs(ctx);
    if ( vfs.is_dir( pszFilename ) )
    {
        vfs.remove_dir( pszFilename );
        return CE_None;
    }
    else
        return CE_Failure;
}

/************************************************************************/
/*                              Identify()                              */
/************************************************************************/

int TileDBDataset::Identify( GDALOpenInfo * poOpenInfo )

{
    // use GDAL VSI for Identify
    if( poOpenInfo->bIsDirectory )
    {
        const char* pszArrayName = CPLGetBasename( poOpenInfo->pszFilename ); 
        const int nMaxFiles =
            atoi(CPLGetConfigOption( "GDAL_READDIR_LIMIT_ON_OPEN", "1000" ) );
        char** papszSiblingFiles = VSIReadDirEx( poOpenInfo->pszFilename, nMaxFiles );

        CPLString osAux;
        osAux.Printf( "%s.tdb.aux.xml", pszArrayName );        
        if( CSLFindString( papszSiblingFiles, osAux ) != -1 )
        {
            return TRUE;
        }
    }
    return FALSE;
}

/************************************************************************/
/*                                Open()                                */
/************************************************************************/

GDALDataset *TileDBDataset::Open( GDALOpenInfo * poOpenInfo )

{
    if( !Identify( poOpenInfo ) )
        return nullptr;

    TileDBDataset *poDS = new TileDBDataset();

    const char* pszConfig = CSLFetchNameValue( poOpenInfo->papszOpenOptions, "CONFIG" );
    if( pszConfig != nullptr )
    {
        tiledb::Config cfg( pszConfig );
        poDS->m_ctx.reset( new tiledb::Context( cfg ) );
    }
    else
    {
        poDS->m_ctx.reset( new tiledb::Context() );
    }

    CPLString osArrayPath;
    osArrayPath = poOpenInfo->pszFilename; 
    const char* pszArrayName = CPLGetBasename( osArrayPath ); 
    CPLString osAux;
    osAux.Printf( "%s.tdb", pszArrayName );        

    poDS->m_array.reset( new tiledb::Array( *poDS->m_ctx, osArrayPath, TILEDB_READ ) );

    const tiledb::ArraySchema schema = poDS->m_array->schema();
    std::vector<tiledb::Dimension> dims = schema.domain().dimensions();

    if( dims.size() == 3 )
    {
        poDS->nBands = dims[2].domain<size_t>().second
                                - dims[2].domain<size_t>().first + 1;
        poDS->nBlockXSize = dims[0].tile_extent<size_t>();
        poDS->nBlockYSize = dims[1].tile_extent<size_t>();
        
        if( poDS->nRasterXSize <= 0 || poDS->nRasterYSize <= 0 || poDS->nBands <= 0)
        {
            CPLError( CE_Failure, CPLE_AppDefined,
                    "Invalid dimensions : %d x %d, bands: %i",
                    poDS->nRasterXSize, poDS->nRasterYSize, poDS->nBands );
            delete poDS;
            return nullptr;
        }
    }
    else
    {
        delete poDS;
        CPLError( CE_Failure, CPLE_AppDefined,
            "Wrong number of dimensions %li expected 3.", dims.size() );
        return nullptr;
    }

    // aux file is in array folder
    poDS->SetPhysicalFilename( CPLFormFilename( osArrayPath, osAux, nullptr ) );
    // Initialize any PAM information.
    poDS->SetDescription( osArrayPath );
    // dependent on PAM metadata for information about array
    poDS->TryLoadXML();

    const char* pszXSize = poDS->GetMetadataItem( "X_SIZE", "IMAGE_STRUCTURE" );
    poDS->nRasterXSize = ( pszXSize ) ? atoi( pszXSize ) : 8;

    const char* pszYSize = poDS->GetMetadataItem( "Y_SIZE", "IMAGE_STRUCTURE" );
    poDS->nRasterYSize = ( pszYSize ) ? atoi( pszYSize ) : 8;

    const char* pszNBits = poDS->GetMetadataItem( "NBITS", "IMAGE_STRUCTURE" );
    poDS->nBitsPerSample = ( pszNBits ) ? atoi( pszNBits ) : 8;

    const char* pszDataType = poDS->GetMetadataItem( "DATA_TYPE", "IMAGE_STRUCTURE" );
    poDS->eDataType = ( pszDataType ) ? static_cast<GDALDataType>( atoi( pszDataType ) ) : GDT_Unknown;

    poDS->eAccess = poOpenInfo->eAccess;

    // Create band information objects.
    for ( int i = 1; i <= poDS->nBands; ++i )
    {
        poDS->SetBand( i, new TileDBRasterBand( poDS, i ) );
    }

    poDS->oOvManager.Initialize( poDS, CPLFormFilename(osArrayPath, pszArrayName, nullptr ) );
    
    return poDS;
}


/************************************************************************/
/*                              CreateAttribute()                       */
/************************************************************************/
CPLErr TileDBDataset::CreateAttribute(tiledb::ArraySchema& schema)
{
    switch (eDataType)
    {
        case GDT_Byte:
        {
            tiledb::Attribute attr = tiledb::Attribute::create<unsigned char>( *m_ctx, TILEDB_VALUES );
            if ( m_filterList->nfilters() > 0 )
                attr.set_filter_list(*m_filterList);
            schema.add_attribute( attr );
            nBitsPerSample = 8;
            break;
        }
        case GDT_UInt16:
        {
            schema.add_attribute( tiledb::Attribute::create<unsigned short>( *m_ctx, TILEDB_VALUES ) );
            nBitsPerSample = 16;
            break;
        }
        case GDT_UInt32:
        {
            schema.add_attribute( tiledb::Attribute::create<unsigned int>( *m_ctx, TILEDB_VALUES ) );
            nBitsPerSample = 32;
            break;
        }
        case GDT_Int16:
        {
            schema.add_attribute( tiledb::Attribute::create<short>( *m_ctx, TILEDB_VALUES ) );
            nBitsPerSample = 16;
            break;
        }
        case GDT_Int32:
        {
            schema.add_attribute( tiledb::Attribute::create<int>( *m_ctx, TILEDB_VALUES ) );
            nBitsPerSample = 32;
            break;
        }
        case GDT_Float32:
        {
            schema.add_attribute( tiledb::Attribute::create<float>( *m_ctx, TILEDB_VALUES ) );
            nBitsPerSample = 32;
            break;
        }
        case GDT_Float64:
        {
            schema.add_attribute( tiledb::Attribute::create<double>( *m_ctx, TILEDB_VALUES ) );
            nBitsPerSample = 64;
            break;
        }
        case GDT_CInt16:
        {
            schema.add_attribute( tiledb::Attribute::create<short[2]>( *m_ctx, TILEDB_VALUES ) );
            nBitsPerSample = 16;
            break;
        }
        case GDT_CInt32:
        {
            schema.add_attribute( tiledb::Attribute::create<int[2]>( *m_ctx, TILEDB_VALUES ) );
            nBitsPerSample = 32;
            break;
        }
        case GDT_CFloat32:
        {
            schema.add_attribute( tiledb::Attribute::create<float[2]>( *m_ctx, TILEDB_VALUES ) );
            nBitsPerSample = 32;
            break;
        }
        case GDT_CFloat64:
        {
            schema.add_attribute( tiledb::Attribute::create<double[2]>( *m_ctx, TILEDB_VALUES ) );
            nBitsPerSample = 64;
            break;
        }
        default:
            return CE_Failure;
    }
    return CE_None;
}

/************************************************************************/
/*                              Create()                                */
/************************************************************************/

GDALDataset *
TileDBDataset::Create( const char * pszFilename, int nXSize, int nYSize, int nBands,
        GDALDataType eType, char ** papszParmList )

{
    TileDBDataset *poDS = new TileDBDataset();
    poDS->nRasterXSize = nXSize;
    poDS->nRasterYSize = nYSize;
    poDS->nBands = nBands;
    poDS->eAccess = GA_Update;
    poDS->eDataType = eType;

    const char* pszConfig = CSLFetchNameValue( papszParmList, "CONFIG" );
    if( pszConfig != nullptr )
    {
        tiledb::Config cfg( pszConfig );
        poDS->m_ctx.reset( new tiledb::Context( cfg ) );
    }
    else
    {
        poDS->m_ctx.reset( new tiledb::Context() );
    }

    const char* pszCompression = CSLFetchNameValue( papszParmList, "COMPRESSION" );  
    const char* pszCompressionLevel = CSLFetchNameValue( papszParmList, "COMPRESSION_LEVEL" );
 
    const char* pszBlockXSize = CSLFetchNameValue( papszParmList, "BLOCKXSIZE" );
    poDS->nBlockXSize = ( pszBlockXSize ) ? atoi( pszBlockXSize ) : 256;
    const char* pszBlockYSize = CSLFetchNameValue( papszParmList, "BLOCKYSIZE" );
    poDS->nBlockYSize = ( pszBlockYSize ) ? atoi( pszBlockYSize ) : 256;
    poDS->bStats = CSLFetchBoolean( papszParmList, "STATS", FALSE );

    for( int i = 0; i < poDS->nBands; i++ )
        poDS->SetBand( i+1, new TileDBRasterBand( poDS, i+1 ) );

    // set dimensions and attribute type for schema
    tiledb::ArraySchema schema( *poDS->m_ctx, TILEDB_DENSE );
    schema.set_tile_order( TILEDB_ROW_MAJOR );
    schema.set_cell_order( TILEDB_ROW_MAJOR );

    poDS->m_filterList.reset(new tiledb::FilterList(*poDS->m_ctx));

    if (pszCompression != nullptr)
    {
        int nLevel = ( pszCompressionLevel ) ? atoi( pszCompressionLevel ) : -1;
        poDS->AddFilter(pszCompression, nLevel);
        schema.set_coords_filter_list(*poDS->m_filterList);
    }

    tiledb::Domain domain( *poDS->m_ctx );

    // Note the dimension bounds are inclusive and are expanded to the match the block size
    // TODO creating dims in tileb with int type fails with Static type (UINT64) does not match expected type (INT32)
    size_t w = DIV_ROUND_UP(nXSize, poDS->nBlockXSize) * poDS->nBlockXSize;
    size_t h = DIV_ROUND_UP(nYSize, poDS->nBlockYSize) * poDS->nBlockYSize;
    
    auto d1 = tiledb::Dimension::create<size_t>( *poDS->m_ctx, "X", {0, w}, poDS->nBlockXSize );
    auto d2 = tiledb::Dimension::create<size_t>( *poDS->m_ctx, "Y", {0, h}, poDS->nBlockYSize );
    auto d3 = tiledb::Dimension::create<size_t>( *poDS->m_ctx, "BANDS", {1, nBands}, 1);

    domain.add_dimensions( d1, d2, d3 );
    schema.set_domain( domain );

    poDS->CreateAttribute( schema );

    tiledb::Array::create( pszFilename, schema );
    poDS->m_array.reset( new tiledb::Array( *poDS->m_ctx, pszFilename, TILEDB_WRITE ) );

    const char* pszArrayName = CPLGetBasename( pszFilename ); 
    CPLString osAux;
    osAux.Printf( "%s.tdb", pszArrayName );        

    // aux file is in array folder, GDAL VSI will write this
    poDS->SetPhysicalFilename( CPLFormFilename( pszFilename, osAux.c_str(), nullptr ) );

    // Initialize any PAM information.
    poDS->SetDescription( pszFilename );

    poDS->SetMetadataItem( "NBITS", CPLString().Printf( "%i", poDS->nBitsPerSample ), "IMAGE_STRUCTURE" );
    poDS->SetMetadataItem( "X_SIZE", CPLString().Printf( "%i", poDS->nRasterXSize ), "IMAGE_STRUCTURE" );
    poDS->SetMetadataItem( "Y_SIZE", CPLString().Printf( "%i", poDS->nRasterYSize ), "IMAGE_STRUCTURE" );
    poDS->SetMetadataItem( "DATA_TYPE", CPLString().Printf( "%i", poDS->eDataType ), "IMAGE_STRUCTURE" );

    return poDS;
}

/************************************************************************/
/*                         GDALRegister_TILEDB()                        */
/************************************************************************/

void GDALRegister_TileDB()

{
    if( GDALGetDriverByName( "TileDB" ) != nullptr )
        return;

    GDALDriver *poDriver = new GDALDriver();

    poDriver->SetDescription( "TileDB" );
    poDriver->SetMetadataItem( GDAL_DCAP_RASTER, "YES" );
    poDriver->SetMetadataItem( GDAL_DMD_LONGNAME, "TileDB" );
    poDriver->SetMetadataItem( GDAL_DMD_HELPTOPIC, "frmt_tiledb.html" );
    poDriver->SetMetadataItem( GDAL_DMD_CREATIONDATATYPES,
                               "Byte UInt16 Int16 UInt32 Int32 Float32 "
                               "Float64 CInt16 CInt32 CFloat32 CFloat64" );
    poDriver->SetMetadataItem(GDAL_DCAP_VIRTUALIO, "YES");

    poDriver->SetMetadataItem( GDAL_DMD_CREATIONOPTIONLIST,
"<CreationOptionList>\n"
"   <Option name='COMPRESSION' type='string-select' description='image compression to use' default='NONE'>\n"
"       <Value>NONE</Value>\n"
"       <Value>GZIP</Value>\n"
"       <Value>ZSTD</Value>\n"
"       <Value>LZ4</Value>\n"
"       <Value>RLE</Value>\n"
"       <Value>BZIP2</Value>\n"
"       <Value>DOUBLE-DELTA</Value>\n"
"       <Value>POSITIVE-DELTA</Value>\n"
"   </Option>\n"
"   <Option name='COMPRESSION_LEVEL' type='int' description='Compression level'/>\n"
"   <Option name='BLOCKXSIZE' type='int' description='Tile Width'/>"
"   <Option name='BLOCKYSIZE' type='int' description='Tile Height'/>"
"   <Option name='STATS' type='boolean' description='Dump TileDB stats'/>"
"   <Option name='CONFIG' type='string' description='location of configuration file for TileDB'/>"
"</CreationOptionList>\n" );

    poDriver->SetMetadataItem( GDAL_DMD_OPENOPTIONLIST,
"<OpenOptionList>"
"   <Option name='STATS' type='boolean' description='Dump TileDB stats'/>"
"   <Option name='CONFIG' type='string' description='location of configuration file for TileDB'/>"
"</OpenOptionList>" );

    poDriver->pfnIdentify = TileDBDataset::Identify;
    poDriver->pfnOpen = TileDBDataset::Open;
    poDriver->pfnCreate = TileDBDataset::Create;
    poDriver->pfnDelete = TileDBDataset::Delete;

    GetGDALDriverManager()->RegisterDriver( poDriver );
}
