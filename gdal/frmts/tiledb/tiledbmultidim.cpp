/******************************************************************************
 *
 * Project:  GDAL TileDB Driver
 * Purpose:  Implement GDAL TileDB Support based on https://www.tiledb.io
 * Author:   TileDB, Inc
 *
 ******************************************************************************
 * Copyright (c) 2020, TileDB, Inc
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

#include "tiledb_headers.h"

CPL_CVSID("$Id$")

#if TILEDB_VERSION_MAJOR >= 1 && TILEDB_VERSION_MINOR >= 7

/************************************************************************/
/*                             SetBuffer()                              */
/************************************************************************/

static CPLErr SetBuffer( tiledb::Query* poQuery, GDALDataType eType,
                        const CPLString& osAttrName, void * pImage, int nSize )
{
    // TODO harmonize this with TileDBDataset SetBuffer and GDALExtendedTypes
    switch (eType)
    {
        case GDT_Byte:
            poQuery->set_buffer(
                osAttrName, reinterpret_cast<unsigned char*>( pImage ), nSize );
            break;
        case GDT_UInt16:
            poQuery->set_buffer(
                osAttrName, reinterpret_cast<unsigned short*>( pImage ), nSize );
            break;
        case GDT_UInt32:
            poQuery->set_buffer(
                osAttrName, reinterpret_cast<unsigned int*>( pImage ), nSize );
            break;
        case GDT_Int16:
            poQuery->set_buffer(
                osAttrName, reinterpret_cast<short*>( pImage ), nSize );
            break;
        case GDT_Int32:
            poQuery->set_buffer(
                osAttrName, reinterpret_cast<int*>( pImage ), nSize );
            break;
        case GDT_Float32:
            poQuery->set_buffer(
                osAttrName, reinterpret_cast<float*>( pImage ), nSize );
            break;
        case GDT_Float64:
            poQuery->set_buffer(
                osAttrName, reinterpret_cast<double*>( pImage ), nSize );
            break;
        case GDT_CInt16:
            poQuery->set_buffer(
                osAttrName, reinterpret_cast<short*>( pImage ), nSize * 2 );
            break;
        case GDT_CInt32:
            poQuery->set_buffer(
                osAttrName, reinterpret_cast<int*>( pImage ), nSize * 2 );
            break;
        case GDT_CFloat32:
            poQuery->set_buffer(
                osAttrName, reinterpret_cast<float*>( pImage ), nSize * 2 );
            break;
        case GDT_CFloat64:
            poQuery->set_buffer(
                osAttrName, reinterpret_cast<double*>( pImage ), nSize * 2 );
            break;
        default:
            return CE_Failure;
    }
    return CE_None;
}

/************************************************************************/
/*                               TileDBGroup                            */
/************************************************************************/

class TileDBGroup final: public GDALGroup
{
    std::map<CPLString, std::shared_ptr<GDALGroup>> m_oMapGroups{};
    std::map<CPLString, std::shared_ptr<GDALMDArray>> m_oMapMDArrays{};
    std::map<CPLString, std::shared_ptr<GDALAttribute>> m_oMapAttributes{};
    std::map<CPLString, std::shared_ptr<GDALDimension>> m_oMapDimensions{};

    std::shared_ptr<tiledb::Context> m_ctx;
    std::shared_ptr<tiledb::Array> m_array;
    char** papszOptions;
public:
    TileDBGroup(const std::string& osParentName, const std::string& osName, CSLConstList papszOptions);
    virtual ~TileDBGroup();

    std::vector<std::string> GetMDArrayNames(CSLConstList papszOptions) const override;
    std::shared_ptr<GDALMDArray> OpenMDArray(const std::string& osName,
                                             CSLConstList papszOptions) const override;

    std::vector<std::string> GetGroupNames(CSLConstList papszOptions) const override;
    std::shared_ptr<GDALGroup> OpenGroup(const std::string& osName,
                                         CSLConstList papszOptions) const override;

    std::shared_ptr<GDALGroup> CreateGroup(const std::string& osName,
                                           CSLConstList papszOptions) override;

    std::shared_ptr<GDALMDArray> CreateMDArray(const std::string& osArrayName,
                                                       const std::vector<std::shared_ptr<GDALDimension>>& aoDimensions,
                                                       const GDALExtendedDataType& oDataType,
                                                       CSLConstList papszOptions) override;
    std::shared_ptr<GDALDimension> CreateDimension(const std::string&,
                                                   const std::string&,
                                                   const std::string&,
                                                   GUInt64,
                                                   CSLConstList papszOptions) override;
    std::shared_ptr<GDALAttribute> CreateAttribute(const std::string& osName,
                                                    const std::vector<GUInt64>& anDimensions,
                                                    const GDALExtendedDataType& oDataType,
                                                    CSLConstList papszOptions) override;
};

/************************************************************************/
/*                         TileDBDimension                              */
/************************************************************************/

class TileDBDimension final: public GDALDimension
{
    std::weak_ptr<GDALMDArray> m_poIndexingVariable{};
public:
    TileDBDimension(const std::string& osParentName,
                 const std::string& osName,
                 const std::string& osType,
                 const std::string& osDirection,
                 GUInt64 nSize);

    std::shared_ptr<GDALMDArray> GetIndexingVariable() const override { return m_poIndexingVariable.lock(); }
};

/************************************************************************/
/*                               TileDBAttribute                        */
/************************************************************************/

class TileDBAttribute final: public GDALAttribute
{
    std::vector<std::shared_ptr<GDALDimension>> m_dims{};
    mutable std::unique_ptr<GDALExtendedDataType> m_dt;
    std::shared_ptr<tiledb::Context> m_ctx;
    std::shared_ptr<tiledb::Array> m_array;

protected:
    TileDBAttribute(std::shared_ptr<tiledb::Context> poCtx, 
                        std::shared_ptr<tiledb::Array> poArray,
                        const std::string& osParentName,
                        const std::string& osName,
                        const std::vector<GUInt64>& anDimensions,
                        const GDALExtendedDataType& oType);
    bool IRead(const GUInt64* arrayStartIdx,     // array of size GetDimensionCount()
                      const size_t* count,                 // array of size GetDimensionCount()
                      const GInt64* arrayStep,        // step in elements
                      const GPtrDiff_t* bufferStride, // stride in elements
                      const GDALExtendedDataType& bufferDataType,
                      void* pDstBuffer) const override;

    bool IWrite(const GUInt64* arrayStartIdx,     // array of size GetDimensionCount()
                      const size_t* count,                 // array of size GetDimensionCount()
                      const GInt64* arrayStep,        // step in elements
                      const GPtrDiff_t* bufferStride, // stride in elements
                      const GDALExtendedDataType& bufferDataType,
                      const void* pSrcBuffer) override;
public:
    static std::shared_ptr<TileDBAttribute> Create(
                                                std::shared_ptr<tiledb::Context> poContext,
                                                std::shared_ptr<tiledb::Array> poArray,
                                                const std::string& osParentName,
                                                const std::string& osName,
                                                const std::vector<GUInt64>& anDimensions,
                                                const GDALExtendedDataType& oDataType)
    {
        auto attr(std::shared_ptr<TileDBAttribute>(
            new TileDBAttribute(poContext, poArray, osParentName, osName, anDimensions, oDataType)));
        attr->SetSelf(attr);
        return attr;
    }

    static std::shared_ptr<TileDBAttribute> Create(const std::string& osParentName,
                const std::string& osName,
                const std::vector<GUInt64>& anDimensions,
                const GDALExtendedDataType& oDataType,
                CSLConstList papszOptions)
    {
        auto attr(std::shared_ptr<TileDBAttribute>(
            new TileDBAttribute(nullptr, nullptr, osParentName, osName, anDimensions, oDataType)));
        attr->SetSelf(attr);
        return attr;
    }

    const std::vector<std::shared_ptr<GDALDimension>>& GetDimensions() const override { return m_dims; }

    const GDALExtendedDataType &GetDataType() const override;
};

/************************************************************************/
/*                         TileDBVariable                               */
/************************************************************************/

class TileDBVariable final: public GDALMDArray
{
    mutable std::vector<std::shared_ptr<GDALDimension>> m_aoDims{};
    mutable std::unique_ptr<GDALExtendedDataType> m_dt;
    std::shared_ptr<tiledb::Context> m_ctx;
    std::shared_ptr<tiledb::Array> m_array;
    std::unique_ptr<tiledb::Query> m_query;
protected:
    TileDBVariable(std::shared_ptr<tiledb::Context> poCtx,
                   std::shared_ptr<tiledb::Array> poArray,
                   const CPLString& osGroupName,
                   const CPLString& osName,
                   const std::vector<std::shared_ptr<GDALDimension>>& dims,
                   CSLConstList papszOptions);

    bool IRead(const GUInt64* arrayStartIdx,     // array of size GetDimensionCount()
                      const size_t* count,                 // array of size GetDimensionCount()
                      const GInt64* arrayStep,        // step in elements
                      const GPtrDiff_t* bufferStride, // stride in elements
                      const GDALExtendedDataType& bufferDataType,
                      void* pDstBuffer) const override;

    bool IWrite(const GUInt64* arrayStartIdx,     // array of size GetDimensionCount()
                      const size_t* count,                 // array of size GetDimensionCount()
                      const GInt64* arrayStep,        // step in elements
                      const GPtrDiff_t* bufferStride, // stride in elements
                      const GDALExtendedDataType& bufferDataType,
                      const void* pSrcBuffer) override;

public:
    static std::shared_ptr<TileDBVariable> Create(
                   std::shared_ptr<tiledb::Context> poCtx,
                   std::shared_ptr<tiledb::Array> poArray,
                   const CPLString& osGroupName,
                   const CPLString& osName,
                   const std::vector<std::shared_ptr<GDALDimension>>& dims,
                   CSLConstList papszOptions)
    {
        auto var(std::shared_ptr<TileDBVariable>(
            new TileDBVariable( poCtx, poArray, osGroupName, osName, dims, papszOptions ) ) );
        var->SetSelf(var);
        return var;
    }

    bool IsWritable() const override { return true; }
    const std::vector<std::shared_ptr<GDALDimension>>& GetDimensions() const override;
    const GDALExtendedDataType &GetDataType() const override;
    bool SetSpatialRef(const OGRSpatialReference* poSRS) override;
    bool SetUnit(const std::string& osUnit) override;
    std::shared_ptr<GDALAttribute> CreateAttribute(
                                        const std::string& osName,
                                        const std::vector<GUInt64>& anDimensions,
                                        const GDALExtendedDataType& oDataType,
                                        CSLConstList papszOptions) override;
};

/************************************************************************/
/*                           TileDBGroup()                              */
/************************************************************************/

TileDBGroup::TileDBGroup(const std::string& osParentName, const std::string& osName,
                         CSLConstList papszOptions):
    GDALGroup(osParentName, osName.empty() ? osName : "")
{
    papszOptions = CSLDuplicate( papszOptions );
    const char* pszConfig = CSLFetchNameValue(
                                    papszOptions,
                                    "TILEDB_CONFIG" );

    // TODO - if the root group is open then copy context using shared_ptr
    if( pszConfig != nullptr )
    {
        tiledb::Config cfg( pszConfig );
        m_ctx = std::make_shared<tiledb::Context>( cfg );
    }
    else
        m_ctx = std::make_shared<tiledb::Context>( );

    tiledb::create_group( *m_ctx, osParentName );
}

/************************************************************************/
/*                           ~TileDBGroup()                             */
/************************************************************************/

TileDBGroup::~TileDBGroup()
{
    CSLDestroy( papszOptions );
}

/************************************************************************/
/*                           GetMDArrayNames()                          */
/************************************************************************/

std::vector<std::string> TileDBGroup::GetMDArrayNames(CSLConstList) const
{
  return std::vector<std::string>();
}

/************************************************************************/
/*                             OpenMDArray()                            */
/************************************************************************/

std::shared_ptr<GDALMDArray> TileDBGroup::OpenMDArray(const std::string& osName,
                                                   CSLConstList) const
{
  return nullptr;
}

/************************************************************************/
/*                            GetGroupNames()                           */
/************************************************************************/

std::vector<std::string> TileDBGroup::GetGroupNames(CSLConstList) const
{
    std::vector<std::string> names;
    for( const auto& iter: m_oMapGroups )
        names.push_back(iter.first);
    return names;
}

/************************************************************************/
/*                              OpenGroup()                             */
/************************************************************************/

std::shared_ptr<GDALGroup> TileDBGroup::OpenGroup(const std::string& osName,
                                               CSLConstList) const
{
    return nullptr;
}

/************************************************************************/
/*                             CreateGroup()                            */
/************************************************************************/

std::shared_ptr<GDALGroup> TileDBGroup::CreateGroup(const std::string& osName,
                                                 CSLConstList /*papszOptions*/)
{
    if( osName.empty() )
    {
        CPLError(CE_Failure, CPLE_NotSupported,
                 "Empty group name not supported");
        return nullptr;
    }
    if( m_oMapGroups.find(osName) != m_oMapGroups.end() )
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "A group with same name already exists");
        return nullptr;
    }
    auto newGroup(std::make_shared<TileDBGroup>( GetFullName(), osName.c_str(), papszOptions ));
    m_oMapGroups[osName] = newGroup;
    return newGroup;
}

/************************************************************************/
/*                            CreateMDArray()                           */
/************************************************************************/

std::shared_ptr<GDALMDArray> TileDBGroup::CreateMDArray(
    const std::string& osArrayName,
    const std::vector<std::shared_ptr<GDALDimension>>& aoDimensions,
    const GDALExtendedDataType& oType,
    CSLConstList papszOptions)
{
    if( osArrayName.empty() )
    {
        CPLError(CE_Failure, CPLE_NotSupported,
                 "Empty array name not supported");
        return nullptr;
    }

    if( m_oMapMDArrays.find(osArrayName) != m_oMapMDArrays.end() )
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "An array with same name already exists");
        return nullptr;
    }

    tiledb::Domain domain( *m_ctx );

    // create the array
    for (auto& dim : aoDimensions)
    {
        // TODO - get block size from dimension, get compression options
        GUInt64 blockSize = MIN(dim->GetSize() - 1, 1024); 
        auto d = tiledb::Dimension::create<uint64_t>(
                    *m_ctx, dim->GetName(), {0, dim->GetSize() - 1}, uint64_t( blockSize ) );
        domain.add_dimension( d );
    }

    tiledb::ArraySchema s( *m_ctx, TILEDB_DENSE );

    // TODO separate this into a common function with TileDBDataset and CO options
    switch (oType.GetNumericDataType())
    {
        case GDT_Byte:
        {
            s.add_attribute( tiledb::Attribute::create<unsigned char>( *m_ctx, TILEDB_VALUES ) );
            break;
        }
        case GDT_UInt16:
        {
            s.add_attribute( tiledb::Attribute::create<unsigned short>( *m_ctx, TILEDB_VALUES ) );
            break;
        }
        case GDT_UInt32:
        {
            s.add_attribute( tiledb::Attribute::create<unsigned int>( *m_ctx, TILEDB_VALUES ) );
            break;
        }
        case GDT_Int16:
        {
            s.add_attribute( tiledb::Attribute::create<short>( *m_ctx, TILEDB_VALUES ) );
            break;
        }
        case GDT_Int32:
        {
            s.add_attribute( tiledb::Attribute::create<int>( *m_ctx, TILEDB_VALUES ) );
            break;
        }
        case GDT_Float32:
        {
            s.add_attribute( tiledb::Attribute::create<float>( *m_ctx, TILEDB_VALUES ) );
            break;
        }
        case GDT_Float64:
        {
            s.add_attribute( tiledb::Attribute::create<double>( *m_ctx, TILEDB_VALUES ) );
            break;
        }
        case GDT_CInt16:
        {
            s.add_attribute( tiledb::Attribute::create<short[2]>( *m_ctx, TILEDB_VALUES ) );
            break;
        }
        case GDT_CInt32:
        {
            s.add_attribute( tiledb::Attribute::create<int[2]>( *m_ctx, TILEDB_VALUES ) );
            break;
        }
        case GDT_CFloat32:
        {
            s.add_attribute( tiledb::Attribute::create<float[2]>( *m_ctx, TILEDB_VALUES ) );
            break;
        }
        case GDT_CFloat64:
        {
            s.add_attribute( tiledb::Attribute::create<double[2]>( *m_ctx, TILEDB_VALUES ) );
            break;
        }
        default:
            return nullptr;
    }

    s.set_domain( domain ).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
    CPLString osFullArrayName = CPLString().Printf("%s/%s", m_osFullName.c_str(), osArrayName.c_str());
    tiledb::Array::create( osFullArrayName, s );

    m_array = std::make_shared<tiledb::Array>( *m_ctx, osFullArrayName, TILEDB_WRITE );

    auto newArray = TileDBVariable::Create(m_ctx, m_array, m_osName, osArrayName, aoDimensions, papszOptions );
    m_oMapMDArrays[osArrayName] = newArray;
    return newArray;
}

/************************************************************************/
/*                             CreateDimension()                        */
/************************************************************************/

std::shared_ptr<GDALDimension> TileDBGroup::CreateDimension(const std::string& osName,
                                                         const std::string& osType,
                                                         const std::string& osDirection,
                                                         GUInt64 nSize,
                                                         CSLConstList)
{
    if( osName.empty() )
    {
        CPLError(CE_Failure, CPLE_NotSupported,
                 "Empty dimension name not supported");
        return nullptr;
    }
    if( m_oMapDimensions.find(osName) != m_oMapDimensions.end() )
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "A dimension with same name already exists");
        return nullptr;
    }
    auto newDim(std::make_shared<TileDBDimension>(GetFullName(), osName, osType, osDirection, nSize));
    m_oMapDimensions[osName] = newDim;
    return newDim;
}

/************************************************************************/
/*                           CreateAttribute()                          */
/************************************************************************/

std::shared_ptr<GDALAttribute> TileDBGroup::CreateAttribute(
        const std::string& osName,
        const std::vector<GUInt64>& anDimensions,
        const GDALExtendedDataType& oDataType,
        CSLConstList papszOptions)
{
    if( osName.empty() )
    {
        CPLError(CE_Failure, CPLE_NotSupported,
                 "Empty attribute name not supported");
        return nullptr;
    }
    if( m_oMapAttributes.find(osName) != m_oMapAttributes.end() )
    {
        CPLError(CE_Failure, CPLE_AppDefined,
                 "An attribute with same name already exists");
        return nullptr;
    }
 
    auto newAttr(TileDBAttribute::Create(
        (GetFullName() == "/" ? "/" : GetFullName() + "/") + "_GLOBAL_",
        osName, anDimensions, oDataType, papszOptions));

    m_oMapAttributes[osName] = newAttr;
    return newAttr;
}

/************************************************************************/
/*                             TileDBDimension()                        */
/************************************************************************/

TileDBDimension::TileDBDimension(const std::string& osParentName,
                           const std::string& osName,
                           const std::string& osType,
                           const std::string& osDirection,
                           GUInt64 nSize):
    GDALDimension(osParentName, osName, osType, osDirection, nSize)
{
}

/************************************************************************/
/*                             TileDBAttribute()                        */
/************************************************************************/

TileDBAttribute::TileDBAttribute(
                           std::shared_ptr<tiledb::Context> poCtx, 
                           std::shared_ptr<tiledb::Array> poArray, 
                           const std::string& osParentName,
                           const std::string& osName,
                           const std::vector<GUInt64>& anDimensions,
                           const GDALExtendedDataType& oType):
    GDALAbstractMDArray(osParentName, osName),
    GDALAttribute(osParentName, osName)
{
    m_dt.reset(new GDALExtendedDataType(oType));
    m_ctx = poCtx;
    m_array = poArray;
}

/************************************************************************/
/*                                   IRead()                            */
/************************************************************************/

bool TileDBAttribute::IRead(const GUInt64* arrayStartIdx,
                               const size_t* count,
                               const GInt64* arrayStep,
                               const GPtrDiff_t* bufferStride,
                               const GDALExtendedDataType& bufferDataType,
                               void* pDstBuffer) const
{
    return false;
}

/************************************************************************/
/*                                IWrite()                              */
/************************************************************************/

bool TileDBAttribute::IWrite(const GUInt64* arrayStartIdx,
                               const size_t* count,
                               const GInt64* arrayStep,
                               const GPtrDiff_t* bufferStride,
                               const GDALExtendedDataType& bufferDataType,
                               const void* pSrcBuffer)
{
    // TileDB array/group level metadata
    if ( bufferDataType.GetClass() == GEDTC_STRING )
    {
        if ( m_array && m_dims.empty() )
        {
            const auto stringDT(GDALExtendedDataType::CreateString());
            char* pszStr = nullptr;
            GDALExtendedDataType::CopyValue(pSrcBuffer,
                                            bufferDataType,
                                            &pszStr,
                                            stringDT);    
            m_array->put_metadata(GetName(), TILEDB_UINT8, strlen(pszStr), pszStr);
            CPLFree( pszStr );
            return true;
        }
        else
        {
            fprintf(stderr, "CURRENTLY SKIPPING GROUP LEVEL METADATA\n");
            return false;
        }
    }
    else
    {
        return false;
    }
}

/************************************************************************/
/*                             GetDataType()                            */
/************************************************************************/

const GDALExtendedDataType &TileDBAttribute::GetDataType() const
{
    return *m_dt;
}

/************************************************************************/
/*                             GetDimensions()                          */
/************************************************************************/

const std::vector<std::shared_ptr<GDALDimension>>& TileDBVariable::GetDimensions() const
{
    return m_aoDims;
}

/************************************************************************/
/*                          TileDBVariable()                            */
/************************************************************************/

TileDBVariable::TileDBVariable(std::shared_ptr<tiledb::Context> poCtx,
                std::shared_ptr<tiledb::Array> poArray,
                const CPLString& osGroupName,
                const CPLString& osName,
                const std::vector<std::shared_ptr<GDALDimension>>& dims,
                CSLConstList papszOptions):
  GDALAbstractMDArray(osGroupName, osName),
  GDALMDArray(osGroupName, osName),
  m_aoDims(dims)
{
  m_ctx = poCtx;
  m_array = poArray;
  m_query.reset( new tiledb::Query( *m_ctx, *m_array ) );
}

/************************************************************************/
/*                                   IRead()                            */
/************************************************************************/

bool TileDBVariable::IRead(const GUInt64* arrayStartIdx,
                               const size_t* count,
                               const GInt64* arrayStep,
                               const GPtrDiff_t* bufferStride,
                               const GDALExtendedDataType& bufferDataType,
                               void* pDstBuffer) const
{
    return true;
}

/************************************************************************/
/*                                IWrite()                              */
/************************************************************************/

bool TileDBVariable::IWrite(const GUInt64* arrayStartIdx,
                               const size_t* count,
                               const GInt64* arrayStep,
                               const GPtrDiff_t* bufferStride,
                               const GDALExtendedDataType& bufferDataType,
                               const void* pSrcBuffer)
{
    // SetBuffer( m_query.get(), m_dt->GetNumericDataType(),
    //            TILEDB_VALUES, const_cast<void*>(pSrcBuffer), GetTotalElementsCount() );
    // auto status = m_query->submit();
    // if (status == tiledb::Query::Status::FAILED)
    //     return false;
    return true;
}

/************************************************************************/
/*                             GetDataType()                            */
/************************************************************************/

const GDALExtendedDataType &TileDBVariable::GetDataType() const
{
    if ( m_dt )
        return *m_dt;

    m_dt.reset(new GDALExtendedDataType(GDALExtendedDataType::Create(GDT_Byte)));
    return *m_dt;
}

bool TileDBVariable::SetSpatialRef(const OGRSpatialReference* poSRS)
{
    if ( m_array )
    {
        char *pszWKT = nullptr;
        poSRS->exportToWkt( &pszWKT );
        m_array->put_metadata("_srs", TILEDB_UINT8, strlen(pszWKT), pszWKT);
        CPLFree( pszWKT );
    }
    return true;
}

/************************************************************************/
/*                          CreateAttribute()                           */
/************************************************************************/

std::shared_ptr<GDALAttribute> TileDBVariable::CreateAttribute(
        const std::string& osName,
        const std::vector<GUInt64>& anDimensions,
        const GDALExtendedDataType& oDataType,
        CSLConstList papszOptions)
{
    return TileDBAttribute::Create(m_ctx, m_array, GetFullName(), osName,
                                    anDimensions, oDataType);
}

/************************************************************************/
/*                              SetUnit()                               */
/************************************************************************/

bool TileDBVariable::SetUnit(const std::string& osUnit)
{
    m_array->put_metadata("_unit", TILEDB_UINT8, osUnit.length() + 1, osUnit.c_str());
    return true;
}


/************************************************************************/
/*                     CreateMultiDimensional()                         */
/************************************************************************/

GDALDataset * TileDBDataset::CreateMultiDimensional( const char * pszFilename,
                                                     CSLConstList papszRootGroupOptions,
                                                     CSLConstList papszOptions )
{
    auto poDS = std::unique_ptr<TileDBDataset>( new TileDBDataset() );

    poDS->SetDescription(pszFilename);
    poDS->eAccess = GA_Update;

    // process options.
    poDS->papszCreationOptions = CSLDuplicate(papszOptions);

    // Create the root group for the dataset.
    if ( CPLIsFilenameRelative( pszFilename ) )
        poDS->osRootGroup = pszFilename;
    else
        poDS->osRootGroup = CPLGetBasename( pszFilename );

    poDS->m_poRootGroup.reset( new TileDBGroup( poDS->osRootGroup, poDS->osRootGroup, papszRootGroupOptions ) );

    return poDS.release();
}

/************************************************************************/
/*                          GetRootGroup()                              */
/************************************************************************/

std::shared_ptr<GDALGroup> TileDBDataset::GetRootGroup() const
{
    return m_poRootGroup;
}

#endif
