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

/************************************************************************/
/* ==================================================================== */
/*                            OGRTileDBDataset                          */
/* ==================================================================== */
/************************************************************************/

/************************************************************************/
/*                         OGRTileDBDataset()                           */
/************************************************************************/

OGRTileDBDataset::OGRTileDBDataset()
{
}

/************************************************************************/
/*                         ~OGRTileDBDataset()                          */
/************************************************************************/

OGRTileDBDataset::~OGRTileDBDataset()
{
}

/************************************************************************/
/*                                Open()                                */
/************************************************************************/

GDALDataset *OGRTileDBDataset::Open(CPL_UNUSED GDALOpenInfo *poOpenInfo)

{
#if 0
    const char *pszConfig =
        CSLFetchNameValue(poOpenInfo->papszOpenOptions, "TILEDB_CONFIG");

    const char *pszTimestamp =
        CSLFetchNameValue(poOpenInfo->papszOpenOptions, "TILEDB_TIMESTAMP");
#endif
    return nullptr;
}

/************************************************************************/
/*                           TestCapability()                           */
/************************************************************************/

int OGRTileDBDataset::TestCapability(CPL_UNUSED const char *pszCap)

{
    return TRUE;
}

/***********************************************************************/
/*                            ExecuteSQL()                             */
/***********************************************************************/

OGRLayer *OGRTileDBDataset::ExecuteSQL(CPL_UNUSED const char *pszSQLCommand,
                                       CPL_UNUSED OGRGeometry *poSpatialFilter,
                                       CPL_UNUSED const char *pszDialect)
{
    return nullptr;
}

/***********************************************************************/
/*                           ICreateLayer()                        */
/***********************************************************************/
OGRLayer *
OGRTileDBDataset::ICreateLayer(CPL_UNUSED const char *pszName,
                               CPL_UNUSED OGRSpatialReference *poSpatialRef,
                               OGRwkbGeometryType eGType, char **papszOptions)
{
    // TODO check access mode
    const char *pszBounds = CSLFetchNameValue(papszOptions, "bounds");

    // TODO add compression filters for attributes as per dense arrays, as well as timestamps for opening the dataset
    auto poLayer = new OGRTileDBLayer(this->GetDescription(), eGType);
    poLayer->m_ctx.reset(new tiledb::Context(*m_ctx));

    if (pszBounds)
    {
        CPLStringList aosBounds(CSLTokenizeString2(pszBounds, ",", 0));
        if (aosBounds.Count() == 4)
        {
            poLayer->nXStart = CPLAtoGIntBig(aosBounds[0]);
            poLayer->nYStart = CPLAtoGIntBig(aosBounds[1]);
            poLayer->nXEnd = CPLAtoGIntBig(aosBounds[2]);
            poLayer->nYEnd = CPLAtoGIntBig(aosBounds[3]);
        }
        else
        {
            CPLError(CE_Failure, CPLE_NoWriteAccess,
                     "Domain bounds specified as minx,miny,maxx,maxy are "
                     "required for array creation.\n");
            return nullptr;
        }
    }
    else
    {
        CPLError(CE_Failure, CPLE_NoWriteAccess,
                 "Domain bounds are required for the array.\n");
        return nullptr;
    }

    return poLayer;
}

/************************************************************************/
/*                          Create()                                    */
/************************************************************************/

GDALDataset *OGRTileDBDataset::Create(const char *pszFilename,
                                      CSLConstList papszOptions)
{
    auto poDS = new OGRTileDBDataset();
    poDS->SetDescription(pszFilename);

    const char *pszConfig = CSLFetchNameValue(papszOptions, "TILEDB_CONFIG");
    if (pszConfig != nullptr)
    {
        tiledb::Config cfg(pszConfig);
        poDS->m_ctx.reset(new tiledb::Context(cfg));
    }
    else
    {
        poDS->m_ctx.reset(new tiledb::Context());
    }
    return poDS;
}

/************************************************************************/
/* ==================================================================== */
/*                            OGRTileDBLayer                            */
/* ==================================================================== */
/************************************************************************/

/************************************************************************/
/*                         OGRTileDBLayer()                             */
/************************************************************************/

OGRTileDBLayer::OGRTileDBLayer(const char *pszFilename,
                               const OGRwkbGeometryType eGType)
    : poFeatureDefn(new OGRFeatureDefn(pszFilename))
{
    this->poFeatureDefn->SetGeomType(eGType);
    this->poFeatureDefn->Reference();
    //OGRGeomFieldDefn *poGeomDefn = this->poFeatureDefn->GetGeomFieldDefn(0);
    // this can be configurable
    this->nBufferCount = 10000;

    SetDescription(pszFilename);
}

/************************************************************************/
/*                         ~OGRTileDBLayer()                            */
/************************************************************************/

OGRTileDBLayer::~OGRTileDBLayer()
{
    try
    {
        if (this->poFeatureDefn)
            this->poFeatureDefn->Release();

        const double nPadX =
            (this->sMaxEnvelope.MaxX - this->sMaxEnvelope.MinX) / 2.0;
        const double nPadY =
            (this->sMaxEnvelope.MaxY - this->sMaxEnvelope.MinY) / 2.0;

        // write the pad metadata
        this->m_array->put_metadata("PAD_X", TILEDB_FLOAT64, 1, &nPadX);
        this->m_array->put_metadata("PAD_Y", TILEDB_FLOAT64, 1, &nPadY);

        if (m_array)
        {
            m_array->close();
        }
    }
    catch (const tiledb::TileDBError &e)
    {
        CPLError(CE_Failure, CPLE_AppDefined, "%s", e.what());
    }
}

/************************************************************************/
/*                         GetNextFeature()                             */
/************************************************************************/

OGRFeature *OGRTileDBLayer::GetNextFeature()
{
    return nullptr;
}

/************************************************************************/
/*                         ResetReading()                               */
/************************************************************************/

void OGRTileDBLayer::ResetReading()
{
}

/************************************************************************/
/*                         CreateField()                                */
/************************************************************************/

OGRErr OGRTileDBLayer::CreateField(OGRFieldDefn *poField, int /* bApproxOK*/)
{
    OGRFieldDefn oFieldDefn(poField);
    poFeatureDefn->AddFieldDefn(&oFieldDefn);
    return OGRERR_NONE;
}

/************************************************************************/
/*                         CreateGeomField()                            */
/************************************************************************/

OGRErr OGRTileDBLayer::CreateGeomField(OGRGeomFieldDefn *poField,
                                       int /* bApproxOK*/)
{
    poFeatureDefn->AddGeomFieldDefn(std::move(poField));
    return OGRERR_NONE;
}

/************************************************************************/
/*                         ICreateFeature()                             */
/************************************************************************/

OGRErr OGRTileDBLayer::ICreateFeature(OGRFeature *poFeature)
{
    if (!bInitialized)
    {
        // create the tiledb schema
        // dimensions will be _x and _y, we can also add _z (2.5d)
        // filters should be implemented as per tiledbdense
        // set dimensions and attribute type for schema
        // we will use row-major for now but we could use hilbert indexing
        m_schema.reset(new tiledb::ArraySchema(*m_ctx, TILEDB_SPARSE));
        m_schema->set_tile_order(TILEDB_ROW_MAJOR);
        m_schema->set_cell_order(TILEDB_ROW_MAJOR);

        tiledb::Domain domain(*m_ctx);

        // hard code block size but this is a creation option in dense arrays
        auto d1 = tiledb::Dimension::create<uint64_t>(
            *m_ctx, "_X", {nXStart, nXEnd}, uint64_t(10000));
        auto d2 = tiledb::Dimension::create<uint64_t>(
            *m_ctx, "_Y", {nYStart, nYEnd}, uint64_t(10000));

        domain.add_dimensions(d1, d2);

        m_schema->set_domain(domain);

        for (int i = 0; i < poFeature->GetFieldCount(); i++)
        {
            OGRFieldDefn *poFieldDefn = poFeature->GetFieldDefnRef(i);
            CPLString osName(poFieldDefn->GetNameRef());
            // prototype, so we will support a subset of data types in the nz building dataset, just name for now

            switch (poFieldDefn->GetType())
            {
                case OFTInteger:
                {
                    //     m_schema->add_attribute(
                    //         tiledb::Attribute::create<unsigned int>(*m_ctx, poFieldDefn->GetNameRef()));
                    break;
                }
                case OFTString:
                {
                    const char *pszName = poFieldDefn->GetNameRef();
                    if (EQUALN(pszName, "name", 5))
                    {
                        m_schema->add_attribute(
                            tiledb::Attribute::create<std::string>(
                                *m_ctx, poFieldDefn->GetNameRef()));
                    }
                    break;
                }
                case OFTDate:
                {
                    //     m_schema->add_attribute(
                    //         tiledb::Attribute::create<double>(*m_ctx, poFieldDefn->GetNameRef()));
                    break;
                }
                default:
                {
                    CPLError(CE_Failure, CPLE_NoWriteAccess,
                             "Unsupported attribute definition.\n");
                    return OGRERR_FAILURE;
                }
            }
        }

        // add geometry attribute
        m_schema->add_attribute(
            tiledb::Attribute::create<std::vector<unsigned char>>(*m_ctx,
                                                                  "geometry"));

        const char *pszArrayName = GetDescription();
        tiledb::Array::create(pszArrayName, *m_schema);
        this->m_array.reset(
            new tiledb::Array(*this->m_ctx, pszArrayName, TILEDB_WRITE));

        this->aGeometryOffsets.push_back(0);
        this->aNameOffsets.push_back(0);

        bInitialized = true;
    }

    // could use tiledb arrow interface here, but proving out algorithm for now
    // buffer features
    const char *pszName =
        poFeature->GetFieldAsString(poFeature->GetFieldIndex("name"));
    this->aNames.push_back(pszName);
    this->aNameOffsets.push_back(
        this->aNameOffsets[this->aNameOffsets.size() - 1] + strlen(pszName));

    OGRGeometry *poGeom = poFeature->GetGeometryRef();
    const size_t nSize = poGeom->WkbSize();
    std::vector<unsigned char> aGeometry(nSize);
    poGeom->exportToWkb(wkbNDR, aGeometry.data());
    this->aGeometries.push_back(aGeometry);
    this->aGeometryOffsets.push_back(
        this->aGeometryOffsets[this->aGeometryOffsets.size() - 1] + nSize);

    OGREnvelope sEnvelope;
    poGeom->getEnvelope(&sEnvelope);

    // use mid point of envelope
    this->aXs.push_back(sEnvelope.MinX +
                        ((sEnvelope.MaxX - sEnvelope.MinX) / 2.0));
    this->aYs.push_back(sEnvelope.MinY +
                        ((sEnvelope.MaxY - sEnvelope.MinY) / 2.0));

    if (this->sMaxEnvelope.IsInit())
    {
        this->sMaxEnvelope = sEnvelope;
    }
    else
    {
        if ((sEnvelope.MaxX - sEnvelope.MinX) >
                (this->sMaxEnvelope.MaxX - this->sMaxEnvelope.MinX) &&
            (sEnvelope.MaxY - sEnvelope.MinY) >
                (this->sMaxEnvelope.MaxY - this->sMaxEnvelope.MinY))
        {
            this->sMaxEnvelope = sEnvelope;
        }
    }

    if (this->aNames.size() >= this->nBufferCount)
    {
        this->aGeometryOffsets.pop_back();
        this->aNameOffsets.pop_back();

        tiledb::Query query(*this->m_ctx, *this->m_array);
        query.set_buffer("_X", this->aXs);
        query.set_buffer("_Y", this->aYs);
        query.set_buffer("name", this->aNameOffsets, this->aNames);
        query.set_buffer("geometry", this->aGeometryOffsets, this->aGeometries);
        query.submit();

        this->aGeometries.clear();
        this->aNames.clear();
        this->aGeometryOffsets.resize(1);
        this->aGeometryOffsets[0] = 0;
        this->aNameOffsets.resize(1);
        this->aNameOffsets[0] = 0;
    }

    return OGRERR_NONE;
}

/************************************************************************/
/*                         TestCapability()                             */
/************************************************************************/

int OGRTileDBLayer::TestCapability(const char * /*pszCap*/)
{
    // override in dev as true for all
    return TRUE;
}
