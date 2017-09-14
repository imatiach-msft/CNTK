//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//

#include "stdafx.h"
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <iostream>

#include "Basics.h"
#include "StringUtil.h"

#include "DataFrameDeserializer.h"
#include "DataFrameConfigHelper.h"
#include "Common/Interfaces.h"
#include "Common/TabularChunk.h"

namespace Microsoft { namespace MSR { namespace CNTK { namespace DF {

using namespace std;

static ElementType ResolveTargetType(std::wstring& confval)
{
    if (AreEqualIgnoreCase(confval, L"double"))
    {
        return ElementType::tdouble;
    }
    else if (AreEqualIgnoreCase(confval, L"float"))
    {
        return ElementType::tfloat;
    }
    else
    {
        InvalidArgument("DataFrameDeserializer doesn't support target type %ls, please change your configuration.", confval.c_str());
    }
}

bool DataFrameDeserializer::GetSequenceDescription(const SequenceDescription& primary, SequenceDescription&)
{
    NOT_IMPLEMENTED;
}


void DataFrameDeserializer::GetSequencesForChunk(ChunkIdType chunkId, std::vector<SequenceDescription>& result)
{
    // nested forloop
    size_t numRowsBeforeChunk = m_rowStartIdxes[chunkId];
    size_t numChunks = m_metadata -> NumberOfRowChunks();

    std::cout << "IN GETSEQUENCESFORCHUNK chunkId: " << chunkId << " starting idx:" << numRowsBeforeChunk << std::endl; 
    /*
    for (unsigned int rg = 0; rg < numChunks; ++rg)
    {
        size_t numRows = m_metadata -> NumberOfRowsInChunk(rg);
        for (size_t row = 0; row < numRows; ++row)
        {
            size_t key = row + numRowsBeforeChunk;
            result.push_back(SequenceDescription {row, 1, rg, KeyType(key, key)});
        }
        numRowsBeforeChunk += numRows;
    }
    */
     size_t numRows = m_metadata -> NumberOfRowsInChunk(chunkId);
     for (size_t row = 0; row < numRows; ++row)
     {
        size_t key = row + numRowsBeforeChunk;
        result.push_back(SequenceDescription {row, 1, chunkId, KeyType(key, key)});
     }
}

DataFrameDeserializer::DataFrameDeserializer(const ConfigParameters& cfg, bool primary) : DataDeserializerBase(primary)
{
    m_logVerbosity = cfg(L"verbosity", 2);
    std::wstring precision = cfg(L"precision", L"double");

    ConfigParameters input = cfg(L"input");
    auto inputName = input.GetMemberIds().front();
    ConfigParameters streamConfig = input(inputName); // TODO: equivalent to cfg(L"input")?

    DataFrameConfigHelper config(streamConfig);

    m_featureDim = config.GetInputDimension(InputType::Features);
    m_labelDim = config.GetInputDimension(InputType::Labels);

    m_precision = ResolveTargetType(precision);

    auto fileProvider = InitializeProvider(config.GetDataSource(), streamConfig);
    printf("Finished initializing HDFSArrrowReader, now initializing ParquetReader\n");
    m_fileReader = InitializeReader(config.GetFormat(), streamConfig);
    printf("Initialized ParquetReader!\n");

    // provider -> List<RandomAccessSource>
    auto filelist = fileProvider -> GetFileList();
    // reader(List<RAS>) -> Metadata
    printf("RETRIEVED the file lists!\n");
    m_metadata = m_fileReader -> InitializeSources(filelist);
    std::cout << "NumRowChunks: " << m_metadata -> NumberOfRowChunks() << std::endl;
    size_t rowStartIdx = 0;
    for (int j = 0; j < m_metadata -> NumberOfRowChunks(); j++) 
    {
        std::cout << "NumRowsInChunk: " << m_metadata ->NumberOfRowsInChunk(j) << "  " << j << std::endl;
        m_rowStartIdxes.push_back(rowStartIdx);
        rowStartIdx += m_metadata ->NumberOfRowsInChunk(j);
    }

    printf("INITIALIZING STREAMS!\n");
    InitializeStreams();
    printf("INITIALIZED STREAMS!!!!!!\n");
}

std::shared_ptr<FileProvider> DataFrameDeserializer::InitializeProvider(
    DataSource source,
    const ConfigParameters& dfdParams) 
{
    if (source == DataSource::HDFS)
    {
        printf("printing dfdparams\n");
        dfdParams.dump();
        ConfigParameters hdfs = dfdParams(L"hdfs");
        printf("printing hdfsparams\n");
        hdfs.dump();
        printf("end printing initialize\n");
        // TODO: In the future, we should support other FileProviders
        std::shared_ptr<FileProvider> reader(new HDFSArrowReader(hdfs));
        return reader;
    }
    else
    {
        InvalidArgument("Unsupported source\n");
    }
    return nullptr;
}

std::shared_ptr<FileReader> DataFrameDeserializer::InitializeReader(
    FileFormat format,
    const ConfigParameters& dfdParams) 
{
    if (format == FileFormat::Parquet)
    {
        std::shared_ptr<FileReader> pqReader(new ParquetReader(dfdParams));
        return pqReader;
    }
    else
    {
        InvalidArgument("Unsupported format\n");
    }
    return nullptr;
}


static SmallVector<size_t> GetDimensionality(const std::shared_ptr<DataType>& dt)
{
    // TODO: More complicated logic here for vector elements
    return SmallVector<size_t>(1, 1);
}

static StorageType GetDensity(const std::shared_ptr<DataType>& dt)
{
    return StorageType::dense;
}

void DataFrameDeserializer::InitializeStreams()
{
    /* // Potentially move to stream/col?
    int cols = metadata->NumberOfColumns();
    for (int i = 0; i < cols, ++i)
    {
        auto type = metadata->GetColumnType(i);

        StreamDescriptionPtr stream = make_shared<StreamDescription>();
        stream->m_id = i;
        stream->m_name = metadata->GetColumnName(i);
        stream->m_sampleLayout = make_shared<TensorShape>(GetDimensionality(type));
        stream->m_elementType = m_targetElementType;
        stream->m_storageType = GetDensity(type);
        
        m_streams.push_back(stream);
    }
    */
    // size_t featureDim;
    StreamDescriptionPtr featureStream = make_shared<StreamDescription>();
    featureStream->m_id = 0;
    featureStream->m_name = L"features";
    featureStream->m_sampleLayout = make_shared<TensorShape>(m_featureDim); // This should have the shape matching the dimensions of the features column
    featureStream->m_elementType = m_precision;
    featureStream->m_storageType = StorageType::dense;
    m_streams.push_back(featureStream);

    // size_t labelDim;
    StreamDescriptionPtr labelStream = make_shared<StreamDescription>();
    labelStream->m_id = 1;
    labelStream->m_name = L"labels";
    labelStream->m_sampleLayout = make_shared<TensorShape>(m_labelDim); // This should have the shape matching the dimensions of the labels column
    labelStream->m_elementType = m_precision;
    labelStream->m_storageType = StorageType::dense;
    m_streams.push_back(labelStream);
}

// Gets information about available chunks.
ChunkDescriptions DataFrameDeserializer::GetChunkDescriptions()
{
    std::cout << "IN DFDS::GETCHUNKDESCRIPTIONS :D" << std::endl;

    ChunkDescriptions chunks;
    auto nchunks = m_metadata -> NumberOfRowChunks(); // nchunks == numberOfRowGroups
    
    std::cout << "Reserving nchunks: " << nchunks << std::endl;
    chunks.reserve(nchunks);

    for (ChunkIdType i = 0; i < nchunks; ++i)
    {
        auto nrowsInChunk = m_metadata -> NumberOfRowsInChunk(i);
        std::cout << "nrowsInChunk: " << nrowsInChunk << std::endl;
        std::shared_ptr<ChunkDescription>cd (new ChunkDescription());
        cd->m_id = i;
        cd->m_numberOfSamples = nrowsInChunk;
        // Figure out rowname for Sequence != Sample
        cd->m_numberOfSequences = nrowsInChunk;
        chunks.push_back(cd);
    }
    return chunks;
}


// Gets a data chunk with the specified chunk id. In a Parquet file, this represents the RowGroup number.
ChunkPtr DataFrameDeserializer::GetChunk(ChunkIdType chunkId)
{
    // A CNTKChunk maps to a rowGroup, so the shape of the chunk should match the # rows and # cols in each rowGroup
    std::cout << "IN DFDS::GETCHUNK with chunkID: " << chunkId << std::endl;
    auto c = m_fileReader->GetChunk(chunkId);
    // std::cout << "GOT CHUNK, NOW RETURNING A SHARED TABULAR CHUNK" << std::endl;
    // auto layout = make_shared<TensorShape>(1); // TODO: replace this with actual dimension of the column
    if (m_precision == ElementType::tfloat)
    {
        shared_ptr<TabularChunk<float>> tc (new TabularChunk<float>(c, m_featureDim, m_labelDim, m_precision, m_rowStartIdxes[chunkId]));
        return tc;
    }
    shared_ptr<TabularChunk<double>> tc (new TabularChunk<double>(c, m_featureDim, m_labelDim, m_precision, m_rowStartIdxes[chunkId]));
    return tc;
/* 
    shared_ptr<TabularChunk> tc (new TabularChunk(c, m_featureDim, m_labelDim, m_precision));
    std::cout << "CONSTRUCTED A TC for chunk id: " << chunkId << std::endl;
    return tc;
*/
};

}}}}
