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

namespace CNTK { namespace DF {

using namespace std;

/**
bool DataFrameDeserializer::GetSequenceDescription(const SequenceDescription& primary, SequenceDescription&)
{
    NOT_IMPLEMENTED;
}
**/

void DataFrameDeserializer::SequenceInfosForChunk(ChunkIdType chunkId, std::vector<SequenceInfo>& result)
{
    // nested forloop
    size_t numRowsBeforeChunk = m_rowStartIdxes[chunkId];
    size_t numChunks = m_metadata -> NumberOfRowChunks();

    // std::cout << "IN GETSEQUENCESFORCHUNK chunkId: " << chunkId << " starting idx:" << numRowsBeforeChunk << std::endl;    
    size_t numRows = m_metadata -> NumberOfRowsInChunk(chunkId);
    for (size_t row = 0; row < numRows; ++row)
    {
         size_t key = row + numRowsBeforeChunk;
         result.push_back(SequenceInfo {row, 1, chunkId, SequenceKey(key, key)});
    }
}

DataFrameDeserializer::DataFrameDeserializer(const ConfigParameters& cfg, bool primary) : DataDeserializerBase(primary)
{
    // printf("In DataFrameDeserializer\n");
    m_logVerbosity = cfg(L"verbosity", 2);
    std::wstring precision = cfg(L"precision", L"double");
    // std::wcout << "the cfg precision is " << precision << std::endl;
    ConfigParameters input = cfg(L"input");
    auto inputName = input.GetMemberIds().front();
    ConfigParameters streamConfig = input(inputName); // TODO: equivalent to cfg(L"input")?

    DataFrameConfigHelper config(streamConfig);

    m_featureDim = config.GetInputDimension(InputType::Features);
    m_labelDim = config.GetInputDimension(InputType::Labels);
    // std::cout << "Is feature sparse? " << config.IsInputSparse(InputType::Features) << std::endl;
    // std::cout << "Is labels sparse? " << config.IsInputSparse(InputType::Labels) << std::endl;
    m_featureStorageType =  config.GetInputStorageType(InputType::Features);
    m_labelStorageType = config.GetInputStorageType(InputType::Labels);
    m_precision = config.ResolveTargetType(precision);

    auto fileProvider = InitializeProvider(config.GetDataSource(), streamConfig);
    // printf("Finished initializing HDFSArrrowReader, now initializing ParquetReader\n");
    streamConfig.Insert("precision", cfg("precision"));
    m_fileReader = InitializeReader(config.GetFormat(), streamConfig);
    // printf("Initialized ParquetReader!\n");

    // provider -> List<RandomAccessSource>
    auto filelist = fileProvider -> GetFileList();
    // reader(List<RAS>) -> Metadata
    // printf("RETRIEVED the file lists!\n");
    m_metadata = m_fileReader -> InitializeSources(filelist);
    // std::cout << "NumRowChunks: " << m_metadata -> NumberOfRowChunks() << std::endl;
    size_t rowStartIdx = 0;
    for (int j = 0; j < m_metadata -> NumberOfRowChunks(); j++) 
    {
        // std::cout << "NumRowsInChunk: " << m_metadata ->NumberOfRowsInChunk(j) << "  " << j << std::endl;
        m_rowStartIdxes.push_back(rowStartIdx);
        rowStartIdx += m_metadata ->NumberOfRowsInChunk(j);
    }

    // printf("INITIALIZING STREAMS!\n");
    InitializeStreams();
    // printf("INITIALIZED STREAMS!!!!!!\n");
}

std::shared_ptr<FileProvider> DataFrameDeserializer::InitializeProvider(
    DataSource source,
    const ConfigParameters& dfdParams) 
{
    if (source == DataSource::HDFS)
    {
        // printf("printing dfdparams\n");
        // dfdParams.dump();
        ConfigParameters hdfs = dfdParams(L"hdfs");
        // printf("printing hdfsparams\n");
        // hdfs.dump();
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

/**
static SmallVector<size_t> GetDimensionality(const std::shared_ptr<DataType>& dt)
{
    // TODO: More complicated logic here for vector elements
    return SmallVector<size_t>(1, 1);
}
**/

/**
static StorageFormat GetDensity(const std::shared_ptr<DataType>& dt)
{
    return StorageFormat::Dense;
}
**/

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
    StreamInformation featureStream;
    featureStream.m_id = 0;
    featureStream.m_name = L"features";
    featureStream.m_sampleLayout = NDShape(1, m_featureDim); // This should have the shape matching the dimensions of the features column
    featureStream.m_elementType = m_precision;
    featureStream.m_storageFormat = m_featureStorageType;
    featureStream. m_definesMbSize = true;
    m_streams.push_back(featureStream);

    // size_t labelDim;
    StreamInformation labelStream;
    labelStream.m_id = 1;
    labelStream.m_name = L"labels";
    labelStream.m_sampleLayout = NDShape(1, m_labelDim); // This should have the shape matching the dimensions of the labels column
    labelStream.m_elementType = m_precision;
    labelStream.m_storageFormat = m_labelStorageType;
    labelStream.m_definesMbSize = true;
    m_streams.push_back(labelStream);
}

// Gets information about available chunks.
std::vector<ChunkInfo> DataFrameDeserializer::ChunkInfos()
{
    // std::cout << "IN DFDS::GETCHUNKDESCRIPTIONS :D" << std::endl;

    std::vector<ChunkInfo> chunks;
    auto nchunks = m_metadata -> NumberOfRowChunks(); // nchunks == numberOfRowGroups
    
    chunks.reserve(nchunks);

    for (ChunkIdType i = 0; i < nchunks; ++i)
    {
        auto nrowsInChunk = m_metadata -> NumberOfRowsInChunk(i);
        // std::cout << "nrowsInChunk: " << nrowsInChunk << std::endl;
        chunks.push_back(ChunkInfo {i, nrowsInChunk, nrowsInChunk});
    }
    return chunks;
}


// Gets a data chunk with the specified chunk id. In a Parquet file, this represents the RowGroup number.
ChunkPtr DataFrameDeserializer::GetChunk(ChunkIdType chunkId)
{
    // A CNTKChunk maps to a rowGroup, so the shape of the chunk should match the # rows and # cols in each rowGroup
    auto c = m_fileReader->GetChunkBuffer(chunkId);
    // std::cout << "GOT CHUNK, NOW RETURNING A SHARED TABULAR CHUNK" << std::endl;
    if (m_precision == DataType::Float)
    {
        shared_ptr<TabularChunk<float>> tc (new TabularChunk<float>(c, m_precision, m_rowStartIdxes[chunkId]));
        return tc;
    }
    shared_ptr<TabularChunk<double>> tc (new TabularChunk<double>(c, m_precision, m_rowStartIdxes[chunkId]));
    return tc;
};

}}
