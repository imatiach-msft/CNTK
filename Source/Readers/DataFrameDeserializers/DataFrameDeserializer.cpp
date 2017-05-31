//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//

#include "stdafx.h"
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
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
    size_t numRowsBeforeChunk = 0;
    size_t numChunks = m_metadata -> NumberOfRowChunks();
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
}

DataFrameDeserializer::DataFrameDeserializer(const ConfigParameters& cfg, bool primary) : DataDeserializerBase(primary)
{
    m_logVerbosity = cfg(L"verbosity", 2);
    std::wstring precision = cfg(L"precision", L"double");

    ConfigParameters input = cfg(L"input");
    auto inputName = input.GetMemberIds().front();
    ConfigParameters streamConfig = input(inputName);

    DataFrameConfigHelper config(streamConfig);

    m_targetElementType = ResolveTargetType(precision);

    auto fileProvider = InitializeProvider(config.GetDataSource(), streamConfig);
    m_fileReader = InitializeReader(config.GetFormat(), streamConfig);

    // provider -> List<RandomAccessSource>
    auto filelist = fileProvider -> GetFileList();
    // reader(List<RAS>) -> Metadata
    m_metadata = m_fileReader -> InitializeSources(filelist);

    InitializeStreams();
}

std::shared_ptr<FileProvider> DataFrameDeserializer::InitializeProvider(
    DataSource source,
    const ConfigParameters& dfdParams) 
{
    if (source == DataSource::HDFS)
    {
        //ConfigParameters hdfs = dfdParams(L"hdfs");
        //Instantiate the right things
        std::shared_ptr<FileProvider> reader(new HDFSArrowReader(dfdParams));
        return reader;
    }
    else
    {
        InvalidArgument("Unsupported source %d", source);
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
        InvalidArgument("Unsupported format %d", format);
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

// Describes exposed stream - a single stream of htk features.
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
    StreamDescriptionPtr stream = make_shared<StreamDescription>();
    stream->m_id = 0;
    stream->m_name = L"DFDStream";
    stream->m_sampleLayout = make_shared<TensorShape>(1, m_metadata->NumberOfColumns());
    stream->m_elementType = m_targetElementType;
    stream->m_storageType = StorageType::dense;
    
    m_streams.push_back(stream);
}

// Gets information about available chunks.
ChunkDescriptions DataFrameDeserializer::GetChunkDescriptions()
{
    ChunkDescriptions chunks;
    auto nchunks = m_metadata -> NumberOfRowChunks();
    chunks.reserve(nchunks);

    for (ChunkIdType i = 0; i < nchunks; ++i)
    {
        auto nrow = m_metadata -> NumberOfRowsInChunk(i);
        
        auto cd = make_shared<ChunkDescription>();
        cd->m_id = i;
        cd->m_numberOfSamples = nrow;
        // Figure out rowname for Sequence != Sample
        cd->m_numberOfSequences = nrow;
        chunks.push_back(cd);
    }
    return chunks;
}


// Gets a data chunk with the specified chunk id.
ChunkPtr DataFrameDeserializer::GetChunk(ChunkIdType chunkId)
{
    auto c = m_fileReader->GetChunk(chunkId);
    return make_shared<TabularChunk>(c, m_layout);
};

}}}}
