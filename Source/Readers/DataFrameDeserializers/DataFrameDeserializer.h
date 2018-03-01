//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//

#pragma once

#include "DataDeserializerBase.h"
#include "Config.h"

#include "Common/Constants.h"
#include "HDFS/HDFSArrowReader.h"
#include "Parquet/ParquetReader.h"

namespace CNTK { namespace DF {

class TableMetadata;
class FileProvider;
class FileReader;

// "DataFrame" deserializer, orchestrator to initialize proper components
// Provides a set of chunks/sequences to the upper layers.
class DataFrameDeserializer : public DataDeserializerBase
{
public:
    // Expects new configuration.
    DataFrameDeserializer(const ConfigParameters& config, bool primary);

    // TODO: Should be removed, when legacy config goes away, expects configuration in a legacy mode.
    //DataFrameDeserializer(CorpusDescriptorPtr corpus, const ConfigParameters& featureConfig, const std::wstring& featureName, bool primary);

    // Get information about chunks.
    virtual std::vector<ChunkInfo> ChunkInfos() override;

    // Get information about particular chunk.
    virtual void  SequenceInfosForChunk(ChunkIdType chunkId, std::vector<SequenceInfo>& result) override;

    // Retrieves data for a chunk.
    virtual ChunkPtr GetChunk(ChunkIdType chunkId) override;

    // Gets sequence description by the primary one.
    // virtual bool GetSequenceDescription(const SequenceDescription& primary, SequenceDescription&) override;

private:
    DISABLE_COPY_AND_MOVE(DataFrameDeserializer);

    // Initialization functions.
    std::shared_ptr<FileProvider> InitializeProvider(DataSource source, const ConfigParameters& config);
    std::shared_ptr<FileReader> InitializeReader(FileFormat format, const ConfigParameters& config);

    void InitializeStreams();

    // Gets sequence by its chunk id and id inside the chunk.
    // TODO have client implement vector + allocator for return bucket
    // void GetSequenceById(ChunkIdType chunkId, size_t id, std::vector<SequenceDataPtr>& returnData);


    // Type of the features.
    DataType m_precision;

    std::shared_ptr<TableMetadata> m_metadata;

    std::shared_ptr<FileReader> m_fileReader;

    NDShape m_layout;

    size_t m_featureDim;
    size_t m_labelDim;

    std::vector<size_t> m_rowStartIdxes;

    StorageFormat m_featureStorageType;
    StorageFormat m_labelStorageType;

    // General configuration
    int m_logVerbosity;
};

typedef std::shared_ptr<DataFrameDeserializer> DataFrameDeserializerPtr;

}}
