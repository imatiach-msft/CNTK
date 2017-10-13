//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//

#pragma once
/*
#include "arrow/memory_pool.h"
#include "arrow/buffer.h"
#include "arrow/array.h"
#include "arrow/type.h"
#include "arrow/pretty_print.h"
*/
//#include "arrow/io/interfaces.h"

#include "../Common/Constants.h"
#include "../Common/Interfaces.h"
#include "../Common/TabularChunk.h"
#include "../HDFS/HDFSArrowReader.h"

namespace CNTK { namespace DF
{
    class ParquetReader: public FileReader
    {

    public:
        ParquetReader(const ConfigParameters& config);
        std::shared_ptr<TableMetadata> InitializeSources(const FileList& sources) override;
        std::shared_ptr<ChunkBuffer> GetChunkBuffer(ChunkIdType id) override;

        void CalculateIndexes(ChunkIdType id, int& fileIndex, int& rowGroupIndex);

        void CreateColumnMap(const parquet::SchemaDescriptor* pSchema);
        const parquet::ColumnDescriptor* GetColumnDescriptor(const parquet::SchemaDescriptor* schema, int colIndex);
        void ParquetSchemaToArrowSchema(const parquet::SchemaDescriptor* parquetSchema, std::shared_ptr<arrow::Schema>* arrowSchema);
        arrow::RecordBatch ReadBatch(int rowGroupIndex);
        void PrintRecordBatch(const arrow::RecordBatch& batch, int indent, std::ostream* out);

    private:
        ConfigParameters m_config;
        std::vector<std::unique_ptr<parquet::ParquetFileReader>> m_fileReaders;
        std::vector<int> m_rowGroupsPerFile; // stores the number of rowgroups per file during initializesources
        arrow::DefaultMemoryPool* m_pool;
        arrow::LoggingMemoryPool* m_loggingPool;
        size_t m_featureDim;
        size_t m_labelDim;
        bool   m_isFeatureSparse;
        bool   m_isLabelSparse;
        DataType m_precision;
        // std::set<std::string> m_colNamesToRead;
	//  std::set<int> m_colIndexesToRead;
        // std::set<int> m_columnMap;
        const int32_t numBytesForFixedArray = 1;
    };

} // DF
}
