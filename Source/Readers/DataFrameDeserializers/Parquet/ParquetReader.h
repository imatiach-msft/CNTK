//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//

#pragma once

#include <cassert>

#include "arrow/memory_pool.h"
#include "arrow/buffer.h"
#include "arrow/array.h"
#include "arrow/type.h"
#include "arrow/io/interfaces.h"
#include "parquet/api/reader.h"
#include "parquet/arrow/schema.h"
#include "arrow/pretty_print.h"

#include "HDFSArrowReader.h"
#include "Interfaces.h"

namespace Microsoft { namespace MSR { namespace CNTK { namespace DF
{
    class ParquetReader: FileReader
    {

    public:
        ParquetReader(const ConfigParameters& config);
        ~ParquetReader() override;
        std::shared_ptr<TableMetaData> InitializeSources(const FileList& sources) override;
        std::shared_ptr<TableChunk> GetChunk(ChunkIdType id) override;

        void CalculateIndexes(ChunkIdType id int& fileIndex, int& rowGroupIndex);

        const parquet::ColumnDescriptor* GetColumnDescriptor(const parquet::SchemaDescriptor* schema, int colIndex);
        void ParquetSchemaToArrowSchema(const parquet::SchemaDescriptor* parquetSchema, std::shared_ptr<arrow::Schema>* arrowSchema);
        arrow::RecordBatch ReadBatch(int rowGroupIndex);
        void PrintRecordBatch(const arrow::RecordBatch& batch, int indent, std::ostream* out);

    private:
        ConfigParameters m_config;
        std::vector<std::unique_ptr<parquet::ParquetFileReader>> m_fileReaders;
        std::vector<int> m_rowGroupsPerFile; // stores the number of rowgroups per file during initializesources
        const int32_t numBytesForFixedArray = 1;
    };

} // DF
}}}