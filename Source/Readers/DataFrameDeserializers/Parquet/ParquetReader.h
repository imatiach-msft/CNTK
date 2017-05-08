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
#include "HDFSArrowReader.h"


namespace Microsoft { namespace MSR { namespace CNTK { namespace hdfs
{
    class ParquetReader
    {

    public:
        ParquetReader(std::shared_ptr<arrow::io::RandomAccessFile> filePtr);
        ~ParquetReader();
        std::shared_ptr<parquet::FileMetaData> GetMetadata();
        int GetNumRowGroups();
        int GetNumCols();
        const parquet::SchemaDescriptor* GetSchema();
        std::unique_ptr<parquet::RowGroupMetaData> GetRowGroupMetaData(int rowGroupIndex);
        const parquet::ColumnDescriptor* GetColumnDescriptor(const parquet::SchemaDescriptor* schema, int colIndex);
        void ParquetSchemaToArrowSchema(const parquet::SchemaDescriptor* parquetSchema, std::shared_ptr<arrow::Schema>* arrowSchema);
        arrow::RecordBatch ReadBatch(int rowGroupIndex);

    private:
        std::shared_ptr<arrow::io::RandomAccessFile> m_filePtr;
        std::unique_ptr<parquet::ParquetFileReader> m_pfr;
        std::shared_ptr<parquet::FileMetaData> m_metadata;
        const int32_t numBytesForFixedArray = 1;

    };

} // hdfs
}}}