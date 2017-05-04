//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//

#pragma once
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
        std::shared_ptr<parquet::FileMetaData> ParquetReader::GetMetadata();
        std::shared_ptr<arrow::Schema> ParquetReader::ParquetSchemaToArrowSchema(const parquet::SchemaDescriptor* parquet_schema);

    private:
        std::shared_ptr<arrow::io::RandomAccessFile> m_filePtr;
        std::unique_ptr<parquet::ParquetFileReader> m_pfr;
        std::shared_ptr<parquet::FileMetaData> m_metadata;

    }

} // hdfs
}}}