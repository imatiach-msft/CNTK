//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//
#include <iostream>

#include "ParquetReader.h"

namespace Microsoft { namespace MSR { namespace CNTK { namespace hdfs
{
    // when returning a BatchRecord, we need to instantiate and populate vectors of type Array, which takes a dependency
    // on buffer & memory pools.
    // TODO: Decide on the size of the buffer + memory size for optimization
    ParquetReader::ParquetReader(std::shared_ptr<arrow::io::RandomAccessFile> filePtr) : m_filePtr(filePtr)
    {
        m_pfr = parquet::ParquetFileReader::Open(std::move(m_filePtr));
    }

    // TODO: Is it better to have a wrapper class/struct that contains all the information stored in the FileMetaData?
    std::shared_ptr<parquet::FileMetaData> ParquetReader::GetMetadata()
    {
        m_metadata = m_pfr -> metadata();
        return m_metadata;
    }

    int ParquetReader::GetNumRowGroups()
    {
        return m_metadata -> num_row_groups();
    }

    int ParquetReader::GetNumCols()
    {
        return m_metadata -> num_columns();
    }

    const parquet::SchemaDescriptor* ParquetReader::GetSchema()
    {
        return m_metadata -> schema();
    }

    std::unique_ptr<RowGroupMetaData> ParquetReader::GetRowGroupMetaData(int rowGroupIndex)
    {
        return m_metadata -> RowGroup(rowGroupIndex);
    }

    const parquet::ColumnDescriptor* ParquetReader::GetColumnDescriptor(const parquet::SchemaDescriptor* schema, int colIndex)
    {
        if (schema == nullptr)
        {
            std::cout << "The SchemaDescriptor has not been instantiated." << std::endl;
            return;
        }
        return schema -> Column(colIndex);
    }

    void ParquetReader::ParquetSchemaToArrowSchema(const parquet::SchemaDescriptor* parquetSchema, std::shared_ptr<arrow::Schema>* arrowSchema)
    {
        Status s = parquet::FromParquetSchema(parquetSchema, arrowSchema);
        if (!s.ok()) 
        {
            std::cout << "Could not convert from Parquet schema to Arrow schema." << std::endl;
            return;
        }
    }

    // An arrow RecordBatch (defined in arrow/table.h)'s prototype is:
    // RecordBatch(const std::shared_ptr<Schema>& schema, int64_t num_rows, const std::vector<std::shared_ptr<Array>>& columns);
    // Reads a batch -> currently, we default to using the number of rows per row group.
    arrow::RecordBatch ParquetReader::ReadBatch(int rowGroupIndex) 
    {
        int numRowGroups = GetNumRowGroups();
        int numCols = GetNumCols();
        const parquet::SchemaDescriptor* schema = GetSchema();
        std::shared_ptr<arrow::Schema> arrowSchema;
        ParquetSchemaToArrowSchema(schema, &arrowSchema);
        if (!s.ok())
        {
            std::cout << "Connection to HDFSClient failed." << std::endl;
            return;
        }
        std::vector<std::shared_ptr<arrow::Array>> columns;

        std::unique_ptr<parquet::RowGroupMetaData> rowGroupMetaData = GetRowGroupMetaData(rg);
        // TODO: Error checking - check if the ParquetFileReader has been instantiated
        std::shared_ptr<parquet::RowGroupReader> rgr = m_pfr->RowGroup(rg);
        int64_t numRowsInRowGroup = rowGroupMetaData->num_rows();

        // Loop through the columns in the Row Group
        for (int col = 0; col < numCols; col++)
        {
            std::shared_ptr<parquet::ColumnReader> cr = rgr->Column(col);
            const parquet::ColumnDescriptor* colDescriptor = GetColumnDescriptor(schema, col);
            // Cast the ColumnReader to the correct column
            parquet::Type::type physType = colDescriptor->physical_type();
            // Depending on the physical type of the column, cast the Column Reader to the correct type,
            // read the data into an arrow::Array and push it into the columns vector.
            switch (physType)
            {
            case parquet::Type::FLOAT:

                parquet::FloatReader* floatReader =
                    static_cast<parquet::FloatReader*>(cr.get());
                float results[numRowsInRowGroup]; // a buffer to read in the data
                int64_t valuesRead;
                int rowsRead = floatReader->ReadBatch(numRowsInRowGroup, nullptr, nullptr, results, &valuesRead);
                assert(rowsRead == numRowsInRowGroup);   // the number of rows read must match the number of rows in row group
                assert(valuesRead == numRowsInRowGroup); // the number of values read must also match the number of rows in row group. TODO: check if null values also count as read values

                // At this point, the results array is populated with read values.
                // Now, we pack these values into Arrays and push it into the vector of shared_ptr<arrow::Array>
                DefaultMemoryPool pool;
                LoggingMemoryPool lp(&pool);
                // Build the Array
                std::shared_ptr<arrow::FloatType> type = std::make_shared<arrow::FloatType>();
                std::shared_ptr<arrow::FloatBuilder> builder = std::make_shared<arrow::FloatBuilder>(lp, type);
                builder->Reserve(numRowsInRowGroup);
                // size_t nullCount = 0;
                for (int i = 0; i < numRowsInRowGroup; i++)
                {
                    builder->Append(results[i]);
                    /* TODO: Support for null values in datasets.
                            if (results[i]) builder -> Append(results[i]);
                            else 
                            {
                                builder -> AppendNull();
                                nullCount++;
                            }
                            */
                }
                // TODO: Remove this after checking correct read behaviors
                // const uint8_t* data = reinterpret_cast<const uint8_t*> results;
                // std::shared_ptr<arrow::Buffer> buffWithData = std::make_shared<arrow::Buffer>(data, sizeof(results));

                // TODO: when optimizing, the valuesRead will be different from the numRowsInRowGroup, so make sure
                //       to use the correct variable.
                // std::shared_ptr<arrow::FloatArray> arrayWithData = std::make_shared<arrow::FloatArray>(type, numRowsInRowGroup, buffWithData);
                std::shared_ptr<arrow::Array> arrayWithData;
                arrow::Status s = builder->Finish(&arrayWithData);
                columns.push_back(arrayWithData);
                break;

            case parquet::Type::DOUBLE:
                break;
            case parquet::Type::BYTE_ARRAY:
                break;
            default:
                // Support for other types will be added in the future
            } // switch
        } // column for loop
        // Construct a RecordBatch
        return RecordBatch(arrowSchema, numRowsInRowGroup, &columns);
    }
        // Loop through the columns, check the column type, read all the rows in the column, store it in a buffer, pass it into the Array
        // save the data into a vector of arrow Arrays, then return the Array of columns.
        // Need to check the underlying physical datatype to pass into the Array.
        // First, get the ParquetReader schema, convert it to an Arrow Schema,
        // Then read num_rows and save the data into an Array of columns

} // hdfs
}}}