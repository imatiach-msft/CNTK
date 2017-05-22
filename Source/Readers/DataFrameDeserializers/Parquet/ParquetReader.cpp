//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//
#include <iostream>

#include "ParquetReader.h"

namespace Microsoft { namespace MSR { namespace CNTK { namespace DF
{
    // when returning a BatchRecord, we need to instantiate and populate vectors of type Array, which takes a dependency
    // on buffer & memory pools.
    // TODO: Decide on the size of the buffer + memory size for optimization
    ParquetReader::ParquetReader()
    {
        m_pfr = parquet::ParquetFileReader::Open(std::move(filePtr));
        m_metadata = m_pfr -> metadata();
    }

    ParquetReader::ParquetReader(const ConfigParameters& config) : m_config(config) {}

    std::shared_ptr<TableMetaData> ParquetReader::InitializeSources(const FileList& sources)
    { 
        // Note: FileList is just std::vector<arrow::io::ReadableFileInterface>
        //       There are multiple parquet files, each of which hold multiple row groups
        std::vector<size_t> rowCounts;
        bool storedSchema = false;
        std::shared_ptr<arrow::Schema> schema;
        for (int i = 0; i < sources.size(); i++)
        {
            std::unique_ptr<parquet::ParquetFileReader> pfr =
                parquet::ParquetFileReader::Open(&sources[i]);
            m_fileReaders.push_back(pfr); // the files are sorted and matches the ordering in the vector
            std::shared_ptr<parquet::FileMetaData> md = pfr -> metadata();
            int numRowGroups = md -> num_row_groups();
            m_rowGroupsPerFile.push_back(numRowGroups);
            if (!storedSchema)
            {
                // the schema should be the same for the parquet files in the directory
                const parquet::SchemaDescriptor* pSchema = md -> schema();
                ParquetReader::ParquetSchemaToArrowSchema(pSchema, &schema);
                storedSchema = true;
            }

            // store the number of rows for each rowgroup
            for (int rg = 0; rg < numRowGroups; rg++)
            {
                std::unique_ptr<parquet::RowGroupMetaData> rgMetadata = md -> RowGroup(rg);
                int64_t numRowsInRG = rgMetadata -> num_rows();
                rowCounts.push_back(numRowsInRG);
            }
        }
        return std::shared_ptr<TableMetadata> tMetadata(new TableMetadata(schema, rowCounts));
    }

    void ParquetReader::CalculateIndexes(ChunkIdType id int& fileIndex, int& rowGroupIndex)
    {
        // calculate the file index
        for (int i = 0; i < m_rowGroupsPerFile.size(); i++)
        {
            if (id <= 0) break;
            id -= m_rowGroupsPerFile[i];
            fileIndex = i;
            rowGroupIndex = id;
        }
    }

    std::shared_ptr<TableChunk> ParquetReader::GetChunk(ChunkIdType id)
    {
        // ChunkIdType is just a uint32_t variable used by CNTK,
        // so we need to keep track of the number of files and their respective
        // rowgroup counts to retrieve the chunk from correct file + rowGroupNo.
        int fileIndex, rowGroupIndex;
        CalculateIndexes(id, pfr, rowGroupIndex);
        std::unique_ptr<parquet::ParquetFileReader> pfr = m_fileReaders[fileIndex];
        std::shared_ptr<parquet::FileMetaData> md = pfr -> metadata();
        int numCols = md -> num_columns();

        const parquet::SchemaDescriptor* schema = md -> schema();
        std::shared_ptr<arrow::Schema> arrowSchema;
        ParquetSchemaToArrowSchema(schema, &arrowSchema);

        std::vector<std::shared_ptr<arrow::Array>> columns;
        std::unique_ptr<parquet::RowGroupMetaData> rgMetadata = md -> RowGroup(rowGroupIndex);
        int64_t numRowsInRowGroup = rgMetadata -> num_rows();



        
        // TableChunk is arrow::RecordBatch and ChunkIdType is a tuple of fileIndex + rowGroupNo
        int numCols = GetNumCols();
        const parquet::SchemaDescriptor* schema = GetSchema();
        std::shared_ptr<arrow::Schema> arrowSchema;
        ParquetSchemaToArrowSchema(schema, &arrowSchema);

        std::vector<std::shared_ptr<arrow::Array>> columns;

        std::unique_ptr<parquet::RowGroupMetaData> rowGroupMetaData = GetRowGroupMetaData(rowGroupIndex);
        // TODO: Error checking - check if the ParquetFileReader has been instantiated
        std::shared_ptr<parquet::RowGroupReader> rgr = m_pfr -> RowGroup(rowGroupIndex);
        int64_t numRowsInRowGroup = rowGroupMetaData -> num_rows();

        // Loop through the columns in the Row Group
        for (int col = 0; col < numCols; col++)
        {
            std::shared_ptr<parquet::ColumnReader> cr = rgr -> Column(col);
            const parquet::ColumnDescriptor* colDescriptor = GetColumnDescriptor(schema, col);
            // Cast the ColumnReader to the correct column
            parquet::Type::type physType = colDescriptor -> physical_type();
            // Depending on the physical type of the column, cast the Column Reader to the correct type,
            // read the data into an arrow::Array and push it into the columns vector.
            switch (physType)
            {
                case parquet::Type::FLOAT:
                {
                printf("Reading Floats!\n");
                parquet::FloatReader* floatReader =
                    static_cast<parquet::FloatReader*>(cr.get());
                float results[numRowsInRowGroup]; // a buffer to read in the data
                int64_t valuesRead;
                int rowsRead = floatReader -> ReadBatch(numRowsInRowGroup, nullptr, nullptr, results, &valuesRead);
                assert(rowsRead == numRowsInRowGroup);   // the number of rows read must match the number of rows in row group
                assert(valuesRead == numRowsInRowGroup); // the number of values read must also match the number of rows in row group. TODO: check if null values also count as read values

                // At this point, the results array is populated with read values.
                // Now, we pack these values into Arrays and push it into the vector of shared_ptr<arrow::Array>
                arrow::DefaultMemoryPool pool;
                arrow::LoggingMemoryPool lp(&pool);
                // Build the Array
                std::shared_ptr<arrow::FloatType> type = std::make_shared<arrow::FloatType>();
                std::shared_ptr<arrow::FloatBuilder> builder = std::make_shared<arrow::FloatBuilder>(&lp, type);
                builder -> Reserve(numRowsInRowGroup);

                // size_t nullCount = 0;
                for (int i = 0; i < numRowsInRowGroup; i++)
                {
                    printf("Read in value: %f\n", results[i]);
                    builder -> Append(results[i]);
                    /* TODO: Support for null values in datasets.
                            if (results[i]) builder -> Append(results[i]);
                            else 
                            {
                                builder -> AppendNull();
                                nullCount++;
                            }
                            */
                }
                std::shared_ptr<arrow::Array> arrayWithData;
                arrow::Status s = builder -> Finish(&arrayWithData);
                columns.push_back(arrayWithData);
                break;
                }
                case parquet::Type::DOUBLE:
                {
                    printf("Reading Doubles!\n");
                    parquet::DoubleReader* doubleReader =
                        static_cast<parquet::DoubleReader*>(cr.get());
                    double results[numRowsInRowGroup];
                    int64_t valuesRead;
                    int rowsRead = doubleReader -> ReadBatch(numRowsInRowGroup, nullptr, nullptr, results, &valuesRead);
                    assert(rowsRead == numRowsInRowGroup); // the number of rows read must match the number of rows in row group
                    assert(valuesRead == numRowsInRowGroup);

                    arrow::DefaultMemoryPool pool;
                    arrow::LoggingMemoryPool lp(&pool);
                    // Build the Array
                    std::shared_ptr<arrow::DoubleType> type = std::make_shared<arrow::DoubleType>();
                    std::shared_ptr<arrow::DoubleBuilder> builder = std::make_shared<arrow::DoubleBuilder>(&lp, type);
                    builder -> Reserve(numRowsInRowGroup);

                    for (int i = 0; i < numRowsInRowGroup; i++)
                    {
                        printf("Read in value: %f\n", results[i]);
                        builder -> Append(results[i]);
                    }

                    std::shared_ptr<arrow::Array> arrayWithData;
                    arrow::Status s = builder -> Finish(&arrayWithData);
                    columns.push_back(arrayWithData);
                    break;
                }
                case parquet::Type::FIXED_LEN_BYTE_ARRAY:
                {
                    printf("Reading Fixed Length Byte Arrays!\n");
                    parquet::FixedLenByteArrayReader* fByteReader =
                        static_cast<parquet::FixedLenByteArrayReader*>(cr.get());
                    uint8_t results[numRowsInRowGroup];
                    parquet::FixedLenByteArray flbArray(results);
                    int64_t valuesRead;
                    int rowsRead = fByteReader -> ReadBatch(numRowsInRowGroup, nullptr, nullptr, &flbArray, &valuesRead);
                    assert(rowsRead == numRowsInRowGroup); // the number of rows read must match the number of rows in row group
                    assert(valuesRead == numRowsInRowGroup);

                    arrow::DefaultMemoryPool pool;
                    arrow::LoggingMemoryPool lp(&pool);
                    // Build the Array
                    std::shared_ptr<arrow::FixedSizeBinaryType> type = std::make_shared<arrow::FixedSizeBinaryType>(numBytesForFixedArray);
                    std::shared_ptr<arrow::FixedSizeBinaryBuilder> builder = std::make_shared<arrow::FixedSizeBinaryBuilder>(&lp, type);
                    builder -> Reserve(numRowsInRowGroup);

                    for (int i = 0; i < numRowsInRowGroup; i++)
                    {
                        builder -> Append(&results[i]);
                    }

                    std::shared_ptr<arrow::Array> arrayWithData;
                    arrow::Status s = builder -> Finish(&arrayWithData);
                    columns.push_back(arrayWithData);
                    break;
                }
                case parquet::Type::BOOLEAN:
                {
                    printf("Reading Booleans!\n");
                    parquet::BoolReader* boolReader =
                        static_cast<parquet::BoolReader*>(cr.get());
                    bool results[numRowsInRowGroup];
                    int64_t valuesRead;
                    int rowsRead = boolReader -> ReadBatch(numRowsInRowGroup, nullptr, nullptr, results, &valuesRead);
                    assert(rowsRead == numRowsInRowGroup); // the number of rows read must match the number of rows in row group
                    assert(valuesRead == numRowsInRowGroup);

                    arrow::DefaultMemoryPool pool;
                    arrow::LoggingMemoryPool lp(&pool);
                    // Build the Array
                    std::shared_ptr<arrow::BooleanType> type = std::make_shared<arrow::BooleanType>();
                    std::shared_ptr<arrow::BooleanBuilder> builder = std::make_shared<arrow::BooleanBuilder>(&lp, type);
                    builder -> Reserve(numRowsInRowGroup);

                    for (int i = 0; i < numRowsInRowGroup; i++)
                    {
                        printf("Read in value: %s\n", results[i] ? "true" : "false");
                        builder -> Append(results[i]);
                    }

                    std::shared_ptr<arrow::Array> arrayWithData;
                    arrow::Status s = builder -> Finish(&arrayWithData);
                    columns.push_back(arrayWithData);
                    break;

                }
                case parquet::Type::INT32:
                {
                    printf("Reading Int32s!\n");
                    parquet::Int32Reader* i32Reader =
                        static_cast<parquet::Int32Reader*>(cr.get());
                    int32_t results[numRowsInRowGroup];
                    int64_t valuesRead;
                    int rowsRead = i32Reader -> ReadBatch(numRowsInRowGroup, nullptr, nullptr, results, &valuesRead);
                    assert(rowsRead == numRowsInRowGroup);
                    assert(valuesRead == numRowsInRowGroup);

                    arrow::DefaultMemoryPool pool;
                    arrow::LoggingMemoryPool lp(&pool);
                    // Build the Array
                    std::shared_ptr<arrow::Int32Type> type = std::make_shared<arrow::Int32Type>();
                    std::shared_ptr<arrow::Int32Builder> builder = std::make_shared<arrow::Int32Builder>(&lp, type);
                    builder -> Reserve(numRowsInRowGroup);

                    for (int i = 0; i < numRowsInRowGroup; i++)
                    {
                        printf("Read in value: %i\n", results[i]);
                        builder -> Append(results[i]);
                    }

                    std::shared_ptr<arrow::Array> arrayWithData;
                    arrow::Status s = builder -> Finish(&arrayWithData);
                    columns.push_back(arrayWithData);
                    break;
                }
                case parquet::Type::INT64:
                {
                    printf("Reading Int64s!\n");
                    parquet::Int64Reader* i64Reader =
                        static_cast<parquet::Int64Reader*>(cr.get());
                    int64_t results[numRowsInRowGroup];
                    int64_t valuesRead;
                    int rowsRead = i64Reader -> ReadBatch(numRowsInRowGroup, nullptr, nullptr, results, &valuesRead);
                    assert(rowsRead == numRowsInRowGroup);
                    assert(valuesRead == numRowsInRowGroup);

                    arrow::DefaultMemoryPool pool;
                    arrow::LoggingMemoryPool lp(&pool);
                    // Build the Array
                    std::shared_ptr<arrow::Int64Type> type = std::make_shared<arrow::Int64Type>();
                    std::shared_ptr<arrow::Int64Builder> builder = std::make_shared<arrow::Int64Builder>(&lp, type);
                    builder -> Reserve(numRowsInRowGroup);

                    for (int i = 0; i < numRowsInRowGroup; i++)
                    {
                        printf("Read in value: %ld\n", results[i]);
                        builder -> Append(results[i]);
                    }

                    std::shared_ptr<arrow::Array> arrayWithData;
                    arrow::Status s = builder -> Finish(&arrayWithData);
                    columns.push_back(arrayWithData);
                    break;                    
                }
                default:
                {
                    std::cout << "Support for " << physType << " file types will be added in the future." << std::endl;
                }
                // Support for other types will be added in the future
            } // switch
        } // column for loop
        // Return a constructed RecordBatch
        return arrow::RecordBatch(arrowSchema, numRowsInRowGroup, columns);        
    }

    // TODO: Is it better to have a wrapper class/struct that contains all the 
    // information stored in the FileMetaData?
    // Should this be changed into a simple getter that returns the m_metadata?
    std::shared_ptr<parquet::FileMetaData> ParquetReader::GetMetadata(std::unique_ptr<parquet::ParquetFileReader> pfr)
    {
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

    std::unique_ptr<parquet::RowGroupMetaData> ParquetReader::GetRowGroupMetaData(int rowGroupIndex)
    {
        return m_metadata -> RowGroup(rowGroupIndex);
    }

    const parquet::ColumnDescriptor* ParquetReader::GetColumnDescriptor(const parquet::SchemaDescriptor* schema, int colIndex)
    {
        if (schema == nullptr)
        {
            std::cout << "The SchemaDescriptor has not been instantiated." << std::endl;
            return nullptr;
        }
        return schema -> Column(colIndex);
    }

    void ParquetReader::ParquetSchemaToArrowSchema(const parquet::SchemaDescriptor* parquetSchema, std::shared_ptr<arrow::Schema>* arrowSchema)
    {
        arrow::Status s = parquet::arrow::FromParquetSchema(parquetSchema, arrowSchema);
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
        int numCols = GetNumCols();
        const parquet::SchemaDescriptor* schema = GetSchema();
        std::shared_ptr<arrow::Schema> arrowSchema;

        printf("Converting Parquet Schema to Arrow Schema!\n");

        ParquetSchemaToArrowSchema(schema, &arrowSchema);

        printf("Converted Schema!\n");
        std::vector<std::shared_ptr<arrow::Array>> columns;

        std::unique_ptr<parquet::RowGroupMetaData> rowGroupMetaData = GetRowGroupMetaData(rowGroupIndex);
        // TODO: Error checking - check if the ParquetFileReader has been instantiated
        std::shared_ptr<parquet::RowGroupReader> rgr = m_pfr -> RowGroup(rowGroupIndex);
        int64_t numRowsInRowGroup = rowGroupMetaData -> num_rows();

        // Loop through the columns in the Row Group
        for (int col = 0; col < numCols; col++)
        {
            std::shared_ptr<parquet::ColumnReader> cr = rgr -> Column(col);
            const parquet::ColumnDescriptor* colDescriptor = GetColumnDescriptor(schema, col);
            // Cast the ColumnReader to the correct column
            parquet::Type::type physType = colDescriptor -> physical_type();
            // Depending on the physical type of the column, cast the Column Reader to the correct type,
            // read the data into an arrow::Array and push it into the columns vector.
            switch (physType)
            {
                case parquet::Type::FLOAT:
                {
                printf("Reading Floats!\n");
                parquet::FloatReader* floatReader =
                    static_cast<parquet::FloatReader*>(cr.get());
                float results[numRowsInRowGroup]; // a buffer to read in the data
                int64_t valuesRead;
                int rowsRead = floatReader -> ReadBatch(numRowsInRowGroup, nullptr, nullptr, results, &valuesRead);
                assert(rowsRead == numRowsInRowGroup);   // the number of rows read must match the number of rows in row group
                assert(valuesRead == numRowsInRowGroup); // the number of values read must also match the number of rows in row group. TODO: check if null values also count as read values

                // At this point, the results array is populated with read values.
                // Now, we pack these values into Arrays and push it into the vector of shared_ptr<arrow::Array>
                arrow::DefaultMemoryPool pool;
                arrow::LoggingMemoryPool lp(&pool);
                // Build the Array
                std::shared_ptr<arrow::FloatType> type = std::make_shared<arrow::FloatType>();
                std::shared_ptr<arrow::FloatBuilder> builder = std::make_shared<arrow::FloatBuilder>(&lp, type);
                builder -> Reserve(numRowsInRowGroup);

                // size_t nullCount = 0;
                for (int i = 0; i < numRowsInRowGroup; i++)
                {
                    printf("Read in value: %f\n", results[i]);
                    builder -> Append(results[i]);
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
                arrow::Status s = builder -> Finish(&arrayWithData);
                columns.push_back(arrayWithData);
                break;
                }
                case parquet::Type::DOUBLE:
                {
                    printf("Reading Doubles!\n");
                    parquet::DoubleReader* doubleReader =
                        static_cast<parquet::DoubleReader*>(cr.get());
                    double results[numRowsInRowGroup];
                    int64_t valuesRead;
                    int rowsRead = doubleReader -> ReadBatch(numRowsInRowGroup, nullptr, nullptr, results, &valuesRead);
                    assert(rowsRead == numRowsInRowGroup); // the number of rows read must match the number of rows in row group
                    assert(valuesRead == numRowsInRowGroup);

                    arrow::DefaultMemoryPool pool;
                    arrow::LoggingMemoryPool lp(&pool);
                    // Build the Array
                    std::shared_ptr<arrow::DoubleType> type = std::make_shared<arrow::DoubleType>();
                    std::shared_ptr<arrow::DoubleBuilder> builder = std::make_shared<arrow::DoubleBuilder>(&lp, type);
                    builder -> Reserve(numRowsInRowGroup);

                    for (int i = 0; i < numRowsInRowGroup; i++)
                    {
                        printf("Read in value: %f\n", results[i]);
                        builder -> Append(results[i]);
                    }

                    std::shared_ptr<arrow::Array> arrayWithData;
                    arrow::Status s = builder -> Finish(&arrayWithData);
                    columns.push_back(arrayWithData);
                    break;
                }
                case parquet::Type::FIXED_LEN_BYTE_ARRAY:
                {
                    printf("Reading Fixed Length Byte Arrays!\n");
                    parquet::FixedLenByteArrayReader* fByteReader =
                        static_cast<parquet::FixedLenByteArrayReader*>(cr.get());
                    uint8_t results[numRowsInRowGroup];
                    parquet::FixedLenByteArray flbArray(results);
                    int64_t valuesRead;
                    int rowsRead = fByteReader -> ReadBatch(numRowsInRowGroup, nullptr, nullptr, &flbArray, &valuesRead);
                    assert(rowsRead == numRowsInRowGroup); // the number of rows read must match the number of rows in row group
                    assert(valuesRead == numRowsInRowGroup);

                    arrow::DefaultMemoryPool pool;
                    arrow::LoggingMemoryPool lp(&pool);
                    // Build the Array
                    std::shared_ptr<arrow::FixedSizeBinaryType> type = std::make_shared<arrow::FixedSizeBinaryType>(numBytesForFixedArray);
                    std::shared_ptr<arrow::FixedSizeBinaryBuilder> builder = std::make_shared<arrow::FixedSizeBinaryBuilder>(&lp, type);
                    builder -> Reserve(numRowsInRowGroup);

                    for (int i = 0; i < numRowsInRowGroup; i++)
                    {
                        builder -> Append(&results[i]);
                    }

                    std::shared_ptr<arrow::Array> arrayWithData;
                    arrow::Status s = builder -> Finish(&arrayWithData);
                    columns.push_back(arrayWithData);
                    break;
                }
                case parquet::Type::BOOLEAN:
                {
                    printf("Reading Booleans!\n");
                    parquet::BoolReader* boolReader =
                        static_cast<parquet::BoolReader*>(cr.get());
                    bool results[numRowsInRowGroup];
                    int64_t valuesRead;
                    int rowsRead = boolReader -> ReadBatch(numRowsInRowGroup, nullptr, nullptr, results, &valuesRead);
                    assert(rowsRead == numRowsInRowGroup); // the number of rows read must match the number of rows in row group
                    assert(valuesRead == numRowsInRowGroup);

                    arrow::DefaultMemoryPool pool;
                    arrow::LoggingMemoryPool lp(&pool);
                    // Build the Array
                    std::shared_ptr<arrow::BooleanType> type = std::make_shared<arrow::BooleanType>();
                    std::shared_ptr<arrow::BooleanBuilder> builder = std::make_shared<arrow::BooleanBuilder>(&lp, type);
                    builder -> Reserve(numRowsInRowGroup);

                    for (int i = 0; i < numRowsInRowGroup; i++)
                    {
                        printf("Read in value: %s\n", results[i] ? "true" : "false");
                        builder -> Append(results[i]);
                    }

                    std::shared_ptr<arrow::Array> arrayWithData;
                    arrow::Status s = builder -> Finish(&arrayWithData);
                    columns.push_back(arrayWithData);
                    break;

                }
                case parquet::Type::INT32:
                {
                    printf("Reading Int32s!\n");
                    parquet::Int32Reader* i32Reader =
                        static_cast<parquet::Int32Reader*>(cr.get());
                    int32_t results[numRowsInRowGroup];
                    int64_t valuesRead;
                    int rowsRead = i32Reader -> ReadBatch(numRowsInRowGroup, nullptr, nullptr, results, &valuesRead);
                    assert(rowsRead == numRowsInRowGroup);
                    assert(valuesRead == numRowsInRowGroup);

                    arrow::DefaultMemoryPool pool;
                    arrow::LoggingMemoryPool lp(&pool);
                    // Build the Array
                    std::shared_ptr<arrow::Int32Type> type = std::make_shared<arrow::Int32Type>();
                    std::shared_ptr<arrow::Int32Builder> builder = std::make_shared<arrow::Int32Builder>(&lp, type);
                    builder -> Reserve(numRowsInRowGroup);

                    for (int i = 0; i < numRowsInRowGroup; i++)
                    {
                        printf("Read in value: %i\n", results[i]);
                        builder -> Append(results[i]);
                    }

                    std::shared_ptr<arrow::Array> arrayWithData;
                    arrow::Status s = builder -> Finish(&arrayWithData);
                    columns.push_back(arrayWithData);
                    break;
                }
                case parquet::Type::INT64:
                {
                    printf("Reading Int64s!\n");
                    parquet::Int64Reader* i64Reader =
                        static_cast<parquet::Int64Reader*>(cr.get());
                    int64_t results[numRowsInRowGroup];
                    int64_t valuesRead;
                    int rowsRead = i64Reader -> ReadBatch(numRowsInRowGroup, nullptr, nullptr, results, &valuesRead);
                    assert(rowsRead == numRowsInRowGroup);
                    assert(valuesRead == numRowsInRowGroup);

                    arrow::DefaultMemoryPool pool;
                    arrow::LoggingMemoryPool lp(&pool);
                    // Build the Array
                    std::shared_ptr<arrow::Int64Type> type = std::make_shared<arrow::Int64Type>();
                    std::shared_ptr<arrow::Int64Builder> builder = std::make_shared<arrow::Int64Builder>(&lp, type);
                    builder -> Reserve(numRowsInRowGroup);

                    for (int i = 0; i < numRowsInRowGroup; i++)
                    {
                        printf("Read in value: %ld\n", results[i]);
                        builder -> Append(results[i]);
                    }

                    std::shared_ptr<arrow::Array> arrayWithData;
                    arrow::Status s = builder -> Finish(&arrayWithData);
                    columns.push_back(arrayWithData);
                    break;                    
                }
                default:
                {
                    std::cout << "Support for " << physType << " file types will be added in the future." << std::endl;
                }
                // Support for other types will be added in the future
            } // switch
        } // column for loop
        // Return a constructed RecordBatch
        return arrow::RecordBatch(arrowSchema, numRowsInRowGroup, columns);
    }

    void ParquetReader::PrintRecordBatch(const arrow::RecordBatch& batch, int indent, std::ostream* out)
    {
        arrow::Status s = arrow::PrettyPrint(batch, indent, out); 
        if (!s.ok())
        {
            std::cout << "Could not print the provided RecordBatch." << std::endl;
            return;
        }
    }
        // Loop through the columns, check the column type, read all the rows in the column, store it in a buffer, pass it into the Array
        // save the data into a vector of arrow Arrays, then return the Array of columns.
        // Need to check the underlying physical datatype to pass into the Array.
        // First, get the ParquetReader schema, convert it to an Arrow Schema,
        // Then read num_rows and save the data into an Array of columns

} // hdfs
}}}