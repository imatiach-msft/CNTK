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
    ParquetReader::ParquetReader(const ConfigParameters& config) : m_config(config) 
    {
        m_pool = new arrow::DefaultMemoryPool();
        m_loggingPool = new arrow::LoggingMemoryPool(m_pool);
    }

    void ParquetReader::CreateColumnMap(const parquet::SchemaDescriptor* pSchema)
    {
        // Loop through the columns and create a map of column names and indexes
        int numCols = pSchema -> num_columns();
        for (int col = 0; col < numCols; col++)
        {
            const ColumnDescriptor* colDescriptor = pSchema(col);
            // check if the column name exists in the brainscript config
            // If true, store it in the map.
        }
    }

    std::shared_ptr<TableMetadata> ParquetReader::InitializeSources(const FileList& sources)
    { 
        // Note: FileList is just std::vector<arrow::io::ReadableFileInterface>
        //       There are multiple parquet files, each of which hold multiple row groups
        std::vector<size_t> rowCounts;
        bool storedSchema = false;
        std::shared_ptr<arrow::Schema> schema;
        for (int i = 0; i < sources.size(); i++)
        {
            std::printf("initializing source from index %d\n", i);
            std::cout << "NOW READING filePtr: " << sources[i] << std::endl;
            std::unique_ptr<parquet::ParquetFileReader> pfr =
                parquet::ParquetFileReader::Open(sources[i]);
            std::cout << "OPENED PQ FILE" << std::endl;
            std::cout << "pushed the file into the m_fileReaders vector" << std::endl;

            std::shared_ptr<parquet::FileMetaData> md = pfr -> metadata();
            int numRowGroups = md -> num_row_groups();
            m_rowGroupsPerFile.push_back(numRowGroups);
            m_fileReaders.push_back(std::move(pfr)); // the files are sorted and matches the ordering in the vector            
            if (!storedSchema)
            {
                // the schema should be the same for the parquet files in the directory
                // we are also creating a map of column names and indexes so that users can specify column names in branscript
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
        std::shared_ptr<TableMetadata> tMetadata(new TableMetadata(schema, rowCounts));
        return tMetadata;
    }

    void ParquetReader::CalculateIndexes(ChunkIdType id, int& fileIndex, int& rowGroupIndex)
    {
        // calculate the file index
        std::cout << "In da calculateindexes" << std::endl;
        for (int i = 0; i < m_rowGroupsPerFile.size(); i++)
        {
            std::cout << "m_rowGroupsPerFile[" << i << "] = " << m_rowGroupsPerFile[i] << std::endl;
            std::cout << "chunkidtype id: " << id << std::endl;
            if (id <= 0) break;
            id -= m_rowGroupsPerFile[i];
            fileIndex = i;
            rowGroupIndex = id;
        }
    }

    // In our current setup, chunkId == rowGroup number in a parquet file
    std::shared_ptr<TableChunk> ParquetReader::GetChunk(ChunkIdType id)
    {
        // ChunkIdType is just a uint32_t variable used by CNTK,
        // so we need to keep track of the number of files and their respective
        // rowgroup counts to retrieve the chunk from correct file + rowGroupNo.

        std::cout << "IN PQREADER::GETCHUNK WITH ID: " << id << std::endl; 

        int fileIndex = 0;
        int rowGroupIndex = 0;
        CalculateIndexes(id, fileIndex, rowGroupIndex);
        std::cout << "calculated indexes: fileIndex = " << fileIndex << " rowGroupIndex = " << rowGroupIndex << std::endl;
        parquet::ParquetFileReader* pfr = m_fileReaders[fileIndex].get(); // used .get() to not take away ownership of the unique_ptr from the vector
        std::shared_ptr<parquet::FileMetaData> md = pfr -> metadata();
        int numCols = md -> num_columns();

        const parquet::SchemaDescriptor* schema = md -> schema();
        std::shared_ptr<arrow::Schema> arrowSchema;
        ParquetSchemaToArrowSchema(schema, &arrowSchema);

        std::cout << "converted pqs to as " << std::endl;

        std::vector<std::shared_ptr<arrow::Array>> columns(numCols);
        std::unique_ptr<parquet::RowGroupMetaData> rgMetadata = md -> RowGroup(rowGroupIndex);
        std::shared_ptr<parquet::RowGroupReader> rgr = pfr -> RowGroup(rowGroupIndex);
        int64_t numRowsInRowGroup = rgMetadata -> num_rows();

        std::cout << "Before entering the reading LOOOP" << std::endl;

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

                    // Build the Array
                    std::shared_ptr<arrow::FloatType> type = std::make_shared<arrow::FloatType>();
                    std::shared_ptr<arrow::FloatBuilder> builder = std::make_shared<arrow::FloatBuilder>(m_loggingPool, type);
                    builder -> Init(numRowsInRowGroup);

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
                    arrow::Status s = builder -> Finish(&arrayWithData); // contains data read in from a column's rowgroup
                    columns[col] = arrayWithData;
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

                    // Build the Array
                    std::shared_ptr<arrow::DoubleType> type = std::make_shared<arrow::DoubleType>();
                    std::shared_ptr<arrow::DoubleBuilder> builder = std::make_shared<arrow::DoubleBuilder>(m_loggingPool, type);
                    builder -> Init(numRowsInRowGroup);

                    for (int i = 0; i < numRowsInRowGroup; i++)
                    {
                        printf("Read in value: %f\n", results[i]);
                        builder -> Append(results[i]);
                    }

                    std::shared_ptr<arrow::Array> arrayWithData;
                    arrow::Status s = builder -> Finish(&arrayWithData);
                    columns[col] = arrayWithData;
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

                    // Build the Array
                    std::shared_ptr<arrow::FixedSizeBinaryType> type = std::make_shared<arrow::FixedSizeBinaryType>(numBytesForFixedArray);
                    std::shared_ptr<arrow::FixedSizeBinaryBuilder> builder = std::make_shared<arrow::FixedSizeBinaryBuilder>(m_loggingPool, type);
                    builder -> Init(numRowsInRowGroup);

                    for (int i = 0; i < numRowsInRowGroup; i++)
                    {
                        builder -> Append(&results[i]);
                    }

                    std::shared_ptr<arrow::Array> arrayWithData;
                    arrow::Status s = builder -> Finish(&arrayWithData);
                    columns[col] = arrayWithData;
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

                    // Build the Array
                    std::shared_ptr<arrow::BooleanType> type = std::make_shared<arrow::BooleanType>();
                    std::shared_ptr<arrow::BooleanBuilder> builder = std::make_shared<arrow::BooleanBuilder>(m_loggingPool, type);
                    builder -> Init(numRowsInRowGroup);

                    for (int i = 0; i < numRowsInRowGroup; i++)
                    {
                        printf("Read in value: %s\n", results[i] ? "true" : "false");
                        builder -> Append(results[i]);
                    }

                    std::shared_ptr<arrow::Array> arrayWithData;
                    arrow::Status s = builder -> Finish(&arrayWithData);
                    columns[col] = arrayWithData;
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

                    // Build the Array
                    std::shared_ptr<arrow::Int32Type> type = std::make_shared<arrow::Int32Type>();
                    std::shared_ptr<arrow::Int32Builder> builder = std::make_shared<arrow::Int32Builder>(m_loggingPool, type);
                    builder -> Init(numRowsInRowGroup);

                    for (int i = 0; i < numRowsInRowGroup; i++)
                    {
                        printf("Read in value: %i\n", results[i]);
                        builder -> Append(results[i]);
                    }

                    std::shared_ptr<arrow::Array> arrayWithData;
                    arrow::Status s = builder -> Finish(&arrayWithData);
                    columns[col] = arrayWithData;
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

                    // Build the Array
                    std::shared_ptr<arrow::Int64Type> type = std::make_shared<arrow::Int64Type>();
                    std::shared_ptr<arrow::Int64Builder> builder = std::make_shared<arrow::Int64Builder>(m_loggingPool, type);
                    builder -> Init(numRowsInRowGroup);

                    for (int i = 0; i < numRowsInRowGroup; i++)
                    {
                        printf("Read in value: %ld\n", results[i]);
                        builder -> Append(results[i]);
                    }

                    std::shared_ptr<arrow::Array> arrayWithData;
                    arrow::Status s = builder -> Finish(&arrayWithData);
                    columns[col] = arrayWithData;
                    break;
                }
                default:
                {
                    // TODO: Should throw an error
                    std::cout << "Support for " << physType << " file types will be added in the future." << std::endl;
                }
                // Support for other types will be added in the future
            } // switch
        } // column for loop
        // Return a constructed RecordBatch
        shared_ptr<arrow::RecordBatch> rb(new arrow::RecordBatch(arrowSchema, numRowsInRowGroup, columns));
        std::cout << "Returning a new RecordBatch, index: " << id << std::endl;
        return rb;
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