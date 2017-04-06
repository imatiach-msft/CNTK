//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//
#include <iostream>

#include "ParquetReader.h"
#include "../DataFrameConfigHelper.h"

namespace CNTK { namespace DF
{
    static int64_t ReadDoubleColumn(parquet::DoubleReader* reader, int64_t valuesToRead, double* values)
    {
         // printf("Reading Doubles outlined!\n");
         int64_t valuesAlreadyRead = 0;
         int64_t valuesRead;
         int totalRead = 1;
         while (totalRead > 0)
	 {
             totalRead = reader -> ReadBatch(valuesToRead, nullptr, nullptr, values + valuesAlreadyRead, &valuesRead);
             valuesAlreadyRead += valuesRead;
	 }
         // assert(valuesAlreadyRead == valuesToRead);
         return valuesAlreadyRead;
    }

    static int64_t ReadInt32Column(parquet::Int32Reader* reader, int64_t valuesToRead, int32_t* values)
    {
         // printf("Reading Int32s outlined!\n");
         int64_t valuesAlreadyRead = 0;
         int64_t valuesRead;
         int totalRead = 1;
         while (totalRead > 0)
	 {
             totalRead = reader -> ReadBatch(valuesToRead, nullptr, nullptr, values + valuesAlreadyRead, &valuesRead);
             valuesAlreadyRead += valuesRead;
	 }
         // assert(valuesAlreadyRead == valuesToRead);
         return valuesAlreadyRead;
    }

    static int64_t ReadFloatColumn(parquet::FloatReader* reader, int64_t valuesToRead, float* values)
    {
        // printf("Reading Float outlined!\n");
         int64_t valuesAlreadyRead = 0;
         int64_t valuesRead;
         int totalRead = 1;
         while (totalRead > 0)
	 {
             totalRead = reader -> ReadBatch(valuesToRead, nullptr, nullptr, values + valuesAlreadyRead, &valuesRead);
             valuesAlreadyRead += valuesRead;
	 }
         // assert(valuesAlreadyRead == valuesToRead);
         return valuesAlreadyRead;
    }

    static bool CheckValueColumnType(DataType expected, parquet::Type::type actual)
    {
         if (expected == DataType::Double && actual != parquet::Type::DOUBLE)
	 {
	     return false;
	 }
         if (expected == DataType::Float && actual != parquet::Type::FLOAT)
	 {
             return false;
	 }
         return true;
    }

    static int64_t ReadSizeColumn(std::shared_ptr<parquet::ColumnReader> reader, int64_t valuesToRead, std::shared_ptr<ChunkBuffer> buffer, int64_t& totalNzs)
    {
         parquet::Int32Reader* int32Reader = dynamic_cast<parquet::Int32Reader*>(reader.get());
         if (int32Reader == nullptr)
	 { 
             InvalidArgument("Parquet column data is expected to be in the int32_t type.");
         }
         int64_t valuesAlreadyRead = 0;
         int32_t* nonZeros =  new int32_t[valuesToRead];
         valuesAlreadyRead = ReadInt32Column(int32Reader, valuesToRead, nonZeros);
         assert(valuesAlreadyRead == valuesToRead);
         auto wrapper = std::shared_ptr<ArrayWrapper<int32_t>>(new ArrayWrapper<int32_t>(nonZeros));
         buffer -> m_columnBuffers.push_back(wrapper);

         // Compute the total number of nz values
         totalNzs = 0;
         for (int64_t i = 0; i < valuesAlreadyRead; ++i)
	 {
	    totalNzs += nonZeros[i];
         }
         
         return valuesAlreadyRead;
    }

    static int64_t ReadIndexColumn(std::shared_ptr<parquet::ColumnReader> reader, int64_t valuesToRead, std::shared_ptr<ChunkBuffer> buffer)
    {
         parquet::Int32Reader* int32Reader = dynamic_cast<parquet::Int32Reader*>(reader.get());
         if (int32Reader == nullptr)
	 { 
             InvalidArgument("Parquet column data is expected to be in the int32_t type.");
         }
         int64_t valuesAlreadyRead = 0;
         int32_t* indices =  new int32_t[valuesToRead];
         valuesAlreadyRead = ReadInt32Column(int32Reader, valuesToRead, indices);
         assert(valuesAlreadyRead == valuesToRead);
         auto wrapper = std::shared_ptr<ArrayWrapper<int32_t>>(new ArrayWrapper<int32_t>(indices));
         buffer -> m_columnBuffers.push_back(wrapper);
         
         return valuesAlreadyRead;
    }

    static int64_t ReadValueColumn(std::shared_ptr<parquet::ColumnReader> reader, DataType expectedType, parquet::Type::type actualType, int64_t valuesToRead, std::shared_ptr<ChunkBuffer> buffer)
    {
         // printf("Reading values outlined!\n");
         if (!CheckValueColumnType(expectedType, actualType))
	 {
              InvalidArgument("Parquet column data is not serialized in the specified type.");
	 }
         int64_t valuesAlreadyRead = 0;
         switch (actualType)
         {
              case parquet::Type::FLOAT:
	      {
                   parquet::FloatReader* floatReader = static_cast<parquet::FloatReader*>(reader.get());
                   float* values =  new float[valuesToRead];
                   valuesAlreadyRead = ReadFloatColumn(floatReader, valuesToRead, values);
                   auto wrapper = std::shared_ptr<ArrayWrapper<float>>(new ArrayWrapper<float>(values));
                   buffer -> m_columnBuffers.push_back(wrapper);
                   buffer -> m_isDouble = false;
                   break;
              }
              case parquet::Type::DOUBLE:
              {
                   parquet::DoubleReader* doubleReader = static_cast<parquet::DoubleReader*>(reader.get());
                   double* values =  new double[valuesToRead];
                   valuesAlreadyRead = ReadDoubleColumn(doubleReader, valuesToRead, values);
                   auto wrapper = std::shared_ptr<ArrayWrapper<double>>(new ArrayWrapper<double>(values));
                   buffer -> m_columnBuffers.push_back(wrapper);
                   buffer -> m_isDouble = true;
                   break;
              }
              default:
              {
                   InvalidArgument("Parquet column data is serialized in an unsupported type.");
              }
         }     
         assert(valuesAlreadyRead == valuesToRead);

         return valuesAlreadyRead;
    }

    // when returning a BatchRecord, we need to instantiate and populate vectors of type Array, which takes a dependency
    // on buffer & memory pools.
    // TODO: Decide on the size of the buffer + memory size for optimization
    ParquetReader::ParquetReader(const ConfigParameters& config) : m_config(config) 
    {
        m_pool = new arrow::DefaultMemoryPool();
        m_loggingPool = new arrow::LoggingMemoryPool(m_pool);

        // ConfigParameters input = config(L"input");
        // auto inputName = input.GetMemberIds().front();
        // ConfigParameters streamConfig = input(inputName);
        DataFrameConfigHelper configHelper(config);
        m_featureDim = configHelper.GetInputDimension(InputType::Features);
        m_labelDim = configHelper.GetInputDimension(InputType::Labels);
        m_isFeatureSparse = configHelper.IsInputSparse(InputType::Features);
        m_isLabelSparse = configHelper.IsInputSparse(InputType::Labels);
        std::wstring precision = config(L"precision", L"double");
	// std::wcout << "precision = " << precision <<std::endl;
        m_precision = configHelper.ResolveTargetType(precision);
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
	    // std::printf("initializing source from index %d\n", i);
            // std::cout << "NOW READING filePtr: " << sources[i] << std::endl;
            std::unique_ptr<parquet::ParquetFileReader> pfr =
                parquet::ParquetFileReader::Open(sources[i]);
            // std::cout << "OPENED PQ FILE" << std::endl;
            // std::cout << "pushed the file into the m_fileReaders vector" << std::endl;

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
        // std::cout << "In da calculateindexes" << std::endl;
        for (int i = 0; i < m_rowGroupsPerFile.size(); i++)
        {
	    // std::cout << "m_rowGroupsPerFile[" << i << "] = " << m_rowGroupsPerFile[i] << std::endl;
	    // std::cout << "chunkidtype id: " << id << std::endl;
            if (id < m_rowGroupsPerFile[i])
	    {
                 fileIndex = i;
                 rowGroupIndex = id;
                 break;
            }
            else
	    {
                 id -= m_rowGroupsPerFile[i];
            }
        }
    }

    // In our current setup, chunkId == rowGroup number in a parquet file
    std::shared_ptr<ChunkBuffer> ParquetReader::GetChunkBuffer(ChunkIdType id)
    {
        // ChunkIdType is just a uint32_t variable used by CNTK,
        // so we need to keep track of the number of files and their respective
        // rowgroup counts to retrieve the chunk from correct file + rowGroupNo.

        // std::cout << "IN PQREADER::GETCHUNK WITH ID: " << id << std::endl; 

        int fileIndex = 0;
        int rowGroupIndex = 0;
        CalculateIndexes(id, fileIndex, rowGroupIndex);
        // std::cout << "calculated indexes: fileIndex = " << fileIndex << " rowGroupIndex = " << rowGroupIndex << std::endl;
        parquet::ParquetFileReader* pfr = m_fileReaders[fileIndex].get(); // used .get() to not take away ownership of the unique_ptr from the vector
        std::shared_ptr<parquet::FileMetaData> md = pfr -> metadata();
        int numCols = md -> num_columns();
        std::vector<size_t> rowSizes = {m_featureDim, m_labelDim};
	std::vector<bool> isColSparse = {m_isFeatureSparse, m_isLabelSparse};

        const parquet::SchemaDescriptor* schema = md -> schema();
        
        std::unique_ptr<parquet::RowGroupMetaData> rgMetadata = md -> RowGroup(rowGroupIndex);
        std::shared_ptr<parquet::RowGroupReader> rgr = pfr -> RowGroup(rowGroupIndex);
        int64_t numRowsInRowGroup = rgMetadata -> num_rows();

        // std::cout << "Before entering the reading LOOOP: numRows =" << numRowsInRowGroup << std::endl;
	std::shared_ptr<ChunkBuffer> buffer(new ChunkBuffer(id, numRowsInRowGroup, rowSizes.size()));
        // Loop through the columns in the Row Group
        int col = 0;
        for (int idx = 0; idx < rowSizes.size(); idx++)
        {
          if (isColSparse[idx])
          { 
            
	    // std::cout << "Sparse col: " << col << std::endl;
	    // Read the number of nonzeroes first
            int64_t valuesToRead = numRowsInRowGroup;
        
            // Read the number of nonzeroes
            std::shared_ptr<parquet::ColumnReader> cr = rgr -> Column(col);
            ReadSizeColumn(cr, valuesToRead, buffer, valuesToRead);
            buffer -> m_columnBufferIdxes.push_back(col);

            // Read the indices
            col++;
            std::shared_ptr<parquet::ColumnReader> cr1 = rgr -> Column(col);
            ReadIndexColumn(cr1, valuesToRead, buffer);

            col++;
            std::shared_ptr<parquet::ColumnReader> cr2 = rgr -> Column(col);
            const parquet::ColumnDescriptor* colDescriptor = GetColumnDescriptor(schema, col);
            parquet::Type::type physType = colDescriptor -> physical_type();
            ReadValueColumn(cr2, m_precision, physType, valuesToRead, buffer);
            
            buffer -> m_columnDims.push_back(rowSizes[idx]);
            buffer -> m_isColSparse.push_back(true);
          }
          else // the dense case
          {
	    size_t rowSize = rowSizes[idx];
            // std::cout << "row size = " << rowSize << std::endl;
            std::shared_ptr<parquet::ColumnReader> cr = rgr -> Column(col);
            const parquet::ColumnDescriptor* colDescriptor = GetColumnDescriptor(schema, col);
            // Cast the ColumnReader to the correct column
            parquet::Type::type physType = colDescriptor -> physical_type();
           
	    // std::cout << "col name = " << colDescriptor -> name() << " type length = " << colDescriptor -> type_length() << std::endl;
            // std::cout << "max_def_level = " << colDescriptor -> max_definition_level() << " max_rep_level = " << colDescriptor ->  max_repetition_level() << std::endl;
            // Depending on the physical type of the column, cast the Column Reader to the correct type,
            // read the data into an arrow::Array and push it into the columns vector.
            int64_t valuesToRead = numRowsInRowGroup * rowSize;
            ReadValueColumn(cr, m_precision, physType, valuesToRead, buffer);
            
            buffer -> m_columnBufferIdxes.push_back(col);
            buffer -> m_columnDims.push_back(rowSize);
            buffer -> m_isColSparse.push_back(false);
	  } // if 
          
          col++;
        } // column for loop
        // Return a constructed RecordBatch
        // std::cout << "Returning a new RecordBatch, index: " << id << std::endl;
        return buffer;
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

} // hdfs
}
