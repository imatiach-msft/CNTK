//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//

#pragma once

#include <iostream>
#include <typeinfo>

#include "DataDeserializerBase.h"

#include "Constants.h"

namespace CNTK { namespace DF {

struct ArrayWrapperBase
{
    virtual ~ArrayWrapperBase() = default;
    virtual const void* GetArray() = 0;
};

template <typename T>
struct ArrayWrapper : public ArrayWrapperBase
{
    ArrayWrapper(T* const array) : m_array(array) {}
    
    virtual ~ArrayWrapper() override
    {
        // printf("In ArrayWrapper destructor!\n");
        delete[] m_array;
    }

    const void* GetArray() override
    {    
         return m_array;
    }

private:
    T* const m_array;
};

typedef std::shared_ptr<ArrayWrapperBase> ArrayWrapperPtr;

struct ChunkBuffer
{
    // std::vector<void*>  m_columnBuffers;
    std::vector<ArrayWrapperPtr>  m_columnBuffers;
    std::vector<size_t> m_columnDims;
    std::vector<bool>   m_isColSparse;
    std::vector<size_t> m_columnBufferIdxes;
    int64_t     m_numCols;
    int64_t     m_numRows;
    bool        m_isDouble;
    ChunkIdType m_chunkId;

    ChunkBuffer(ChunkIdType chunkId, int numRows, int numCols) :
      m_chunkId(chunkId), m_numRows(numRows), m_numCols(numCols)
    {
       m_columnDims.reserve(m_numCols);
       m_isColSparse.reserve(m_numCols);
       m_columnBufferIdxes.reserve(m_numCols);
    }

    // Unloads the data from memory.
    ~ChunkBuffer()
    {
        // std::cout << "destroying chunk buffers" << std::endl;
        // m_columnBuffers.clear(); // ArrayWrapperPtr is shared_ptr
    }  
};

template <typename T>
struct DenseFieldData : public DenseSequenceData
{
    DenseFieldData(const T* buffer, int row, int col, size_t rowSize) :
    m_buffer(buffer), m_row(row), m_col(col), m_rowSize(rowSize)
    {
        m_dim = NDShape(1, rowSize);
    }

    const void* GetDataBuffer() override
    {
      // std::cout << "Getting data buffer for Chunk at [ " << m_row << " ," << m_col << " ] " << std::endl; 
      // std::cout << "Index of data buffer for the above Chunk: " << (m_row * m_rowSize + m_displacement) << std::endl;
      /*
        if (m_col >= 0)
	{
          T *darry = m_buffer + (m_row * m_rowSize);
          for (int i = 0; i < m_rowSize; i++)
	  {
              std::cout << "Data from buffer: " << *darry << std::endl;
              darry++;
	  }
	  }
        **/
       return m_buffer + (m_row * m_rowSize);
    }

    const NDShape& GetSampleShape() override
    {
       return m_dim;
    } 

private:
    const T* m_buffer;
    int m_row;
    int m_col;
    size_t m_rowSize;
    NDShape m_dim;
};

template <typename T>
struct SparseFieldData : public SparseSequenceData
{
    SparseFieldData(const T* values, const int* indices, std::vector<int>& nnzCounts, int row, int col, size_t rowSize) :
      m_values(values), m_row(row), m_col(col)
    {
         SparseSequenceData::m_indices = const_cast<int*>(indices);
         SparseSequenceData::m_nnzCounts = nnzCounts,
         SparseSequenceData::m_totalNnzCount = 0;
         for (int i = 0; i < SparseSequenceData::m_nnzCounts.size(); ++i)
         {
	     SparseSequenceData::m_totalNnzCount =+ SparseSequenceData::m_nnzCounts[i];
         }
         // std::cout << "total nnz count is " << SparseSequenceData::m_totalNnzCount << std::endl;
         m_dim = NDShape(1, rowSize);    
    }

    const void* GetDataBuffer() override
    {
      // std::cout << "Getting sparse data buffer for Chunk at [ " << m_row << " ," << m_col << " ] disp: " << std::endl; 
      // std::cout << "Index of data buffer for the above Chunk: " << (m_row * m_rowSize + m_displacement) << std::endl;
      /*
        if (m_col >= 0)
	{
          T *darry = m_values;
          int* iarray = SparseSequenceData::m_indices;
          int nzCount =  SparseSequenceData::m_nnzCounts[0];
          for (int i = 0; i < nzCount; i++)
	  {
              std::cout << "Data from buffer: " << *darry << std::endl;
              std::cout << "Idx from buffer: " << *iarray << std::endl;
              darry++; iarray++;
	  }
	}
      **/
          return m_values;
    }

    const NDShape& GetSampleShape() override
    {
       return m_dim;
    } 

private:
    const T* m_values;
    int m_row;
    int m_col;
    NDShape m_dim;
};

template <class A_type> class TabularChunk : public Chunk
{
public:

  TabularChunk(const std::shared_ptr<ChunkBuffer>& chunk,  DataType precision, size_t rowStartIdx) 
    : m_chunkBuffer(chunk), m_precision(precision), m_rowStartIdx(rowStartIdx)
    {
        // TableChunk == arrow's RecordBatch.
        // At this point, TableChunk contains a vector of shared_ptrs to arrow::Arrays (Simply put, it contains all the data in a RowGroup)
        // Convert to vector<SequenceDataPtr>
        auto nrow = chunk->m_numRows;
        auto ncol = chunk->m_numCols;
        m_dataptrs.reserve(nrow * numCols); // For now, numSeq == numSamples, so 
        
        // if seq == samples in our case,
        // the number of times I need to push to m_dataptrs
        // the order in which I push to the streams should match the order of the streams
        // hence, push every time I read in from a column.

        // std::cout << "m_dataptrs size BEFORE is: " << m_dataptrs.size() << std::endl;
	std::vector<std::shared_ptr<std::vector<int>>> sparseColRowDisplacements;
        sparseColRowDisplacements.resize(numCols);
        for (int c = 0; c < numCols; ++c)
        {
            if (m_chunkBuffer -> m_isColSparse[c])
	    {
	        const int* numNZs = static_cast<const int*>(m_chunkBuffer -> m_columnBuffers[m_chunkBuffer->m_columnBufferIdxes[c]]->GetArray());
                std::shared_ptr<std::vector<int>> dispPtr(new std::vector<int>());
                int disp = 0;
                for (int r = 0; r < nrow; ++r)
		{
                  // std::cout << "disp = " << disp << std::endl;
		  dispPtr -> push_back(disp);
                  disp += *numNZs;
                  numNZs++;	  
                }
                dispPtr -> push_back(disp);
                sparseColRowDisplacements[c] = dispPtr;
		// std::cout << "dispPtr size" << sparseColRowDisplacements[c] -> size() << std::endl;
            }
        }

        for (int r = 0; r < nrow; ++r)
        {
            // We only support two input cols - features and labels
            for (int c = 0; c < numCols; ++c)
            {
                // NOTE: Once again, this assumes that the features columns come before labels columns
                // std::cout << "Before make_shared<TabularData>" << std::endl;
              if (m_chunkBuffer -> m_isColSparse[c])
	      {
		int disp = sparseColRowDisplacements[c] -> operator[](r);
                auto bufferColIdx = m_chunkBuffer->m_columnBufferIdxes[c];
                int nnzCount =  sparseColRowDisplacements[c] -> operator[](r + 1) - disp;
		std::vector<int> nnzCounts({nnzCount});
                auto sd = std::make_shared<SparseFieldData<A_type>>(static_cast<const A_type*>(m_chunkBuffer->m_columnBuffers[bufferColIdx + 2]->GetArray()) + disp, 
								    static_cast<const int*>(m_chunkBuffer->m_columnBuffers[bufferColIdx + 1]->GetArray()) + disp,
                                                                    nnzCounts, r, c, m_chunkBuffer->m_columnDims[c]);
                sd->m_numberOfSamples = 1;
                sd->m_elementType = m_precision; // should get from brainscript
                // sd->m_sampleLayout = make_shared<TensorShape>(m_chunkBuffer->m_columnDims[c]);
                // std::cout << "TensorShape:" <<  dd->m_sampleLayout->GetNumElements() << std::endl;
                sd->m_key = SequenceKey(r + rowStartIdx, c);
                m_dataptrs.push_back(sd);
	      }
              else
	      {
	        auto dd = std::make_shared<DenseFieldData<A_type>>(static_cast<const A_type*>(m_chunkBuffer->m_columnBuffers[m_chunkBuffer->m_columnBufferIdxes[c]]->GetArray()), 
                                                                   r, c, m_chunkBuffer->m_columnDims[c]);
                dd->m_numberOfSamples = 1;
                dd->m_elementType = m_precision; // should get from brainscript
                // dd->m_sampleLayout = make_shared<TensorShape>(m_chunkBuffer->m_columnDims[c]);
                // std::cout << "TensorShape:" <<  dd->m_sampleLayout->GetNumElements() << std::endl;
                dd->m_key = SequenceKey(r + rowStartIdx, c);
                m_dataptrs.push_back(dd);
	      }
            }
        }
        // std::cout << "m_dataptrs size AFTER is: " << m_dataptrs.size() << std::endl;
    }

    // Gets data for the sequence.
    virtual void GetSequence(SequenceIdType sequenceId, vector<SequenceDataPtr>& result) override
    {
        // std::cout << "seqID: " << sequenceId << " In TB::GetSequence" << std::endl;
        // std::cout << "m_dataptrs.size() = " << m_dataptrs.size() << std::endl;
        // TODO: NOTE! the number of times you push into the result vector must match the number of streams.
        //       The indexes in m_dataptrs PROBABLY matches the sample layout of the input streams.
        // NOTE! this configuration requires features columns first, then labels columns.
        result.push_back(m_dataptrs[sequenceId * numCols]); // features columns
        result.push_back(m_dataptrs[sequenceId * numCols + 1]); // labels columns
    }

    // Unloads the data from memory.
    ~TabularChunk()
    {
        // std::cout << "destroying chunk in TB" << std::endl;
        // m_dataptrs.clear(); // SequenceDataPtr is shared_ptr
    }

private:
    DISABLE_COPY_AND_MOVE(TabularChunk);
    std::vector<SequenceDataPtr> m_dataptrs;
    std::shared_ptr<std::vector<A_type>> m_data;
    DataType m_precision;
    size_t m_featureDim;
    size_t m_labelDim;
    size_t m_rowStartIdx;
    const int numCols = 2; // Number of columns - we only support two columns for now.
    const std::shared_ptr<ChunkBuffer> m_chunkBuffer;
};

}}
