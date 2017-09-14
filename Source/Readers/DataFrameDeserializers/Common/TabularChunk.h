//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//

#pragma once

#include <iostream>
#include <typeinfo>

#include "DataDeserializerBase.h"

#include "Constants.h"

namespace Microsoft { namespace MSR { namespace CNTK { namespace DF {


// Transpose view for a chunk of data in memory. Given up to the randomizer.
template <typename T>
struct TabularData : public DenseSequenceData
{
  TabularData(const std::shared_ptr<std::vector<T>>buff, int row, int col, int ncol, size_t rowSize, size_t displacement) :
    m_buffer(buff), m_row(row), m_col(col), m_ncol(ncol), m_rowSize(rowSize), m_displacement(displacement)
        {
	  /*
            for (int i = 0; i < m_buffer -> size(); i++)
            std::cout << "Data from buffer at index (" << i << "): " << m_buffer -> operator[](i) << std::endl;
	  */
        }

    const void* GetDataBuffer() override
    {
        // std::cout << "Getting data buffer for Chunk at [ " << m_row << " ," << m_col << " ] disp: " << m_displacement << std::endl; 
        // std::cout << "Index of data buffer for the above Chunk: " << (m_row * m_rowSize + m_displacement) << std::endl;
        // return m_buffer->data() + (m_row * m_ncol + m_col);
        /**
	std::vector<int> colDims {2, 3};
        if (m_col >= 0)
	{
          T *darry = m_buffer->data() + (m_row * m_rowSize + m_displacement);
          for (int i = 0; i < colDims[m_col]; i++)
	  {
              std::cout << "Data from buffer: " << *darry << std::endl;
              darry++;
	  }
	  }**/
        return m_buffer->data() + (m_row * m_rowSize + m_displacement);
    }

private:
    std::shared_ptr<std::vector<T>> m_buffer;
    int m_row;
    int m_col;
    int m_ncol;
    size_t m_rowSize;
    size_t m_displacement;
};

template <class A_type> class TabularChunk : public Chunk
{
public:

  TabularChunk(const std::shared_ptr<TableChunk>& chunk, size_t featureDim, size_t labelDim, ElementType precision, size_t rowStartIdx) 
    : m_precision(precision), m_featureDim(featureDim), m_labelDim(labelDim), m_rowStartIdx(rowStartIdx)
    {
        // TableChunk == arrow's RecordBatch.
        // At this point, TableChunk contains a vector of shared_ptrs to arrow::Arrays (Simply put, it contains all the data in a RowGroup)
        // Convert to vector<SequenceDataPtr>
        auto nrow = chunk->num_rows();
        auto ncol = chunk->num_columns();
        std::cout << "nrow: " << nrow << "ncol: " << ncol << std::endl;
        m_dataptrs.reserve(nrow * numCols); // For now, numSeq == numSamples, so 
        std::cout << "reserving dataptrs" << std::endl;
        
        size_t rowSize = m_featureDim + m_labelDim;
	std::vector<size_t> colDims {m_featureDim, m_labelDim};   
        std::vector<size_t> disp {0, m_featureDim};
        
        m_data = std::make_shared<std::vector<A_type>>(nrow * rowSize); // make it a template
        
        std::cout << "reserving data, m_data's capacity is: " << m_data -> capacity() << " m_data of zero is: " << m_data -> operator[](0) << std::endl;

        // BAD LOOP - look into a better mechanism for this
        // "TransposeAllocator?"
        // working columnwise for better mem access pattern
        //auto cols = chunk->columns();
        std::cout << "reserved dataptrs and data and now entering THE LOOP" << std::endl;
        
        for (int c = 0; c < numCols; ++c)
        {
            std::cout << "in THE LOOP, at col: " << c << " " << std::endl;
            std::shared_ptr<arrow::Array> arr = chunk -> column(c);
            //   const arrow::Array* arr = cols[c].get();
            std::cout << "got the arrow::Array!" << std::endl;    
            const Type::type type = arr->type_id();
            if (type == Type::type::DOUBLE)
            {
                // This contains entries from each column * numRowsInRowGroup
                const double* darr = std::static_pointer_cast<arrow::NumericArray<arrow::DoubleType>>(arr) -> raw_data();

                std::cout << "In DOUBLE. arr type is: " << typeid(arr).name() << std::endl;
               // std::cout << "darr type is: " << typeid((std::static_pointer_cast<std::shared_ptr<arrow::NumericArray<arrow::DoubleType>>>(arr)) -> raw_data()).name() << std::endl;

                for (int r = 0; r < nrow; ++r)
                {
		  // std::cout << "Retrieving the raw data from row: " << r << std::endl;   
		  // std::cout << "nrow * ncol = " << nrow * ncol << std::endl;
		  // std::cout << "r * ncol + c = " << r * ncol + c << std::endl;
                    // TODO: LOOP THROUGH ARRAY-
                    // m_data->operator[](r * ncol + c) = *darr;
                    // darr++;
                    size_t startIdx = r * rowSize;
                    for (int i = 0; i < colDims[c]; ++i)
                    {
                        m_data->operator[](startIdx + disp[c] +i) = *darr;
                        darr++;
                    }
                    // std::cout << "RETRIEVED THE DATA! " << std::endl;
                }
            }
            else if (type == Type::type::FLOAT)
            {
                const float* darr = std::static_pointer_cast<arrow::NumericArray<arrow::FloatType>>(arr) -> raw_data();

                std::cout << "In FLOAT. arr type is: " << typeid(arr).name() << std::endl;
               // std::cout << "darr type is: " << typeid((std::static_pointer_cast<std::shared_ptr<arrow::NumericArray<arrow::DoubleType>>>(arr)) -> raw_data()).name() << std::endl;

                for (int r = 0; r < nrow; ++r)
                {
                    std::cout << "Retrieving the raw data from row: " << r << std::endl;   
                    std::cout << "nrow * ncol = " << nrow * ncol << std::endl;
                    std::cout << "r * ncol + c = " << r * ncol + c << std::endl;
                    std::cout << "Value for *darr = " << *darr << std::endl;
                    // m_data->operator[](r * ncol + c) = *darr;
                    // darr++;
                    size_t startIdx = r * rowSize;
                    for (int i = 0; i < colDims[c]; ++i)
                    {
		        m_data->operator[](startIdx + disp[c] +i) = *darr;
                        darr++;
                    }
                    std::cout << "Value stored in m_data = " << m_data->operator[](r*ncol+c) << std::endl;
                    std::cout << "RETRIEVED THE DATA! " << std::endl;    
                }          
            }
            else
            {
                InvalidArgument("Unsupported array type %d", type);
            }
            
            std::cout << "DONE RETRIEVING THE DATA" << std::endl;
        }

        // Now, we are done storing our data in m_data, now we need to push into m_dataptrs

        // if seq == samples in our case,
        // the number of times I need to push to m_dataptrs
        // the order in which I push to the streams should match the order of the streams
        // hence, push every time I read in from a column.

        // TODO: Vector logic
        std::cout << "m_dataptrs size BEFORE is: " << m_dataptrs.size() << std::endl;
        for (int r = 0; r < nrow; ++r)
        {
            // We only support two input cols - features and labels
            for (int c = 0; c < numCols; ++c)
            {
                // NOTE: Once again, this assumes that the features columns come before labels columns
                // std::cout << "Before make_shared<TabularData>" << std::endl;
                auto dd = std::make_shared<TabularData<A_type>>(m_data, r, c, ncol, rowSize, disp[c]);
                // std::cout << "DONE MAKING <TabularData>" << std::endl;

                dd->m_numberOfSamples = 1;
                dd->m_elementType = m_precision; // should get from brainscript
                dd->m_sampleLayout = make_shared<TensorShape>(colDims[c]);
                dd->m_key = KeyType(r + rowStartIdx, c);
                m_dataptrs.push_back(dd);
            }
        }
        std::cout << "m_dataptrs size AFTER is: " << m_dataptrs.size() << std::endl;
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
        std::cout << "destroying chunk in TB" << std::endl;
        //m_data->resize(0);
        m_dataptrs.clear();
    }

private:
    DISABLE_COPY_AND_MOVE(TabularChunk);
    std::vector<SequenceDataPtr> m_dataptrs;
    std::shared_ptr<std::vector<A_type>> m_data;
    ElementType m_precision;
    size_t m_featureDim;
    size_t m_labelDim;
    size_t m_rowStartIdx;
    const int numCols = 2; // Number of columns - we only support two columns for now.
};

}}}}
