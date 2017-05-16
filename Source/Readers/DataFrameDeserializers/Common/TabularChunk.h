//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//

#pragma once

#include "DataDeserializerBase.h"

#include "Constants.h"

namespace Microsoft { namespace MSR { namespace CNTK { namespace DF {


// Transpose view for a chunk of data in memory. Given up to the randomizer.
struct TabularData : public DenseSequenceData
{
    TabularData(const std::shared_ptr<std::vector<double>>& buff, int row, int ncol) :
        m_buffer(buff), m_row(row), m_ncol(ncol) {}

    const void* GetDataBuffer() override
    {
        return m_buffer->data() + m_row * m_ncol;
    }

private:
    std::shared_ptr<std::vector<double>> m_buffer;
    int m_row;
    int m_ncol;
}

class TabularChunk : public Chunk
{
public:
    TabularChunk(const std::shared_ptr<TableChunk>& chunk, TensorShapePtr& layout)
    {
        // Convert to vector<SequenceDataPtr>
        auto nrow = chunk->num_rows();
        auto ncol = chunk->num_columns();
        m_dataptrs.reserve(nrow);
        m_data->reserve(nrow * ncol);
        // BAD LOOP - look into a better mechanism for this
        // "TransposeAllocator?"
        // working columnwise for better mem access pattern
        auto cols = chunk->columns();
        for (int c = 0; c < ncol; ++c)
        {
            const arrow::Array * arr = cols[c].get();
            auto type = arr->type_id();
            if (type = Type::type::DOUBLE)
            {
                auto darr = static_cast<const arrow::NumericArray<arrow::DoubleType> *>(arr)->raw_data();
                for (int r = 0; r < nrow; ++r)
                {
                    m_data->operator[](r * ncol + c) = *darr++;
                }
            }
            else
            {
                InvalidArgument("Unsupported array type %d", type);
            }
        }

        for (int r = 0; r < nrow; ++r)
        {
            auto dd = std::make_shared<TabularData>(m_data, r, ncol);
            dd -> m_numberOfSamples = 1;
            dd -> m_elementType = ElementType::tdouble;
            dd -> m_sampleLayout = layout;
            dd -> m_key = KeyType(r, 0);
            m_dataptrs.push_back(dd);
        }
    }

    // Gets data for the sequence.
    virtual void GetSequence(SequenceIdType sequenceId, vector<SequenceDataPtr>& result) override
    {
        result.push_back(m_dataptrs[sequenceId]);
    }

    // Unloads the data from memory.
    ~TabularChunk()
    {
        m_data->resize(0);
    }

private:
    DISABLE_COPY_AND_MOVE(TabularChunk);
    std::vector<SequenceDataPtr> m_dataptrs;
    std::shared_ptr<std::vector<double>> m_data;
};

}}}}
