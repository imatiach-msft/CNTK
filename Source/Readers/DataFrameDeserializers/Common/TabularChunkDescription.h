//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//
/* Unused
#pragma once

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include "DataDeserializer.h"

namespace Microsoft { namespace MSR { namespace CNTK { namespace DF {

class TabularChunkDescription
{
    // Total number of rows in this chunk
    size_t m_numRows;

    // Chunk id.
    ChunkIdType m_chunkId;

public:

    TabularChunkDescription(ChunkIdType chunkId, size_t nrow) : 
        m_chunkId(chunkId), m_numRows(nrow) { }

    // Gets number of utterances in the chunk.
    size_t NumberOfRows() const
    {
        return m_numRows;
    }

    ChunkIdType GetChunkId() const
    {
        return m_chunkId;
    }
};
*/
}}}}
