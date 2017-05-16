//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//

#pragma once

#include <numeric>

#include "Constants.h"

namespace Microsoft { namespace MSR { namespace CNTK { namespace DF {

// Wrapper metadata view for this table's schema - used to initialize streams
// Can also be used to validate all RFIs adhere to same schema and error out if not
class TableMetadata
{
public:
    TableMetadata(std::shared_ptr<Schema> schema, const std::vector<size_t>& rowCounts) :
        m_schema(schema), m_rowcounts(rowCounts) {}

    // Only int in arrow
    int NumberOfColumns() const { return m_schema->num_fields(); }
    std::wstring GetColumnName(int col) const { return std::wstring(m_schema->field(col)->name()); }
    std::shared_ptr<DataType> GetColumnType(int col) const { return m_schema->field(col)->type(); }

    size_t NumberOfRowsInChunk(int rowgroup) const { return m_rowcounts[rowgroup]; }
    size_t NumberOfRowChunks() const { return m_rowcounts.size(); }
    size_t NumberOfRows() const { return std::accumulate(m_rowcounts.begin(), m_rowcounts.end(), 0); }

    ~TableMetadata() {}

private:
    DISABLE_COPY_AND_MOVE(TableMetadata);
    std::shared_ptr<Schema> m_schema;
    std::vector<size_t> m_rowcounts;
};

}}}}
