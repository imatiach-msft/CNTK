//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//

#pragma once

#define __STDC_FORMAT_MACROS
#include "Constants.h"
#include "TableMetadata.h"
#include "../../ReaderLib/DataDeserializer.h"

namespace Microsoft { namespace MSR { namespace CNTK { namespace DF {

class FileProvider
{
public:
    virtual ~FileProvider() {}
    virtual FileList GetFileList() = 0;
};

class FileReader
{
public:
    virtual ~FileReader() {}
    virtual std::shared_ptr<TableMetadata> InitializeSources(const FileList& sources) = 0;
    virtual std::shared_ptr<TableChunk> GetChunk(ChunkIdType id) = 0;
};

class TableTransformer
{
public:
    virtual ~TableTransformer() {}
    virtual std::shared_ptr<TableChunk> Transform(std::shared_ptr<TableChunk> input) = 0;
};

}}}}
