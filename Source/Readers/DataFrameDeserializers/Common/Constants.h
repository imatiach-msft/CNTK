//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//
#pragma once

#include <utility>
#include <string>
#include <vector>
#include <set>

#include "Config.h"
#include "Reader.h"

#undef UNUSED
#include "arrow/io/interfaces.h"
#include "arrow/io/hdfs.h"
#include "parquet/api/reader.h"
#include "parquet/arrow/schema.h"
#include "arrow/api.h"

namespace CNTK { namespace DF {

// make this into a scoped logger
#define LOG(f, v) do { fprintf(stderr, "[Info]:\t"); fprintf(stderr, (f), (v)); } while(0)
#define ERR(f, v) do { fprintf(stderr, "[Error]:\t"); fprintf(stderr, (f), (v)); } while(0)

enum class FileFormat: uint8_t
{
    Parquet = 0,
    Unknown = (uint8_t)(-1)
};

enum class DataSource: uint8_t
{
    HDFS = 0,
    Unknown = (uint8_t)(-1)
};

enum class InputType: uint8_t
{
    Features = 0,
    Labels = 1,
    Unknown = (uint8_t)(-1)
};

const std::wstring CLASS_TYPE_NAME = L"DataFrameDeserializer";

typedef arrow::io::ReadableFileInterface File;
typedef arrow::RecordBatch TableChunk;
typedef arrow::Schema Schema;
// typedef arrow::DataType DataType;
typedef arrow::Type Type;

typedef std::vector<std::shared_ptr<File>> FileList;

// Chunk is uint32_t - any reason?
// Default HDFS partition size -> 64TB?
typedef size_t SequenceIdType;

}}
