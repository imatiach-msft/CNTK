//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//
#pragma once

#include <utility>
#include <string>
#include <vector>
#include "Config.h"
#include "Reader.h"

namespace Microsoft { namespace MSR { namespace CNTK {

namespace hdfs {

enum class FileFormat: uint8_t
{
    Parquet = 0,
    Unknown = (uint8_t)(-1)
}

enum class FileSystem: uint8_t
{
    HDFS = 0,
    Unknown = (uint8_t)(-1)
}

const std::wstring CLASS_TYPE_NAME = L"HDFSDeserializer";

const int BATCH_SIZE = 10; // For ColumnReader num rows per read

}
}}}
