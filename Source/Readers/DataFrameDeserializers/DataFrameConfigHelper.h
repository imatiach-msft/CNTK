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
#include "Constants.h"
#include "HDFSFileObjects.h"

namespace Microsoft { namespace MSR { namespace CNTK {

// A helper class for HDFS Deserializer configuration.
// Provides typed accessor to config parameters.
class HDFSConfigHelper
{
public:
    // TODO: Decide on what the constructor should take
    HDFSConfigHelper(const ConfigParameters& config) : m_config(config)
    {}

    // File location resolution parameters
    // TODO: Add in Hadoop conf -> auth + nameserver resolution
    // TODO: Understand WASB resolution via libHDFS
    std::wstring GetNameNodeAddress() const;
    uint32_t GetNameNodePort() const;
    std::wstring GetRelativeFilePath() const;

    // File properties (e.g. Parquet)
    FileFormat GetFileFormat() const;

    // Gets feature dimension.
    size_t GetFeatureDimension();

    // Gets label dimension.
    size_t GetLabelDimension();

    // Gets element type.
    // Currently both features and labels should be of the same type.
    ElementType GetElementType() const;

    // Gets randomization window.
    size_t GetRandomizationWindow();

    // Gets randomizer type - "auto" or "block"
    std::wstring GetRandomizer();

private:
    DISABLE_COPY_AND_MOVE(ConfigHelper);

    const ConfigParameters& m_config;
    FileFormat fileFormat;
};

}}}
