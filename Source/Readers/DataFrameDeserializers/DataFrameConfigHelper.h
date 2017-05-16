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

#include "Common/Constants.h"

namespace Microsoft { namespace MSR { namespace CNTK { namespace DF {

// A helper class for DataFrame Deserializer configuration.
// Provides typed accessor to config parameters.
class DataFrameConfigHelper
{
public:
    // TODO: Decide on what the constructor should take
    DataFrameConfigHelper(const ConfigParameters& config);

    // Datasource selection
    DataSource GetDataSource() const { return m_source; }
    FileFormat GetFormat() const { return m_format; }
    
    // Gets element type.
    // Currently both features and labels should be of the same type.
    ElementType GetElementType() const { return m_elementType; };

private:
    DISABLE_COPY_AND_MOVE(DataFrameConfigHelper);

    ConfigParameters m_config;

    // std::shared_ptr<RetryPolicy> m_retryPolicy;
    // std::shared_ptr<Logger> m_logger;

    FileFormat m_format;
    DataSource m_source;
    ElementType m_elementType;
};

}}}}
