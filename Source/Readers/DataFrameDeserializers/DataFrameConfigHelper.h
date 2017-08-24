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
    DataFrameConfigHelper(const ConfigParameters& config);

    // Credentials for HDFS connections
    // const std::string host,std::string path, int port,
    void GetHDFSConfig(std::string& host, std::string& filePath, int& port);
    size_t GetInputDimension(InputType type);

    // Stream configurations
    void GetFeatureConfigs(size_t& featDim, StorageType& elemType);
    void GetLabelConfigs(size_t& labelDim, StorageType& elemType);
    void GetHdfsConfigs(std::string& host, std::string& filePath, int& port, FileFormat& fileFormat);

    // Datasource selection
    DataSource GetDataSource() const { return m_source; }
    FileFormat GetFormat() const { return m_format; }

private:
    DISABLE_COPY_AND_MOVE(DataFrameConfigHelper);

    const ConfigParameters& m_config;

    // std::shared_ptr<RetryPolicy> m_retryPolicy;
    // std::shared_ptr<Logger> m_logger;

    // Connection configurations
    std::string    m_host;
    std::string    m_filePath;
    int            m_port;
    FileFormat     m_format;
    DataSource     m_source;

    // Stream configurations
    StorageType    m_labelElemType;
    size_t         m_labelDim;
    StorageType    m_featureElemType;
    size_t         m_featureDim;

};

}}}}
