//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//
#include "stdafx.h"

#include "DataFrameConfigHelper.h"
#include "StringUtil.h"
#include "ConfigUtil.h"
#include <iostream>

namespace CNTK { namespace DF {

using namespace std;

void DataFrameConfigHelper::GetFeatureConfigs(size_t& featDim, StorageFormat& elemType)
{
    if (!m_config.Exists(L"features")) 
    {
        InvalidArgument("Features property is missing. Please specify feature information in your BrainScript.");
    }

    ConfigParameters featuresInput = m_config(L"features");

    if (!featuresInput.Exists(L"dim"))
    {
        InvalidArgument("Features must specify dimension: 'dim' property is missing.");
    }

    if (!featuresInput.Exists(L"format"))
    {
        InvalidArgument("Features must specify format: 'format' property is missing.");        
    }
    
    featDim = std::stoi(featuresInput(L"dim"));
    std::wstring format = featuresInput(L"format");
    if (Microsoft::MSR::CNTK::AreEqualIgnoreCase(format, L"dense"))
    {
         elemType = StorageFormat::Dense;
    }
    else if (Microsoft::MSR::CNTK::AreEqualIgnoreCase(format, L"sparse"))
    {
         elemType = StorageFormat::SparseCSC;
    }
    else
    {
         InvalidArgument("'format' property has invalid values (either 'dense' or 'sparse').");
    }
}

void DataFrameConfigHelper::GetLabelConfigs(size_t& labelDim, StorageFormat& elemType)
{
    if (!m_config.Exists(L"labels"))
    {
        InvalidArgument("Labels property is missing. Please specify label information in your BrainScript.");
    }

    ConfigParameters labelsConfig = m_config(L"labels");

    if (!labelsConfig.Exists(L"dim"))
    {
        InvalidArgument("Labels must specify dimension: 'dim' property is missing.");
    }

    if (!labelsConfig.Exists(L"format"))
    {
        InvalidArgument("Labels must specify format: 'format' property is missing.");
    }

    labelDim = std::stoi(labelsConfig(L"dim"));
    std::wstring format = labelsConfig(L"format");
    if (Microsoft::MSR::CNTK::AreEqualIgnoreCase(format, L"dense"))
    {
         elemType = StorageFormat::Dense;
    }
    else if (Microsoft::MSR::CNTK::AreEqualIgnoreCase(format, L"sparse"))
    {
         elemType = StorageFormat::SparseCSC;
    }
    else
    {
         InvalidArgument("'format' property has invalid values (either 'dense' or 'sparse').");
    }
}

void DataFrameConfigHelper::GetHdfsConfigs(std::string& host, std::string& filePath, int& port, FileFormat& fileFormat)
{
    if (!m_config.Exists(L"hdfs"))
    {
        InvalidArgument("No HDFS configurations were specified. Please specify HDFS configuration in your BrainScript.");
    }

    ConfigParameters hdfsConfig = m_config(L"hdfs");

    if (!hdfsConfig.Exists(L"host"))
    {
        InvalidArgument("HDFS configurations must specify host: 'host' property is missing.");
    }

    if (!hdfsConfig.Exists(L"filePath"))
    {
        InvalidArgument("HDFS configurations must specify file path: 'filePath' property is missing.");
    }

    if (!hdfsConfig.Exists(L"port"))
    {
        InvalidArgument("HDFS configurations must specify port number: 'port' property is missing.");
    }

    if (!hdfsConfig.Exists(L"format"))
    {
        InvalidArgument("HDFS configurations must specify file format: 'format' property is missing.");
    }

    m_host = hdfsConfig(L"host");
    m_filePath = hdfsConfig(L"filePath");
    m_port = std::stoi(hdfsConfig(L"port"));
    m_format = FileFormat::Parquet;  // currently only supports parquet file formats
}

// Helper class to help parse BrainScript for stream configurations
DataFrameConfigHelper::DataFrameConfigHelper(const ConfigParameters& config) : m_config(config)
{
    // cout << "In DFConfig Helper" << endl;
    // m_config.dump();

    // Parse connection configurations
    m_source = DataSource::HDFS;
    GetHdfsConfigs(m_host, m_filePath, m_port, m_format);

    // Parse stream configurations
    GetLabelConfigs(m_labelDim, m_labelElemType);
    GetFeatureConfigs(m_featureDim, m_featureElemType);
}

void DataFrameConfigHelper::GetHDFSConfig(std::string& host, std::string& path, int& port)
{
    host = m_host;
    path = m_filePath;
    port = m_port;
}

size_t DataFrameConfigHelper::GetInputDimension(InputType type)
{
    if (type == InputType::Features)
    {
        return m_featureDim;
    } 
    
    return m_labelDim;
}

StorageFormat DataFrameConfigHelper::GetInputStorageType(InputType type)
{
    if (type == InputType::Features)
    {
        return m_featureElemType;
    } 
    
    return m_labelElemType;
}

bool DataFrameConfigHelper::IsInputSparse(InputType type)
{
    if (type == InputType::Features)
    {
        return m_featureElemType == StorageFormat::SparseCSC;
    } 
    
    if (type == InputType::Labels)
    {
        return m_labelElemType == StorageFormat::SparseCSC;
    }
 
    return false;
}

DataType DataFrameConfigHelper::ResolveTargetType(std::wstring& confval)
{
    if (Microsoft::MSR::CNTK::AreEqualIgnoreCase(confval, L"double"))
    {
        return DataType::Double;
    }
    else if (Microsoft::MSR::CNTK::AreEqualIgnoreCase(confval, L"float"))
    {
        return DataType::Float;
    }
    else
    {
        InvalidArgument("DataFrameDeserializer doesn't support target type %ls, please change your configuration.", confval.c_str());
    }
}

}}
