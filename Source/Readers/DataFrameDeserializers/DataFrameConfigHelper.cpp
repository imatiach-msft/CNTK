//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//
#include "stdafx.h"

#include "DataFrameConfigHelper.h"
#include "StringUtil.h"
#include "ConfigUtil.h"

namespace Microsoft { namespace MSR { namespace CNTK { namespace DF {

using namespace std;


DataFrameConfigHelper::DataFrameConfigHelper(const ConfigParameters& config) : m_config(config)
{
    // NOTE: would getting the parameters and setting that up in the constructor
    //       be a good idea? there are specific parameters for file systems
    //       and may throw errors.

    // retrieve the parameter names, check if certain parameter names exist,
    // and then populate the correct ones.
    // Then, this would require a change in the constructor for the HDFSReader
    // to check for datasources. 
    // How would we check for the parameter names?
    std::vector<std::string> hdfsConfigNames = GetSectionsWithParameter("DataFrameDeserializer", config, "hdfs");
    if (hdfsConfigNames.size() == 1)
    {
        m_source = DataSource::HDFS;
        // Current ordering for storing HDFSConfigInfo is:
        //     string host, string path, int port, Optional: string user, string kerbTicket
        ConfigParameters hdfsConfigs = config(hdfsConfigNames[0]);
        m_host = hdfsConfigs("host");
        m_filePath = hdfsConfigs("filePath");
        m_port = std::stoi(hdfsConfigs("port"));
        if (hdfsConfigs("format") == "Parquet") m_format = FileFormat::Parquet;
        else RuntimeError("We currently only support Parquet Formats");
    }
    else RuntimeError("DataFrameDeserializers currently only support HDFS.");
}

void DataFrameConfigHelper::GetHDFSConfig(std::string& host, std::string& path, int& port)
{
    host = m_host;
    path = m_filePath;
    port = m_port;
}

}}}}
