//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//
#include <iostream>

#include "HDFSArrowReader.h"


// This class is used by the DataFrameDeserialzers class to read in files
// using Arrow's HDFS file interfaces 

namespace Microsoft { namespace MSR { namespace CNTK { namespace hdfs
{
    HDFSArrowReader::HDFSArrowReader(
        const std::string host, 
        const std::string path, // Relative path to the file within HDFS
        const int port, 
        const std::string user,
        const std::string kerbTicket,
        arrow::io::HdfsDriver driver) : m_filepath(path)
    {
        HDFSconf = new arrow::io::HdfsConnectionConfig();
        HDFSconf -> host = host;
        HDFSconf -> port = port;
        HDFSconf -> user = user;
        HDFSconf -> kerb_ticket = kerbTicket;
        HDFSconf -> driver = driver;
        HDFSClient = nullptr;
    }

    void HDFSArrowReader::Connect() 
    {
        m_status = arrow::io::HdfsClient::Connect(HDFSconf, &HDFSclient);
        if (!m_status.ok()) 
        {
            std::cout << "Connection to HDFSClient failed." << std::endl;
            return;
        }
    }

    void HDFSArrowReader::OpenReadable()
    {
        m_status = HDFSClient -> OpenReadable(path, &filePtr);
        if (!m_status.ok())
        {
            std::cout << "Could not open the readable file." << std::endl;
            return;
        }
        // filePtr now points to an HdfsReadableFile that has been correctly instatiated.
    }
    


} // hdfs
}}}