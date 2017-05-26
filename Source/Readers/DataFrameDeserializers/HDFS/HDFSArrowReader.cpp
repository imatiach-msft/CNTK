//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//
#include <iostream>

#include "HDFSArrowReader.h"


// This class is used by the DataFrameDeserialzers class to read in files
// using Arrow's HDFS file interfaces 

namespace Microsoft { namespace MSR { namespace CNTK { namespace DF
{
    HDFSArrowReader::HDFSArrowReader(const ConfigParameters& config) : m_config(config)
    {
        DataFrameConfigHelper configHelper(config);
        configHelper.GetHDFSConfig(m_host, m_filePath, m_port);

        m_HDFSconf = new arrow::io::HdfsConnectionConfig();
        m_HDFSconf -> host = m_host;
        m_HDFSconf -> port = m_port;
        m_HDFSconf -> user = m_user;
        m_HDFSconf -> kerb_ticket = m_kerbTicket; 
        m_HDFSconf -> driver = m_driver;
        m_HDFSClient = nullptr;
        Connect();
    }

    void HDFSArrowReader::Connect() 
    {
        m_status = arrow::io::HdfsClient::Connect(m_HDFSconf, &m_HDFSClient);
        if (!m_status.ok()) 
        {
            std::cout << "Connection to HDFSClient failed." << std::endl;
            return;
        }
    }

    void HDFSArrowReader::GetPathInfo(const std::string path, arrow::io::HdfsPathInfo* info)
    {
        m_status = m_HDFSClient -> GetPathInfo(path, info);
        if (!m_status.ok())
        {
            std::cout << "Check your path and make sure that the HDFSClient has been"
            " connected through the Connect() method." << std::endl;
            return;
        }
    }

    // Returns a sorted list of file names in the directory that contain the file extension .parquet
    void HDFSArrowReader::GetFilesInDirectory(std::vector<arrow::io::HdfsPathInfo>& list, std::vector<std::string>& filePaths)
    {
        for (size_t i = 0; i < list.size(); i++)
        {
            if (list[i].kind == arrow::io::ObjectType::FILE && ((list[i].name).find(".parquet") != std::string::npos))
                filePaths.push_back(list[i].name);
        }
        std::sort(filePaths.begin(), filePaths.end()); // uses sort to create a deterministic order of the parquet files
    }

    void HDFSArrowReader::ListDirectory(const std::string path, std::vector<arrow::io::HdfsPathInfo>& list)
    {
        m_status = m_HDFSClient -> ListDirectory(path, &list);
        if (!m_status.ok())
        {
            std::cout << "Couldn't get the list of files in the directory. Check your path and"
            " make sure that the HDFSClient has been connected through the Connect() method." << std::endl;
            return;
        }
    }

    bool HDFSArrowReader::IsDirectory(arrow::io::HdfsPathInfo* info)
    {
        return info -> kind == arrow::io::ObjectType::DIRECTORY;
    }

    void HDFSArrowReader::OpenReadable(const std::string filePath, std::shared_ptr<arrow::io::HdfsReadableFile>* filePtr)
    {
        m_status = m_HDFSClient -> OpenReadable(filePath, filePtr);
        if (!m_status.ok())
        {
            std::cout << "Could not open the readable file." << std::endl;
            return;
        }
        // filePtr now points to an HdfsReadableFile that has been correctly instatiated.
    }

    // This helper function loops through the list of Files, opens them and stores them
    // in the provided vector
    void HDFSArrowReader::OpenReadableParquetFiles(const std::vector<std::string>& listOfFiles,
        std::vector<std::shared_ptr<arrow::io::ReadableFileInterface>>& openedFiles)
    {
        for (int i = 0; i < listOfFiles.size(); i++)
        {
            std::shared_ptr<arrow::io::HdfsReadableFile> filePtr;
            HDFSArrowReader::OpenReadable(listOfFiles[i], &filePtr);
            openedFiles.push_back(std::move(filePtr));
        }
    }

    FileList HDFSArrowReader::GetFileList()
    {
        // 1. GetPathInfo
        // 2. Check IsDirectory
        // 3. If path IsDirectory, get a sorted list of the files in the directory
        // 4. Loop through and return a vector of opened RFIs.
        // TODO: store the file paths in the config!!
        FileList fileList;
        std::vector<std::string> filePaths;
        arrow::io::HdfsPathInfo* pathInfo;
        GetPathInfo(m_filePath, pathInfo);
        if (!IsDirectory(pathInfo)) filePaths.push_back(m_filePath);
        else {
            std::vector<arrow::io::HdfsPathInfo> directoryList;
            ListDirectory(m_filePath, directoryList);
            GetFilesInDirectory(directoryList, filePaths);
        }

        OpenReadableParquetFiles(filePaths, fileList);
        return fileList;
    }
    
} // hdfs
}}}
