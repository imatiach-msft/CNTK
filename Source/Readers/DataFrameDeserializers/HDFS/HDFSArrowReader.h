//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//

#pragma once

#include "../Common/Constants.h"
#include "../Common/Interfaces.h"

namespace Microsoft { namespace MSR { namespace CNTK { namespace DF
{
    class HDFSArrowReader : public FileProvider
    {

    public:
        HDFSArrowReader(const ConfigParameters& config);
        ~HDFSArrowReader();

        FileList GetFileList() override;

        // Workflow:
        //     1. instantiate the HDFSArrowReader
        //     2. call Connect to instantiate the HdfsClient
        //     3. call GetPathInfo to retrieve the HdfsPathInfo for the path
        //     4. call IsDirectory to check if the path is a directory.
        //     5. if the path is a directory, get a list of the files in the directory
        //     TODO: Should we consider nested directories?
        //     6. if it is a single file, just call OpenReadable


        // How to handle directories - Get a vector of HdfsPathInfo,
        // create a vector of strings that contain the paths after checking that the type is
        // indeed a file, not a directory
        // and then loop through the vector to call OpenReadable for all the files
        // and read the data using the corresponding readers.
        
        // This function loops through the directory and adds the file names to the provided vector.
        void GetFilesInDirectory(std::vector<arrow::io::HdfsPathInfo>& list, std::vector<std::string>& filePaths);
        void ListDirectory(const std::string path, std::vector<arrow::io::HdfsPathInfo>& list);
        bool IsDirectory(arrow::io::HdfsPathInfo* info);
        void GetPathInfo(const std::string path, arrow::io::HdfsPathInfo* info);
        void Connect(); 
        void OpenReadable(const std::string filePath, std::shared_ptr<arrow::io::HdfsReadableFile>* filePtr);
        void OpenReadableParquetFiles(const std::vector<std::string>& listOfFiles, std::vector<std::shared_ptr<arrow::io::ReadableFileInterface>>& openedFiles);

    private:
        ConfigParameters m_config;
        arrow::io::HdfsConnectionConfig* m_HDFSconf;
        std::shared_ptr<arrow::io::HdfsClient> m_HDFSClient;
        arrow::Status m_status;

        std::string m_host;
        std::string m_filePath;
        int m_port;
        std::string m_user = "";
        std::string m_kerbTicket = "";
        arrow::io::HdfsDriver m_driver = arrow::io::HdfsDriver::LIBHDFS;
    };

} // hdfs
}}}