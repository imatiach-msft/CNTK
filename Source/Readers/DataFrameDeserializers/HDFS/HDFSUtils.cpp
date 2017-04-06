//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//
#include "stdafx.h"
#include "HDFSUtils.h"

namespace Microsoft { namespace MSR { namespace CNTK { 
    
class HDFSUtils {

    HDFSFileSystemPtr Connect(const std::string& nameNode, int port)
    {
        return std::make_shared<HDFSFileSystem>(nameNode, port);
    }

    HDFSFilePtr OpenFile (HDFSFileSystemPtr fs, const std::string& fileName, HDFSFileMode mode)
    {
        return std::make_shared<HDFSFile> (fs, fileName, mode);
    }

}

}}}