//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//

#pragma once
#include "HDFSFileObjects.h"

namespace Microsoft { namespace MSR { namespace CNTK {

class HDFSUtils {

    HDFSFileSystemPtr   Connect(const std::string& nameNode, int port);    
    HDFSFilePtr         OpenFile (HDFSFileSystemPtr fileSystem, const std::string& fileName, HDFSFileMode = HDFS_MODE_READ);
}

}}}