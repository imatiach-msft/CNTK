//
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE.md file in the project root for full license information.
//
#include "stdafx.h"
#include <regex>
#include "ConfigHelper.h"
#include "DataReader.h"
#include "StringUtil.h"

namespace Microsoft { namespace MSR { namespace CNTK {

using namespace std;

public:

wstring HDFSConfigHelper::GetNameNodeAddress() {
    if (!m_config.Exists(L"nameNodeAddr")) {
        InvalidArgument("The address of the NameNode not been set yet.");
    }
    return m_config(L"nameNodeAddr");
}

uint32_t HDFSConfigHelper::GetNameNodePort() {
    if (!m_config.Exists(L"nameNodePort")) {
        InvalidArgument("The port for the NameNode has not been set yet.");
    }
    return m_config(L"nameNodePort");
}

wstring HDFSConfigHelper::GetRelativeFilePath() {
    if (!m_config.Exists(L"relativeFilePath")) {
        InvalidArgument("The relative file path has not been set yet.");
    }
}

FileFormat HDFSConfigHelper::GetFileFormat() {
    return fileFormat;
}

void ConfigHelper::CheckFeatureType()
{
    wstring type = m_config(L"type", L"real");
    if (_wcsicmp(type.c_str(), L"real"))
    {
        InvalidArgument("Feature type must be of type 'real'.");
    }
}

void ConfigHelper::CheckLabelType()
{
    wstring type;
    if (m_config.Exists(L"labelType"))
    {
        // TODO: let's deprecate this eventually and just use "type"...
        type = static_cast<const wstring&>(m_config(L"labelType"));
    }
    else
    {
        // outputs should default to category
        type = static_cast<const wstring&>(m_config(L"type", L"category"));
    }

    if (_wcsicmp(type.c_str(), L"category"))
    {
        InvalidArgument("Label type must be of type 'category'.");
    }
}

ElementType ConfigHelper::GetElementType() const
{
    string precision = m_config.Find("precision", "float");
    if (AreEqualIgnoreCase(precision, "float"))
    {
        return ElementType::tfloat;
    }

    if (AreEqualIgnoreCase(precision, "double"))
    {
        return ElementType::tdouble;
    }

    RuntimeError("Not supported precision '%s'. Expected 'double' or 'float'.", precision.c_str());
}

size_t ConfigHelper::GetFeatureDimension()
{
    if (m_config.Exists(L"dim"))
    {
        return m_config(L"dim");
    }

    InvalidArgument("Features must specify dimension: 'dim' property is missing.");
}

size_t ConfigHelper::GetLabelDimension()
{
    if (m_config.Exists(L"labelDim"))
    {
        return m_config(L"labelDim");
    }

    if (m_config.Exists(L"dim"))
    {
        return m_config(L"dim");
    }

    InvalidArgument("Labels must specify dimension: 'dim/labelDim' property is missing.");
}

size_t ConfigHelper::GetRandomizationWindow()
{
    size_t result = randomizeAuto;

    if (m_config.Exists(L"randomize"))
    {
        wstring randomizeString = m_config.CanBeString(L"randomize") ? m_config(L"randomize") : wstring();
        if (!_wcsicmp(randomizeString.c_str(), L"none"))
        {
            result = randomizeNone;
        }
        else if (!_wcsicmp(randomizeString.c_str(), L"auto"))
        {
            result = randomizeAuto;
        }
        else
        {
            result = m_config(L"randomize");
        }
    }

    return result;
}

wstring ConfigHelper::GetRandomizer()
{
    // Check (on the action) if we're writing (inputs only) or training/evaluating (inputs and outputs)
    bool isActionWrite = wstring(m_config(L"action", L"")) == L"write";

    // Get the read method, defaults to "blockRandomize".
    wstring randomizer = m_config(L"readMethod", L"blockRandomize");

    if (isActionWrite && randomizer != L"none")
    {
        InvalidArgument("'readMethod' must be 'none' for write action.");
    }

    if (randomizer == L"blockRandomize" && GetRandomizationWindow() == randomizeNone)
    {
        InvalidArgument("'randomize' cannot be 'none' when 'readMethod' is 'blockRandomize'.");
    }

    return randomizer;
}

}}}
