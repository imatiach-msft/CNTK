#!/bin/bash
cd "/home/azureml/cntk"

# HDFSFO
/usr/local/mpi/bin/mpic++ \
    -c Source/Readers/DataFrameDeserializers/HDFS/HDFSFileObjects.cpp \
    -o /home/azureml/cntk/build/debug/.build/Source/Readers/DataFrameDeserializers/HDFS/HDFSFileObjects.o \
    -DHAS_MPI=1 -D_POSIX_SOURCE -D_XOPEN_SOURCE=600 -D__USE_XOPEN2K -std=c++11 -DCPUONLY -DUSE_MKL -D_DEBUG -DNO_SYNC  -DCNTK_COMPONENT_VERSION="2.0rc2d" \
    -msse4.1 -mssse3 -std=c++0x -fopenmp -fpermissive -fPIC -Werror -fcheck-new -Wno-error=literal-suffix -g \
    -ISource/Common/Include -ISource/CNTKv2LibraryDll -ISource/CNTKv2LibraryDll/API -ISource/CNTKv2LibraryDll/proto -ISource/../Examples/Extensibility/CPP -ISource/Math -ISource/CNTK -ISource/ActionsLib -ISource/ComputationNetworkLib -ISource/SGDLib -ISource/SequenceTrainingLib -ISource/CNTK/BrainScript -ISource/Readers/ReaderLib -ISource/PerformanceProfilerDll -I/usr/local/protobuf-3.1.0/include -I/usr/local/CNTKCustomMKL/3/include \
    -I/home/azureml/parquet-cpp/src \
    -I/home/azureml/hadoop-2.8.0/include \
    -I/home/azureml/parquet-cpp/arrow_ep/src/arrow_ep-install/include \
    -ITests/EndToEndTests/CNTKv2Library/Common -I/usr/local/boost-1.60.0/include -ISource/Readers/CNTKTextFormatReader -MD -MP -MF \
    /home/azureml/cntk/build/debug/.build/Source/Readers/DataFrameDeserializers/HDFS/HDFSFileObjects.d

# HDFSU
/usr/local/mpi/bin/mpic++ \
    -c Source/Readers/DataFrameDeserializers/DataFrameDeserializer.cpp \
    -o /home/azureml/cntk/build/debug/.build/Source/Readers/DataFrameDeserializers/DataFrameDeserializer.o \
    -DHAS_MPI=1 -D_POSIX_SOURCE -D_XOPEN_SOURCE=600 -D__USE_XOPEN2K -std=c++11 -DCPUONLY -DUSE_MKL -D_DEBUG -DNO_SYNC  -DCNTK_COMPONENT_VERSION="2.0rc2d" \
    -msse4.1 -mssse3 -std=c++0x -fopenmp -fpermissive -fPIC -Werror -fcheck-new -Wno-error=literal-suffix -g \
    -ISource/Common/Include -ISource/CNTKv2LibraryDll -ISource/CNTKv2LibraryDll/API -ISource/CNTKv2LibraryDll/proto -ISource/../Examples/Extensibility/CPP -ISource/Math -ISource/CNTK -ISource/ActionsLib -ISource/ComputationNetworkLib -ISource/SGDLib -ISource/SequenceTrainingLib -ISource/CNTK/BrainScript -ISource/Readers/ReaderLib -ISource/PerformanceProfilerDll -I/usr/local/protobuf-3.1.0/include -I/usr/local/CNTKCustomMKL/3/include \
    -I/home/azureml/parquet-cpp/arrow_ep/src/arrow_ep-install/include \
    -I/home/azureml/parquet-cpp/src \
    -I/home/azureml/hadoop-2.8.0/include \
    -ITests/EndToEndTests/CNTKv2Library/Common -I/usr/local/boost-1.60.0/include -ISource/Readers/CNTKTextFormatReader -MD -MP -MF \
    /home/azureml/cntk/build/debug/.build/Source/Readers/DataFrameDeserializers/DataFrameDeserializer.d

# Config
/usr/local/mpi/bin/mpic++ \
    -c Source/Readers/DataFrameDeserializers/DataFrameConfigHelper.cpp \
    -o /home/azureml/cntk/build/debug/.build/Source/Readers/DataFrameDeserializers/DataFrameConfigHelper.o \
    -DHAS_MPI=1 -D_POSIX_SOURCE -D_XOPEN_SOURCE=600 -D__USE_XOPEN2K -std=c++11 -DCPUONLY -DUSE_MKL -D_DEBUG -DNO_SYNC  -DCNTK_COMPONENT_VERSION="2.0rc2d" \
    -msse4.1 -mssse3 -std=c++0x -fopenmp -fpermissive -fPIC -Werror -fcheck-new -Wno-error=literal-suffix -g \
    -ISource/Common/Include -ISource/CNTKv2LibraryDll -ISource/CNTKv2LibraryDll/API -ISource/CNTKv2LibraryDll/proto -ISource/../Examples/Extensibility/CPP -ISource/Math -ISource/CNTK -ISource/ActionsLib -ISource/ComputationNetworkLib -ISource/SGDLib -ISource/SequenceTrainingLib -ISource/CNTK/BrainScript -ISource/Readers/ReaderLib -ISource/PerformanceProfilerDll -I/usr/local/protobuf-3.1.0/include -I/usr/local/CNTKCustomMKL/3/include \
    -I/home/azureml/parquet-cpp/arrow_ep/src/arrow_ep-install/include \
    -I/home/azureml/parquet-cpp/src \
    -I/home/azureml/hadoop-2.8.0/include \
    -ITests/EndToEndTests/CNTKv2Library/Common -I/usr/local/boost-1.60.0/include -ISource/Readers/CNTKTextFormatReader -MD -MP -MF \
    /home/azureml/cntk/build/debug/.build/Source/Readers/DataFrameDeserializers/DataFrameConfigHelper.d