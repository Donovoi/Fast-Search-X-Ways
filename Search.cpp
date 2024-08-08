#include <windows.h>
#include <xwfapi.h>
#include <cudf/io/text.hpp>
#include <cudf/strings/contains.hpp>
#include <rmm/mr/device/default_memory_resource.hpp>
#include <d3d12.h>
#include <directstorage.h>
#include <vector>
#include <string>
#include <iostream>
#include <wrl/client.h>
#include <stdexcept>

using Microsoft::WRL::ComPtr;

// DirectStorage and GPU resources
ComPtr<IDStorageFactory> storageFactory;
ComPtr<IDStorageQueue> storageQueue;
ComPtr<ID3D12Device> device;
ComPtr<ID3D12CommandQueue> commandQueue;
ComPtr<ID3D12CommandAllocator> commandAllocator;
ComPtr<ID3D12GraphicsCommandList> commandList;

void initialize_direct_storage() {
    // Initialize the DirectStorage API
    HRESULT hr = DStorageGetFactory(__uuidof(IDStorageFactory), &storageFactory);
    if (FAILED(hr)) {
        throw std::runtime_error("Failed to initialize DirectStorage factory.");
    }

    // Initialize D3D12 device and command queue
    hr = D3D12CreateDevice(nullptr, D3D_FEATURE_LEVEL_12_0, __uuidof(ID3D12Device), &device);
    if (FAILED(hr)) {
        throw std::runtime_error("Failed to create D3D12 device.");
    }

    D3D12_COMMAND_QUEUE_DESC queueDesc = {};
    queueDesc.Type = D3D12_COMMAND_LIST_TYPE_DIRECT;
    queueDesc.Priority = D3D12_COMMAND_QUEUE_PRIORITY_NORMAL;
    queueDesc.Flags = D3D12_COMMAND_QUEUE_FLAG_NONE;
    queueDesc.NodeMask = 0;

    hr = device->CreateCommandQueue(&queueDesc, __uuidof(ID3D12CommandQueue), &commandQueue);
    if (FAILED(hr)) {
        throw std::runtime_error("Failed to create D3D12 command queue.");
    }

    hr = device->CreateCommandAllocator(D3D12_COMMAND_LIST_TYPE_DIRECT, __uuidof(ID3D12CommandAllocator), &commandAllocator);
    if (FAILED(hr)) {
        throw std::runtime_error("Failed to create D3D12 command allocator.");
    }

    hr = device->CreateCommandList(0, D3D12_COMMAND_LIST_TYPE_DIRECT, commandAllocator.Get(), nullptr, __uuidof(ID3D12GraphicsCommandList), &commandList);
    if (FAILED(hr)) {
        throw std::runtime_error("Failed to create D3D12 command list.");
    }

    hr = storageFactory->CreateQueue(queueDesc, __uuidof(IDStorageQueue), &storageQueue);
    if (FAILED(hr)) {
        throw std::runtime_error("Failed to create DirectStorage queue.");
    }
}

std::vector<uint8_t> read_file_direct_storage(const std::string& file_path) {
    ComPtr<IDStorageFile> storageFile;
    HRESULT hr = storageFactory->OpenFile(file_path.c_str(), __uuidof(IDStorageFile), &storageFile);
    if (FAILED(hr)) {
        throw std::runtime_error("Failed to open file with DirectStorage: " + file_path);
    }

    uint64_t fileSize;
    hr = storageFile->GetFileInformation(nullptr, &fileSize, nullptr, nullptr);
    if (FAILED(hr)) {
        throw std::runtime_error("Failed to get file size for: " + file_path);
    }

    std::vector<uint8_t> fileContent(fileSize);
    D3D12_RESOURCE_DESC bufferDesc = {};
    bufferDesc.Dimension = D3D12_RESOURCE_DIMENSION_BUFFER;
    bufferDesc.Width = fileSize;
    bufferDesc.Height = 1;
    bufferDesc.DepthOrArraySize = 1;
    bufferDesc.MipLevels = 1;
    bufferDesc.Format = DXGI_FORMAT_UNKNOWN;
    bufferDesc.SampleDesc.Count = 1;
    bufferDesc.Layout = D3D12_TEXTURE_LAYOUT_ROW_MAJOR;
    bufferDesc.Flags = D3D12_RESOURCE_FLAG_NONE;

    D3D12_HEAP_PROPERTIES heapProperties = {};
    heapProperties.Type = D3D12_HEAP_TYPE_DEFAULT;

    ComPtr<ID3D12Resource> gpuBuffer;
    hr = device->CreateCommittedResource(&heapProperties, D3D12_HEAP_FLAG_NONE, &bufferDesc, D3D12_RESOURCE_STATE_COPY_DEST, nullptr, __uuidof(ID3D12Resource), &gpuBuffer);
    if (FAILED(hr)) {
        throw std::runtime_error("Failed to create GPU buffer for file: " + file_path);
    }

    storageQueue->EnqueueReadFile(storageFile.Get(), 0, fileSize, gpuBuffer.Get(), nullptr, 0, nullptr);
    storageQueue->Submit();

    // Wait for the operation to complete
    storageQueue->WaitForIdle();

    // Create a readback buffer for CPU access
    D3D12_HEAP_PROPERTIES readbackHeapProperties = {};
    readbackHeapProperties.Type = D3D12_HEAP_TYPE_READBACK;

    ComPtr<ID3D12Resource> readbackBuffer;
    hr = device->CreateCommittedResource(&readbackHeapProperties, D3D12_HEAP_FLAG_NONE, &bufferDesc, D3D12_RESOURCE_STATE_COPY_DEST, nullptr, __uuidof(ID3D12Resource), &readbackBuffer);
    if (FAILED(hr)) {
        throw std::runtime_error("Failed to create readback buffer for file: " + file_path);
    }

    D3D12_RESOURCE_BARRIER barrier = {};
    barrier.Type = D3D12_RESOURCE_BARRIER_TYPE_TRANSITION;
    barrier.Transition.pResource = gpuBuffer.Get();
    barrier.Transition.StateBefore = D3D12_RESOURCE_STATE_COPY_DEST;
    barrier.Transition.StateAfter = D3D12_RESOURCE_STATE_COPY_SOURCE;

    commandList->ResourceBarrier(1, &barrier);
    commandList->CopyResource(readbackBuffer.Get(), gpuBuffer.Get());

    // Execute command list
    ID3D12CommandList* commandLists[] = { commandList.Get() };
    commandQueue->ExecuteCommandLists(_countof(commandLists), commandLists);
    commandQueue->Signal(nullptr, 0);  // Add proper synchronization if needed

    // Map readback buffer to CPU
    void* mappedData;
    hr = readbackBuffer->Map(0, nullptr, &mappedData);
    if (FAILED(hr)) {
        throw std::runtime_error("Failed to map readback buffer for file: " + file_path);
    }

    memcpy(fileContent.data(), mappedData, fileSize);
    readbackBuffer->Unmap(0, nullptr);

    return fileContent;
}

void search_files_with_regex(const std::vector<std::string>& file_paths, const std::string& regex_pattern) {
    rmm::mr::device_memory_resource* mr = rmm::mr::get_current_device_resource();

    for (const auto& file_path : file_paths) {
        try {
            std::vector<uint8_t> fileContent = read_file_direct_storage(file_path);
            std::string content(fileContent.begin(), fileContent.end());

            auto content_column = cudf::strings::create_column_from_scalar(cudf::string_scalar(content), content.size(), mr);

            auto result = cudf::strings::contains(content_column->view(), regex_pattern);

            std::cout << "File: " << file_path << " contains pattern: " << regex_pattern << " -> " << result->view() << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "Error processing file " << file_path << ": " << e.what() << std::endl;
        }
    }
}

extern "C" __declspec(dllexport) int XWF_Run() {
    try {
        initialize_direct_storage();

        std::string regex_pattern = "your-regex-pattern";
        std::vector<std::string> file_paths = {
            "file1.txt",
            "file2.txt",
            "file3.txt"
        };

        search_files_with_regex(file_paths, regex_pattern);
    } catch (const std::exception& e) {
        std::cerr << "XWF_Run encountered an error: " << e.what() << std::endl;
        return 1;
    }

    return 0;  // Return 0 on success
}

extern "C" __declspec(dllexport) int XWF_Exit() {
    // Clean up resources
    storageQueue.Reset();
    storageFactory.Reset();
    commandList.Reset();
    commandAllocator.Reset();
    commandQueue.Reset();
    device.Reset();

    return 0;  // Return 0 on success
}

BOOL APIENTRY DllMain(HMODULE hModule, DWORD ul_reason_for_call, LPVOID lpReserved) {
    switch (ul_reason_for_call) {
    case DLL_PROCESS_ATTACH:
    case DLL_THREAD_ATTACH:
    case DLL_THREAD_DETACH:
    case DLL_PROCESS_DETACH:
        break;
    }
    return TRUE;
}
