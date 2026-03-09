using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Networking.Transport;
using Unity.Networking.Transport.Error;

namespace ZG
{
    public struct NetworkSendBuffer
    {
        private int __index;
        private UnsafeList<int> __sizes;
        private UnsafeList<byte> __bytes;

        public NetworkSendBuffer(in AllocatorManager.AllocatorHandle allocator)
        {
            __index = 0;

            __sizes = new UnsafeList<int>(1, allocator);
            __bytes = new UnsafeList<byte>(1, allocator);
        }

        public void Dispose()
        {
            __sizes.Dispose();
            __bytes.Dispose();
        }

        public void Clear()
        {
            __index = 0;
            __sizes.Clear();
            __bytes.Clear();
        }

        public void Append(in NetworkSendBuffer buffer)
        {
            int length = buffer.__sizes.Length;
            if (length > buffer.__index)
            {
                int sizeLength = __sizes.Length;
                __sizes.Resize(sizeLength + length - buffer.__index, NativeArrayOptions.UninitializedMemory);
                
                int index = buffer.__sizes[buffer.__index],
                    offset = __bytes.Length - index, 
                    sizeOffset = sizeLength - buffer.__index;
                for (int i = buffer.__index; i < length; ++i)
                    __sizes[sizeOffset + i] = buffer.__sizes[i] + offset;

                int size = buffer.__bytes.Length - index;
                unsafe
                {
                    __bytes.AddRange(buffer.__bytes.Ptr + index, size);
                }
            }
        }

        public bool Apply(
            in NetworkConnection connection,
            in NetworkPipeline pipeline,
            ref NetworkDriver.Concurrent driver)
        {
            int length = __sizes.Length, count, byteOffset, result;
            StatusCode statusCode;
            DataStreamWriter writer = default;
            while (__index < length)
            {
                if (!writer.IsCreated)
                {
                    statusCode = (StatusCode)driver.BeginSend(pipeline, connection, out writer);
                    if (StatusCode.Success != statusCode)
                    {
                        LogError(statusCode);

                        return false;
                    }
                }

                byteOffset = __index > 0 ? __sizes[__index - 1] : 0;
                count = __BinarySearch(
                    __sizes, 
                    writer.Capacity - writer.Length + byteOffset, 
                    __index);
                if (count < __index)
                {
                    result = driver.EndSend(writer);
                    if (result < 0)
                    {
                        statusCode = (StatusCode)result;

                        LogError(statusCode);
                    }

                    writer = default;

                    continue;
                }

                writer.WriteBytes(__AsArray(byteOffset, count));

                __index = count + 1;
            }
            
            if(writer.IsCreated)
                driver.EndSend(writer);

            Clear();

            return true;
        }

        public bool ReadNext(out NativeArray<byte> bytes)
        {
            if (__index >= __sizes.Length)
            {
                bytes = default;
                
                return false;
            }

            int byteOffset = __index > 0 ? __sizes[__index - 1] : 0;

            bytes = __AsArray(byteOffset + UnsafeUtility.SizeOf<ushort>(), __index);

            return true;
        }

        public bool BeginWrite(out DataStreamWriter writer, short capacity = 1024)
        {
            if (capacity < 1)
            {
                writer = default;

                return false;
            }

            int index = __sizes.Length, offset = index > 0 ? __sizes[index - 1] : 0;
            if (offset < __bytes.Length)
            {
                writer = default;

                return false;
            }

            offset += UnsafeUtility.SizeOf<ushort>();

            __bytes.Resize(offset + capacity, NativeArrayOptions.UninitializedMemory);

            unsafe
            {
                writer = new DataStreamWriter(__bytes.Ptr + offset, capacity);
            }

            return true;
        }

        public void EndWrite(in DataStreamWriter writer)
        {
            int size = writer.Length;
            if (size < 1)
                return;

            int index = __sizes.Length, offset = index > 0 ? __sizes[index - 1] : 0;

            unsafe
            {
                *(ushort*)(__bytes.Ptr + offset) = (ushort)size;
            }

            size += offset + UnsafeUtility.SizeOf<ushort>();

            __sizes.Add(size);

            __bytes.Resize(size, NativeArrayOptions.UninitializedMemory);
        }

        private NativeArray<byte> __AsArray(int byteOffset, int count)
        {
            NativeArray<byte> bytes;
            unsafe
            {
                bytes = CollectionHelper.ConvertExistingDataToNativeArray<byte>(
                    __bytes.Ptr + byteOffset,
                    __sizes[count] - byteOffset,
                    Allocator.None,
                    true);
            }

            return bytes;
        }

        private static int __BinarySearch(in UnsafeList<int> list, int value, int offset)
        {
            int index = offset - 1, count = list.Length - offset, middle;
            while (count > 0)
            {
                middle = (count + 1) >> 1;
                if (list[index + middle] > value)
                {
                    if (middle < 2)
                        break;

                    count = middle;
                }
                else
                {
                    index += middle;

                    count -= middle;
                }
            }

            return index;
        }

        public static void LogError(StatusCode statusCode)
        {
            UnityEngine.Debug.LogError($"NetworkSendMessage: {(int)statusCode}");
        }
    }
}