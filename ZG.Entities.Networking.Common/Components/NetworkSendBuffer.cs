using System;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Networking.Transport;
using Unity.Networking.Transport.Error;

namespace ZG
{
    public interface INetworkDriver
    {
        int BeginSend(NetworkPipeline pipe, NetworkConnection connection, out DataStreamWriter writer,
            int requiredPayloadSize = 0);

        int EndSend(DataStreamWriter writer);
    }
    
    public struct NetworkSendBuffer
    {
        private struct DriverWrapper : INetworkDriver
        {
            private NetworkDriver.Concurrent __instance;

            public DriverWrapper(ref NetworkDriver.Concurrent instance)
            {
                __instance = instance;
            }
            
            public int BeginSend(NetworkPipeline pipe, NetworkConnection connection, out DataStreamWriter writer,
                int requiredPayloadSize = 0) => __instance.BeginSend(pipe, connection, out writer, requiredPayloadSize);
            
            public int EndSend(DataStreamWriter writer) => __instance.EndSend(writer);
        }
        
        private struct MultiDriverWrapper : INetworkDriver
        {
            private MultiNetworkDriver.Concurrent __instance;
            
            public MultiDriverWrapper(ref MultiNetworkDriver.Concurrent instance)
            {
                __instance = instance;
            }

            public int BeginSend(NetworkPipeline pipe, NetworkConnection connection, out DataStreamWriter writer,
                int requiredPayloadSize = 0) => __instance.BeginSend(pipe, connection, out writer, requiredPayloadSize);
            
            public int EndSend(DataStreamWriter writer) => __instance.EndSend(writer);
        }
        
        private UnsafeList<int> __sizes;
        private UnsafeList<byte> __bytes;

        public bool isCreated => __sizes.IsCreated && __bytes.IsCreated;

        public int messageCount => __sizes.IsCreated ? __sizes.Length : 0;

        public int byteCount => __bytes.IsCreated ? __bytes.Length : 0;

        public int byteCapacity => __bytes.IsCreated ? __bytes.Capacity : 0;
        
        public NetworkSendBuffer(in AllocatorManager.AllocatorHandle allocator)
        {
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
            __sizes.Clear();
            __bytes.Clear();
        }

        /// <summary>
        /// Releases retry-queue peak capacity after a previously congested destination drains.
        /// Normal current-Tick sends do not use this persistent buffer.
        /// </summary>
        public void Reset()
        {
            var allocator = __sizes.Allocator;
            Dispose();
            this = new NetworkSendBuffer(allocator);
        }

        /// <summary>
        /// Drops the already-sent prefix tracked by <paramref name="index"/> while retaining the
        /// unsent messages and their backing capacity. This keeps a queue-full retry buffer bounded
        /// by pending data rather than by all data ever appended to it.
        /// </summary>
        public unsafe void Compact(ref int index)
        {
            if (index <= 0)
            {
                index = 0;
                return;
            }

            int messageCount = __sizes.Length;
            if (index >= messageCount)
            {
                Clear();
                index = 0;
                return;
            }

            int byteOffset = __sizes[index - 1];
            int remainingMessageCount = messageCount - index;
            int remainingByteCount = __bytes.Length - byteOffset;

            UnsafeUtility.MemMove(__bytes.Ptr, __bytes.Ptr + byteOffset, remainingByteCount);
            for (int i = 0; i < remainingMessageCount; ++i)
                __sizes[i] = __sizes[index + i] - byteOffset;

            __sizes.Resize(remainingMessageCount, NativeArrayOptions.UninitializedMemory);
            __bytes.Resize(remainingByteCount, NativeArrayOptions.UninitializedMemory);
            index = 0;
        }

        /// <summary>
        /// Appends one framed application message when doing so stays inside the caller-provided
        /// pending queue limits. The payload is copied once into the destination retry queue.
        /// </summary>
        public unsafe bool TryAppendMessage(
            in NativeArray<byte> payload,
            int maxMessageCount,
            int maxByteCount)
        {
            int payloadLength = payload.Length;
            if (!payload.IsCreated || payloadLength < 1 || payloadLength > ushort.MaxValue ||
                maxMessageCount < 1 || maxByteCount < UnsafeUtility.SizeOf<ushort>())
                return false;

            int destinationMessageCount = __sizes.Length;
            int destinationByteCount = __bytes.Length;
            int framedByteCount = UnsafeUtility.SizeOf<ushort>() + payloadLength;
            if (destinationMessageCount >= maxMessageCount ||
                destinationByteCount > maxByteCount - framedByteCount)
                return false;

            int endByteCount = destinationByteCount + framedByteCount;
            __bytes.Resize(endByteCount, NativeArrayOptions.UninitializedMemory);
            *(ushort*)(__bytes.Ptr + destinationByteCount) = (ushort)payloadLength;
            UnsafeUtility.MemCpy(
                __bytes.Ptr + destinationByteCount + UnsafeUtility.SizeOf<ushort>(),
                payload.GetUnsafeReadOnlyPtr(),
                payloadLength);
            __sizes.Add(endByteCount);
            return true;
        }

        public void Append(in NetworkSendBuffer buffer, int index)
        {
            if (index < 0 || !__sizes.IsCreated || !__bytes.IsCreated ||
                !buffer.__sizes.IsCreated || !buffer.__bytes.IsCreated)
                return;

            int sourceSizeLength = buffer.__sizes.Length;
            if (index >= sourceSizeLength)
                return;

            int destinationSizeLength = __sizes.Length;
            int destinationByteLength = __bytes.Length;
            int previousSize = 0;
            for (int i = 0; i < destinationSizeLength; ++i)
            {
                int size = __sizes[i];
                if (size <= previousSize || size > destinationByteLength)
                    return;

                previousSize = size;
            }

            if (previousSize != destinationByteLength)
                return;

            int sourceByteLength = buffer.__bytes.Length;
            int sourceOffset = index > 0 ? buffer.__sizes[index - 1] : 0;
            previousSize = sourceOffset;
            if (sourceOffset < 0 || sourceOffset > sourceByteLength)
                return;

            for (int i = index; i < sourceSizeLength; ++i)
            {
                int size = buffer.__sizes[i];
                if (size <= previousSize || size > sourceByteLength)
                    return;

                previousSize = size;
            }

            if (previousSize != sourceByteLength)
                return;

            int sourceSizeCount = sourceSizeLength - index;
            int sourceByteCount = sourceByteLength - sourceOffset;
            if (sourceSizeCount > int.MaxValue - destinationSizeLength ||
                sourceByteCount > int.MaxValue - destinationByteLength)
                return;

            __sizes.Resize(destinationSizeLength + sourceSizeCount, NativeArrayOptions.UninitializedMemory);

            for (int i = index; i < sourceSizeLength; ++i)
                __sizes[destinationSizeLength + i - index] =
                    destinationByteLength + (buffer.__sizes[i] - sourceOffset);

            unsafe
            {
                __bytes.AddRange(buffer.__bytes.Ptr + sourceOffset, sourceByteCount);
            }
        }

        public bool Apply<T>(
            in NetworkConnection connection,
            in NetworkPipeline pipeline,
            ref T driver, 
            ref int index) where T : struct, INetworkDriver
        {
            int length = __sizes.Length, count, byteOffset, result, previousIndex = index;
            StatusCode statusCode;
            DataStreamWriter writer = default;
            while (index < length)
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

                byteOffset = index > 0 ? __sizes[index - 1] : 0;
                count = __BinarySearch(
                    __sizes, 
                    writer.Capacity - writer.Length + byteOffset, 
                    index);
                if (count < index || !writer.WriteBytes(__AsArray(byteOffset, count)))
                {
                    result = driver.EndSend(writer);
                    if (result < 0)
                    {
                        statusCode = (StatusCode)result;

                        if(StatusCode.NetworkSendQueueFull != statusCode)
                            LogError(statusCode);

                        index = previousIndex;
                        
                        return false;
                    }

                    previousIndex = index;

                    writer = default;

                    continue;
                }

                index = count + 1;
            }

            if (writer.IsCreated)
            {
                result = driver.EndSend(writer);
                
                if (result < 0)
                {
                    statusCode = (StatusCode)result;

                    if(StatusCode.NetworkSendQueueFull != statusCode)
                        LogError(statusCode);
                        
                    index = previousIndex;

                    return false;
                }
            }

            Clear();
            index = 0;

            return true;
        }

        public bool Apply(
            in NetworkConnection connection,
            in NetworkPipeline pipeline,
            ref MultiNetworkDriver.Concurrent driver,
            ref int index)
        {
            var wrapper = new MultiDriverWrapper(ref driver);
            return Apply(
                connection,
                pipeline,
                ref wrapper,
                ref index);
        }

        public bool Apply(
            in NetworkConnection connection,
            in NetworkPipeline pipeline,
            ref NetworkDriver.Concurrent driver,
            ref int index)
        {
            var wrapper = new DriverWrapper(ref driver);
            return Apply(
                connection,
                pipeline,
                ref wrapper,
                ref index);
        }
        
        public bool ReadNext(ref int index, out NativeArray<byte> bytes)
        {
            if (index >= __sizes.Length)
            {
                bytes = default;
                
                return false;
            }

            int byteOffset = index > 0 ? __sizes[index - 1] : 0;

            bytes = __AsArray(byteOffset + UnsafeUtility.SizeOf<ushort>(), index);

            ++index;

            return true;
        }

        public bool BeginWrite(out DataStreamWriter writer, ushort capacity = 1024)
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

        // DIAG (temporary): pending (un-flushed) message count from a given flush index.
        public int GetPending(int index) => __sizes.Length - index;
    }
}
