using System;
using Unity.Jobs;
using Unity.Burst;
using Unity.Entities;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Mathematics;
using Unity.Networking.Transport;
using Unity.Networking.Transport.Error;
using ZG;
using static PlasticPipe.Client.InvokeMethodRetry;

public struct NetworkServerSendBuffer
{
    private struct ConnectionIndex
    {
        public int value;
        public int channelIndex;

        public int payloadOffset;
        public int payloadSize;
    }

    private struct SendBuffer
    {
        public int index;
        public NetworkSendBuffer value;
    }

    private struct Channel
    {
        private UnsafeList<int> __values;

        public Channel(in AllocatorManager.AllocatorHandle allocator)
        {
            __values = new UnsafeList<int>(1, allocator);
        }

        public void Dispose()
        {
            __values.Dispose();
        }

        public void Clear()
        {
            __values.Clear();
        }

        public bool Or(in Channel channel)
        {
            bool result = __values.IsEmpty || channel.__values.IsEmpty;
            if (!result)
            {
                foreach (var value in __values)
                {
                    if (channel.__values.Contains(value))
                    {
                        result = true;

                        break;
                    }
                }
            }

            return result;
        }

        public bool And(in Channel channel)
        {
            bool result = false;
            if (!__values.IsEmpty && !channel.__values.IsEmpty)
            {
                result = true;
                foreach (var value in __values)
                {
                    if (!channel.__values.Contains(value))
                    {
                        result = false;

                        break;
                    }
                }
            }

            return result;
        }

        public bool Add(int value)
        {
            if (__values.Contains(value))
                return false;

            __values.Add(value);

            return true;
        }

        public bool Remove(int value)
        {
            int index = __values.IndexOf(value);
            if (index == -1)
                return false;

            __values.RemoveAtSwapBack(index);

            return true;
        }

        public UnsafeList<int>.Enumerator GetEnumerator() => __values.GetEnumerator();
    }

    public struct Concurrent
    {
        public readonly int ChannelCount;

        [ReadOnly]
        private NativeHashMap<uint, ConnectionIndex> __connectionIndices;
        [ReadOnly]
        private NativeArray<byte> __payloads;

        [NativeDisableParallelForRestriction]
        private NativeArray<Channel> __channels;
        [NativeDisableParallelForRestriction]
        private NativeArray<UnsafeList<int>> __targets;
        [NativeDisableParallelForRestriction]
        private NativeArray<NetworkSendBuffer> __buffers;
        private NativeList<int>.ParallelWriter __sendAllConnectionIndices;
        private NativeParallelMultiHashMap<int, int>.ParallelWriter __sendChannelConnectionIndices;
        private NativeParallelMultiHashMap<int, int>.ParallelWriter __sendIdentityConnectionIndices;

        public Concurrent(ref NetworkServerSendBuffer sendBuffer)
        {
            ChannelCount = sendBuffer.ChannelCount;
            __connectionIndices = sendBuffer.__connectionIndices;
            __payloads = sendBuffer.__payloads.AsDeferredJobArray();
            __channels = sendBuffer.__channels.AsDeferredJobArray();
            __targets = sendBuffer.__targets.AsDeferredJobArray();
            __buffers = sendBuffer.__buffers.AsDeferredJobArray();
            __sendAllConnectionIndices = sendBuffer.__sendAllConnectionIndices.AsParallelWriter();
            __sendChannelConnectionIndices = sendBuffer.__sendChannelConnectionIndices.AsParallelWriter();
            __sendIdentityConnectionIndices = sendBuffer.__sendIdentityConnectionIndices.AsParallelWriter();
        }

        public NativeArray<byte> GetPayload(uint id)
        {
            var connectionIndex = __connectionIndices[id];

            return __payloads.GetSubArray(connectionIndex.payloadOffset, connectionIndex.payloadSize);
        }

        public int GetConnectionIndex(uint id) => __connectionIndices[id].value;

        public int GetChannelIndex(uint id) => __connectionIndices[id].channelIndex;

        public bool AddChannel(uint id, int value)
        {
            int channelIndex = GetChannelIndex(id);
            var channel = __channels[channelIndex];

            if (channel.Add(value))
            {
                __channels[channelIndex] = channel;

                return true;
            }

            return false;
        }

        public bool RemoveChannel(uint id, int value)
        {
            int channelIndex = GetChannelIndex(id);
            var channel = __channels[channelIndex];

            if (channel.Remove(value))
            {
                __channels[channelIndex] = channel;

                return true;
            }

            return false;
        }

        public bool BeginWrite(uint id, out DataStreamWriter writer, ushort capacity = 1024)
        {
            return __BeginWrite(capacity, 0, id, out writer);
        }

        public bool BeginWrite(uint id, int channel, out DataStreamWriter writer, ushort capacity = 1024)
        {
            return __BeginWrite(capacity, GetTargetFromChannel(channel), id, out writer);
        }

        public bool BeginWrite(uint id, uint targetID, out DataStreamWriter writer, ushort capacity = 1024)
        {
            if (__connectionIndices.TryGetValue(targetID, out var connectionIndex))
            {
                writer = default;
                return false;
            }

            return __BeginWrite(capacity, GetTargetFromConnectionIndex(connectionIndex.value), id, out writer);
        }

        public void EndWrite(in DataStreamWriter writer)
        {
            int value = (int)writer.m_SendHandleData;

            var buffer = __buffers[value];
            buffer.EndWrite(writer);
            __buffers[value] = buffer;

            int numConnections = __targets.Length, target = GetBufferTarget(value, __GetChannelCount(), numConnections, out int connectionIndex);
            var targets = __targets[connectionIndex];
            if(targets.IndexOf(target) == -1)
            {
                targets.Add(target);

                __targets[connectionIndex] = targets;

                if (target > 0)
                    __sendIdentityConnectionIndices.Add(target - 1, connectionIndex);
                else if (target < 1)
                    __sendChannelConnectionIndices.Add(-target - 1, connectionIndex);
                else
                    __sendAllConnectionIndices.AddNoResize(connectionIndex);
            }
        }

        private bool __BeginWrite(ushort capacity, int target, uint id, out DataStreamWriter writer)
        {
            if (__connectionIndices.TryGetValue(id, out var connectionIndex))
            {
                writer = default;
                return false;
            }

            int bufferIndex = GetBufferIndex(target, __GetChannelCount(), connectionIndex.value, __connectionIndices.Count);

            var buffer = __buffers[bufferIndex];

            if (!buffer.BeginWrite(out writer, capacity))
                return false;

            writer.m_SendHandleData = (IntPtr)bufferIndex;

            return true;
        }

        private int __GetChannelCount() => ChannelCount == 0 ? __channels.Length : 0;
    }

    public struct Identity
    {
        public uint ID;

        private Concurrent __sendBuffer;

        public int cnnectionIndex => __sendBuffer.GetConnectionIndex(ID);

        public int channelIndex => __sendBuffer.GetChannelIndex(ID);

        internal Identity(uint id,
            ref Concurrent sendBuffer)
        {
            ID = id;
            __sendBuffer = sendBuffer;
        }

        public NativeArray<byte> GetPayload(uint id)
        {
            return __sendBuffer.GetPayload(id);
        }

        public bool AddChannel(int value)
        {
            return __sendBuffer.AddChannel(ID, value);
        }

        public bool RemoveChannel(int value)
        {
            return __sendBuffer.RemoveChannel(ID, value);
        }

        public bool BeginWrite(
            out DataStreamWriter writer, ushort capacity = 1024)
        {
            return __sendBuffer.BeginWrite(ID, out writer, capacity);
        }

        public bool BeginWrite(
            int channel,
            out DataStreamWriter writer, ushort capacity = 1024)
        {
            return __sendBuffer.BeginWrite(ID, channel, out writer, capacity);
        }

        public bool BeginWrite(
            uint id,
            out DataStreamWriter writer, ushort capacity = 1024)
        {
            return __sendBuffer.BeginWrite(ID, id, out writer, capacity);
        }

        public void EndWrite(in DataStreamWriter writer)
        {
            __sendBuffer.EndWrite(writer);
        }
    }

    public struct Sender
    {
        public readonly int ChannelCount;

        [ReadOnly]
        private NativeHashMap<NetworkConnection, uint> __connectionIDs;
        [ReadOnly]
        private NativeHashMap<uint, ConnectionIndex> __connectionIndices;
        [ReadOnly]
        private NativeArray<Channel> __channels;
        [ReadOnly]
        private NativeArray<NetworkSendBuffer> __buffers;
        [ReadOnly]
        private NativeList<int> __sendAllConnectionIndices;
        [ReadOnly]
        private NativeParallelMultiHashMap<int, int> __sendChannelConnectionIndices;
        [ReadOnly]
        private NativeParallelMultiHashMap<int, int> __sendIdentityConnectionIndices;

        private NativeArray<SendBuffer> __sendBuffers;

        public Sender(ref NetworkServerSendBuffer sendBuffer)
        {
            ChannelCount = sendBuffer.ChannelCount;
            __connectionIDs = sendBuffer.__connectionIDs;
            __connectionIndices = sendBuffer.__connectionIndices;
            __channels = sendBuffer.__channels.AsDeferredJobArray();
            __buffers = sendBuffer.__buffers.AsDeferredJobArray();
            __sendAllConnectionIndices = sendBuffer.__sendAllConnectionIndices;
            __sendChannelConnectionIndices = sendBuffer.__sendChannelConnectionIndices;
            __sendIdentityConnectionIndices = sendBuffer.__sendIdentityConnectionIndices;
            __sendBuffers = sendBuffer.__sendBuffers.AsDeferredJobArray();
        }

        public void Send(in NetworkConnection connection,
            in NetworkPipeline pipeline, 
            ref NetworkDriver.Concurrent driver)
        {
            uint id = __connectionIDs[connection];
            var connectionIndex = __connectionIndices[id];
            var sendBuffer = __sendBuffers[connectionIndex.value];
            if (sendBuffer.value.Apply(connection, pipeline, ref driver, ref sendBuffer.index))
            {
                sendBuffer.value.Clear();

                sendBuffer.index = 0;
            }

            int channelCount = ChannelCount == 0 ? __channels.Length : 0, connectionCount = __connectionIDs.Count, index;
            NetworkSendBuffer buffer;
            foreach (int connectionIndexToSend in __sendIdentityConnectionIndices.GetValuesForKey(connectionIndex.value))
            {
                buffer = __buffers[GetBufferIndex(GetTargetFromConnectionIndex(connectionIndex.value), channelCount, connectionIndexToSend, connectionCount)];

                index = 0;
                if (!buffer.Apply(connection, pipeline, ref driver, ref index))
                    sendBuffer.value.Append(buffer, index);
            }

            foreach (int channel in __channels[connectionIndex.channelIndex])
            {
                foreach(int connectionIndexToSend in __sendChannelConnectionIndices.GetValuesForKey(channel))
                {
                    if (connectionIndexToSend == connectionIndex.value)
                        continue;

                    buffer = __buffers[GetBufferIndex(GetTargetFromChannel(channel), channelCount, connectionIndexToSend, connectionCount)];

                    index = 0;
                    if (!buffer.Apply(connection, pipeline, ref driver, ref index))
                        sendBuffer.value.Append(buffer, index);
                }
            }

            foreach (int connectionIndexToSend in __sendAllConnectionIndices)
            {
                if (connectionIndexToSend == connectionIndex.value)
                    continue;

                buffer = __buffers[GetBufferIndex(0, channelCount, connectionIndexToSend, connectionCount)];

                index = 0;
                if (!buffer.Apply(connection, pipeline, ref driver, ref index))
                    sendBuffer.value.Append(buffer, index);
            }
        }
    }

    private NativeHashMap<NetworkConnection, uint> __connectionIDs;
    private NativeHashMap<uint, ConnectionIndex> __connectionIndices;
    private NativeList<NetworkConnection> __connections;
    private NativeList<byte> __payloads;
    private NativeList<Channel> __channels;
    private NativeList<SendBuffer> __sendBuffers;
    private NativeList<UnsafeList<int>> __targets;
    private NativeList<NetworkSendBuffer> __buffers;
    private NativeList<int> __sendAllConnectionIndices;
    private NativeParallelMultiHashMap<int, int> __sendChannelConnectionIndices;
    private NativeParallelMultiHashMap<int, int> __sendIdentityConnectionIndices;

    public readonly int ChannelCount;

    public int channelCount => ChannelCount == 0 ? 0 : __channels.Length;

    public unsafe AllocatorManager.AllocatorHandle allocator => __connections.GetUnsafeList()->Allocator;

    public NativeList<NetworkConnection> connections => __connections;

    public NativeHashMap<NetworkConnection, uint> connectionIDs => __connectionIDs;

    public NetworkServerSendBuffer(in AllocatorManager.AllocatorHandle allocator, int channelCount = 0)
    {
        ChannelCount = 0;

        __connectionIDs = new NativeHashMap<NetworkConnection, uint>(1, allocator);
        __connectionIndices = new NativeHashMap<uint, ConnectionIndex> (1, allocator);
        __connections = new NativeList<NetworkConnection> (allocator);
        __payloads = new NativeList<byte> (allocator);
        __channels = new NativeList<Channel> (allocator);
        __sendBuffers = new NativeList<SendBuffer> (allocator);
        __targets = new NativeList<UnsafeList<int>> (allocator);
        __buffers = new NativeList<NetworkSendBuffer>(allocator);
        __sendAllConnectionIndices = new NativeList<int>(allocator);
        __sendChannelConnectionIndices = new NativeParallelMultiHashMap<int, int> (1, allocator);
        __sendIdentityConnectionIndices = new NativeParallelMultiHashMap<int, int>(1, allocator);
    }

    public void Dispose()
    {
        __connectionIDs.Dispose();
        __connectionIndices.Dispose();
        __connections.Dispose();
        __payloads.Dispose();

        foreach(var channel in __channels)
            channel.Dispose();

        __channels.Dispose();

        foreach (var sendBuffer in __sendBuffers)
            sendBuffer.value.Dispose();

        __sendBuffers.Dispose();

        foreach(var targets in __targets)
            targets.Dispose();

        __targets.Dispose();

        foreach(var buffer in __buffers)
            buffer.Dispose();

        __buffers.Dispose();

        __sendAllConnectionIndices.Dispose();
        __sendChannelConnectionIndices.Dispose();
        __sendIdentityConnectionIndices.Dispose();
    }

    public void Clear()
    {
        int connectionCount = __connections.Length;
        for(int i = 0; i < connectionCount; ++i)
            __targets.ElementAt(i).Clear();

        int bufferCount = GetBufferCountPerConnection(channelCount, connectionCount) * connectionCount;
        for (int i = 0; i < bufferCount; ++i)
            __buffers.ElementAt(i).Clear();

        __sendAllConnectionIndices.Clear();
        __sendChannelConnectionIndices.Clear();
        __sendIdentityConnectionIndices.Clear();
    }

    public Concurrent AsConcurrent() => new Concurrent(ref this);

    public Sender AsSender() => new Sender(ref this);

    public void GetConnection(uint id, out int connectionIndex, out int channelIndex, out NativeArray<byte> payload)
    {
        var temp = __connectionIndices[id];
        connectionIndex = temp.value;
        channelIndex = temp.channelIndex;
        payload = __payloads.AsArray().GetSubArray(temp.payloadOffset, temp.payloadSize);
    }

    public uint Connect(ref NetworkConnection connection, in NativeArray<byte> payload)
    {
        uint id = new DataStreamReader(payload).ReadPackedUInt(StreamCompressionModel.Default);
        var allocator = this.allocator;
        if (__connectionIndices.TryGetValue(id, out var connectionIndex))
        {
            if(connectionIndex.value != -1)
            {
                UnityEngine.Debug.LogError($"connection id mismatch: {id}");

                connection = __connections[connectionIndex.value];

                return 0;
            }

            if (connectionIndex.payloadSize != payload.Length)
            {
                UnityEngine.Debug.LogError($"payload size mismatch: {connectionIndex.payloadSize} != {payload.Length}");

                return 0;
            }

            NativeArray<byte>.Copy(payload,
                0,
                __payloads.AsArray(),
                connectionIndex.payloadOffset,
                connectionIndex.payloadSize);
        }
        else
        {
            connectionIndex.channelIndex = __channels.Length;

            __channels.Add(new Channel(allocator));

            connectionIndex.payloadOffset = __payloads.Length;
            connectionIndex.payloadSize = payload.Length;

            __payloads.AddRange(payload);
        }

        connectionIndex.value = __connections.Length;

        __connections.Add(connection);

        __connectionIndices[id] = connectionIndex;

        __connectionIDs.Add(connection, id);

        int connectionCount = __connections.Length;
        if (__sendBuffers.Length < connectionCount)
        {
            SendBuffer sendBuffer;
            sendBuffer.index = 0;
            sendBuffer.value = new NetworkSendBuffer(allocator);
            __sendBuffers.Add(sendBuffer);
        }
        else
        {
            ref var sendBuffer = ref __sendBuffers.ElementAt(connectionIndex.value);
            sendBuffer.index = 0;
            sendBuffer.value.Clear();
        }

        if (__targets.Length < connectionCount)
            __targets.Add(new UnsafeList<int>(1, allocator));
        else
            __targets.ElementAt(connectionIndex.value).Clear();

        int channelCount = this.channelCount, destinationBufferCount = GetBufferCountPerConnection(channelCount, connectionCount) * connectionCount, sourceBufferCount = __buffers.Length;
        if (sourceBufferCount < destinationBufferCount)
        {
            __buffers.ResizeUninitialized(destinationBufferCount);

            for(int i = sourceBufferCount; i < destinationBufferCount; i++)
                __buffers.ElementAt(i) = new NetworkSendBuffer(allocator);
        }

        destinationBufferCount = math.min(destinationBufferCount, sourceBufferCount);
        sourceBufferCount = GetBufferCountPerConnection(ChannelCount == 0 ? connectionIndex.channelIndex : ChannelCount, connectionIndex.value) * connectionIndex.value;
        for (int i = sourceBufferCount; i < destinationBufferCount; i++)
            __buffers.ElementAt(i).Clear();

        __sendAllConnectionIndices.Capacity = math.max(__sendAllConnectionIndices.Capacity, connectionCount);

        __sendChannelConnectionIndices.Capacity = math.max(__sendChannelConnectionIndices.Capacity, channelCount * connectionCount);
        __sendIdentityConnectionIndices.Capacity = math.max(__sendIdentityConnectionIndices.Capacity, connectionCount * connectionCount);

        return id;
    }

    public uint Disconnect(in NetworkConnection connection)
    {
        if(!__connectionIDs.TryGetValue(connection, out var id))
            return 0;

        __connectionIDs.Remove(connection);

        uint tempID;
        ConnectionIndex connectionIndex = __connectionIndices[id], tempConnectionIndex;
        foreach(var pair in __connectionIDs)
        {
            tempID = pair.Value;
            tempConnectionIndex = __connectionIndices[tempID];
            if (tempConnectionIndex.value > connectionIndex.value)
            {
                --tempConnectionIndex.value;

                __connectionIndices[tempID] = tempConnectionIndex;
            }
        }

        __connections.RemoveAt(connectionIndex.value);

        connectionIndex.value = -1;
        __connectionIndices[id] = connectionIndex;

        return id;
    }

    public static int GetTargetFromChannel(int channel)
    {
        return -channel - 1;
    }

    public static int GetTargetFromConnectionIndex(int connectionIndex)
    {
        return connectionIndex + 1;
    }

    public static int GetBufferCountPerConnection(int channelCount, int connectionCount)
    {
        return connectionCount + channelCount + 1;
    }

    public static int GetBufferIndex(int target, int channelCount, int connectionIndex, int connectionCount)
    {
        return target + connectionCount + GetBufferCountPerConnection(channelCount, connectionCount) * connectionIndex;
    }

    public static int GetBufferTarget(int bufferIndex, int channelCount, int connectionCount, out int connectionIndex)
    {
        int bufferCountPerConnection = GetBufferCountPerConnection(channelCount, connectionCount);
        connectionIndex = bufferIndex / bufferCountPerConnection;

        return bufferIndex - connectionIndex * bufferCountPerConnection - connectionCount;
    }
}