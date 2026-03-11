using System;
using Unity.Jobs;
using Unity.Burst;
using Unity.Entities;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Mathematics;
using Unity.Networking.Transport;
using Unity.Networking.Transport.Error;

namespace ZG
{
    public enum NetworkServerPipelineType
    {
        SendSelfFromOthers, 
        Custom, 
        SendOthers, 
        SendOthersFromChannel,
        SendSelf
    }

    public interface INetworkServerListener
    {
        void Connect(in NetworkConnection connection, uint id);

        void Disconnect(in NetworkConnection connection, uint id);
    }
    
    public interface INetworkServerHandler
    {
        void Connect(NetworkServerSendBufferWrapper sendBuffer);

        void Disconnect(NetworkServerSendBufferWrapper sendBuffer);

        void Read(ref DataStreamReader reader,
            NetworkServerSendBufferWrapper sendBuffer);
    }

    public interface INetworkServerBufferHandler
    {
        bool Apply(
            int pipelineIndex, 
            DataStreamReader reader,
            in NetworkConnection source, 
            in NetworkConnection destination, 
            ref NetworkDriver.Concurrent driver, 
            ref NetworkServerSendBuffer.Concurrent sendBuffer);
    }

    public struct NetworkServerSendBufferWrapper
    {
        public uint ID;
        
        public readonly NetworkConnection Connection;
        
        private NetworkServerSendBuffer.Concurrent __sendBuffer;

        public NativeArray<byte> payload => __sendBuffer.GetPayload(ID);

        internal NetworkServerSendBufferWrapper(in NetworkConnection connection,
            ref NetworkServerSendBuffer.Concurrent sendBuffer)
        {
            ID = sendBuffer[connection];
            Connection = connection;
            __sendBuffer = sendBuffer;
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
            int pipelineIndex,
            out DataStreamWriter writer, short capacity = 1024)
        {
            return __sendBuffer.BeginWrite(pipelineIndex, ID, out writer, capacity);
        }

        public void EndWrite(in DataStreamWriter writer)
        {
            __sendBuffer.EndWrite(writer);
        }
    }

    [BurstCompile]
    public struct NetworkServerInitJob<T> : IJob where T : unmanaged, INetworkServerListener
    {
        public T listener;
        public NetworkDriver driver;

        public NetworkServerSendBuffer sendBuffer;

        public NativeList<NetworkConnection> connections;
        public NativeList<NetworkConnection> connectionsToConnect;
        public NativeList<NetworkConnection> connectionsToDisconnect;

        public void Execute()
        {
            uint id;
            int connectionIndex;
            foreach (var connectionToDisconnect in connectionsToDisconnect)
            {
                connectionIndex = connections.IndexOf(connectionToDisconnect);
                if(connectionIndex != -1)
                    connections.RemoveAtSwapBack(connectionIndex);
                    
                id = sendBuffer.Disconnect(connectionToDisconnect);
                
                listener.Disconnect(connectionToDisconnect, id);
            }
                
            connectionsToDisconnect.Clear();
                
            connectionsToConnect.Clear();

            NetworkConnection connection;
            while ((connection = driver.Accept(out var payload)) != default)
            {
                connectionsToConnect.Add(connection);
                
                connections.Add(connection);
                
                id = sendBuffer.Connect(connection, payload);
                
                listener.Connect(connection, id);
            }

            connectionsToDisconnect.Capacity = math.max(connectionsToDisconnect.Capacity, connections.Length);
        }
    }

    [BurstCompile]
    public struct NetworkServerPopEventsJob<T> : IJobParallelForDefer where T : unmanaged, INetworkServerHandler
    {
        public T handler;
        public NetworkDriver.Concurrent driver;
        public NetworkServerSendBuffer.Concurrent sendBuffer;

        public NativeList<NetworkConnection>.ParallelWriter connectionsToDisconnect;

        [ReadOnly]
        public NativeArray<NetworkConnection> connectionsToConnect;

        [ReadOnly]
        public NativeArray<NetworkConnection> connections;

        public void Execute(int index)
        {
            var connection = connections[index];
            
            var sendBuffer = new NetworkServerSendBufferWrapper(connection, ref this.sendBuffer);

            if(connectionsToConnect.IndexOf(connection) != -1)
                handler.Connect(sendBuffer);
            
            bool isEmpty = false;
            NetworkEvent.Type cmd;
            DataStreamReader reader;
            do
            {
                cmd = driver.PopEventForConnection(connection, out reader);
                switch (cmd)
                {
                    case NetworkEvent.Type.Empty:
                        isEmpty = true;
                        break;
                    case NetworkEvent.Type.Data:
                        int messageSize;
                        do
                        {
                            messageSize = reader.ReadUShort();
                            /*unsafe
                            {
                                reader = new DataStreamReader(
                                    CollectionHelper.ConvertExistingDataToNativeArray<byte>(
                                        (byte*)stream.GetUnsafeReadOnlyPtr() + stream.GetBytesRead(), 
                                        messageSize, 
                                        Allocator.None, 
                                        true));
                            }*/
                            using (var bytes = new NativeArray<byte>(messageSize, Allocator.Temp))
                            {
                                reader.ReadBytes(bytes);

                                var stream = new DataStreamReader(bytes);
                                
                                handler.Read(ref stream, sendBuffer);
                            }
                        } while (reader.GetBytesRead() < reader.Length);

                        break;
                    case NetworkEvent.Type.Connect:
                        handler.Connect(sendBuffer);
                        
                        break;
                    case NetworkEvent.Type.Disconnect:

                        __LogDisconnectReason(connection, (DisconnectReason)reader.ReadByte());

                        handler.Disconnect(sendBuffer);

                        connectionsToDisconnect.AddNoResize(connection);
                        break;
                }
            } while (!isEmpty);
        }

        private void __LogDisconnectReason(in NetworkConnection connection, DisconnectReason disconnectReason)
        {
            UnityEngine.Debug.LogError($"[{connection}]DisconnectReason: {(int)disconnectReason}");
        }
    }

    [BurstCompile]
    public struct NetworkServerSendJob<T> : IJobParallelForDefer where T : unmanaged, INetworkServerBufferHandler
    {
        public T bufferHandler;
        
        [ReadOnly]
        public NativeArray<NetworkConnection> connections;

        public NetworkDriver.Concurrent driver;
            
        public NetworkServerSendBuffer.Concurrent sendBuffer;

        public void Execute(int index)
        {
            sendBuffer.Apply(connections[index], ref driver, ref bufferHandler);
        }
    }

    public struct NetworkServerSendBuffer : IComponentData
    {
        private struct Pipeline
        {
            public NetworkServerPipelineType type;
            
            public NetworkPipeline value;
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
        }
        
        private struct Index
        {
            private struct Comparer : System.Collections.Generic.IComparer<PipelineBuffer>
            {
                public NativeList<Pipeline> pipelines;
                
                public int Compare(PipelineBuffer x, PipelineBuffer y)
                {
                    return ((int)pipelines[x.pipeline].type).CompareTo((int)pipelines[y.pipeline].type);
                }
            }

            public struct PipelineBuffer
            {
                public int pipeline;
                public int buffer;
            }

            public int payloadOffset;
            public int payloadSize;
            public int channel;
            public FixedList32Bytes<PipelineBuffer> pipelineBuffers;

            public void Sort(in NativeList<Pipeline> pipelines)
            {
                Comparer comparer;
                comparer.pipelines = pipelines;

                unsafe
                {
                    NativeSortExtension.Sort(
                        (PipelineBuffer*)((byte*)UnsafeUtility.AddressOf(ref pipelineBuffers) + UnsafeUtility.SizeOf<ushort>()),
                        pipelineBuffers.Length, comparer);
                }
            }
        }

        public struct Concurrent
        {
            [ReadOnly]
            private NativeHashMap<NetworkConnection, uint> __ids;

            [ReadOnly]
            private NativeHashMap<uint, Index> __indices;

            [ReadOnly]
            private NativeList<Pipeline> __pipelines;

            [NativeDisableParallelForRestriction]
            private NativeArray<NetworkSendBuffer> __buffers;

            [NativeDisableParallelForRestriction]
            private NativeArray<Channel> __channels;
            
            [NativeDisableParallelForRestriction]
            private NativeArray<byte> __payload;

            public uint this[NetworkConnection connection] => __ids[connection];

            public Concurrent(ref NetworkServerSendBuffer buffer)
            {
                __ids = buffer.__ids;
                __indices = buffer.__indices;
                __pipelines = buffer.__pipelines;
                __buffers = buffer.__buffers.AsDeferredJobArray();
                __channels = buffer.__channels.AsDeferredJobArray();
                __payload = buffer.__payloads.AsDeferredJobArray();
            }

            public NativeArray<byte> GetPayload(uint id)
            {
                var index = __indices[id];
                
                return __payload.GetSubArray(index.payloadOffset, index.payloadSize);
            }

            public bool AddChannel(uint id, int value)
            {
                int channelIndex = __indices[id].channel;
                Channel channel = __channels[channelIndex];

                if (channel.Add(value))
                {
                    __channels[channelIndex] = channel;

                    return true;
                }
                
                return false;
            }

            public bool RemoveChannel(uint id, int value)
            {
                int channelIndex = __indices[id].channel;
                Channel channel = __channels[channelIndex];

                if (channel.Remove(value))
                {
                    __channels[channelIndex] = channel;

                    return true;
                }

                return false;
            }

            public bool BeginWrite(
                int pipelineIndex,
                uint id, 
                out DataStreamWriter writer, short capacity = 1024)
            {
                foreach (var pipelineBuffers in __indices[id].pipelineBuffers)
                {
                    if (pipelineBuffers.pipeline == pipelineIndex)
                    {
                        var buffer = __buffers[pipelineBuffers.buffer];
                        bool result = buffer.BeginWrite(out writer, capacity);
                        __buffers[pipelineBuffers.buffer] = buffer;

                        writer.m_SendHandleData = (IntPtr)pipelineBuffers.buffer;

                        return result;
                    }
                }

                writer = default;

                return false;
            }

            public void EndWrite(in DataStreamWriter writer)
            {
                int index = (int)writer.m_SendHandleData;
                var buffer = __buffers[index];
                buffer.EndWrite(writer);
                __buffers[index] = buffer;
            }

            public void Apply<T>(
                in NetworkConnection connection, 
                ref NetworkDriver.Concurrent driver, 
                ref T handler) where T : INetworkServerBufferHandler
            {
                /*Pipeline pipeline;
                NetworkSendBuffer buffer;
                foreach (var index in __indices.GetValuesForKey(connection))
                {
                    pipeline = __pipelines[index.pipeline];
                    switch (pipeline.type)
                    {
                        case NetworkServerPipelineType.SendSelfFromOthers:
                            buffer = __buffers[index.buffer];
                            buffer.Apply(connection, pipeline.value, ref driver);
                            __buffers[index.buffer] = buffer;
                            break;
                    }
                }*/

                var id = __ids[connection];
                Index index = __indices[id], tempIndex;
                Channel channel = __channels[__indices[id].channel], tempChannel;
                Pipeline pipeline, tempPipeline;
                NetworkSendBuffer buffer, tempBuffer;
                NetworkConnection tempConnection;
                foreach (var tempID in __ids)
                {
                    tempIndex = __indices[tempID.Value];
                    tempChannel = __channels[tempIndex.channel];
                    if (!tempChannel.Or(channel))
                        continue;

                    tempConnection = tempID.Key;

                    foreach (var tempPipelineBuffer in tempIndex.pipelineBuffers)
                    {
                        tempPipeline = __pipelines[tempPipelineBuffer.pipeline];

                        switch (tempPipeline.type)
                        {
                            case NetworkServerPipelineType.Custom:
                                buffer = __buffers[tempPipelineBuffer.buffer];
                                while (buffer.ReadNext(out var bytes))
                                {
                                    if (!handler.Apply(
                                            tempPipelineBuffer.pipeline,
                                            new DataStreamReader(bytes),
                                            tempConnection,
                                            connection,
                                            ref driver,
                                            ref this))
                                    {
                                        foreach (var pipelineBuffer in index.pipelineBuffers)
                                        {
                                            pipeline = __pipelines[pipelineBuffer.pipeline];
                                            if (NetworkServerPipelineType.SendSelfFromOthers == pipeline.type &&
                                                pipeline.value == tempPipeline.value)
                                            {
                                                tempBuffer = __buffers[pipelineBuffer.buffer];
                                                if (tempBuffer.BeginWrite(out var writer))
                                                {
                                                    writer.WriteBytes(bytes);

                                                    tempBuffer.EndWrite(writer);

                                                    __buffers[pipelineBuffer.buffer] = tempBuffer;
                                                }

                                                break;
                                            }
                                        }
                                    }
                                }

                                break;
                            case NetworkServerPipelineType.SendOthers:
                            case NetworkServerPipelineType.SendOthersFromChannel:
                                if (connection != tempConnection &&
                                    (NetworkServerPipelineType.SendOthersFromChannel != tempPipeline.type ||
                                     tempChannel.And(channel)))
                                {
                                    buffer = __buffers[tempPipelineBuffer.buffer];
                                    if (!buffer.Apply(connection, tempPipeline.value, ref driver))
                                    {
                                        foreach (var pipelineBuffer in index.pipelineBuffers)
                                        {
                                            pipeline = __pipelines[pipelineBuffer.pipeline];
                                            if (NetworkServerPipelineType.SendSelfFromOthers == pipeline.type &&
                                                pipeline.value == tempPipeline.value)
                                            {
                                                tempBuffer = __buffers[pipelineBuffer.buffer];
                                                tempBuffer.Append(buffer);

                                                __buffers[pipelineBuffer.buffer] = tempBuffer;

                                                break;
                                            }
                                        }
                                    }
                                }

                                break;
                            case NetworkServerPipelineType.SendSelf:
                            case NetworkServerPipelineType.SendSelfFromOthers:
                                if (connection == tempConnection)
                                {
                                    buffer = __buffers[tempPipelineBuffer.buffer];
                                    buffer.Apply(connection, tempPipeline.value, ref driver);
                                    __buffers[tempPipelineBuffer.buffer] = buffer;
                                }

                                break;
                        }
                    }
                }
            }

            public void Clear(in NetworkConnection connection)
            {
                Pipeline pipeline;
                NetworkSendBuffer buffer;
                var id = __ids[connection];
                foreach (var pipelineBuffer in __indices[id].pipelineBuffers)
                {
                    pipeline = __pipelines[pipelineBuffer.pipeline];
                    switch (pipeline.type)
                    {
                        case NetworkServerPipelineType.SendOthers:
                        case NetworkServerPipelineType.SendOthersFromChannel:
                        case NetworkServerPipelineType.Custom:
                            buffer = __buffers[pipelineBuffer.buffer];
                            buffer.Clear();
                            __buffers[pipelineBuffer.buffer] = buffer;
                            break;
                    }
                }
            }
        }

        private NativeList<byte> __payloads;
        private NativeList<Channel> __channels;
        private NativeList<Pipeline> __pipelines;
        private NativeList<NetworkSendBuffer> __buffers;
        private NativeHashMap<uint, Index> __indices;
        private NativeHashMap<NetworkConnection, uint> __ids;

        public unsafe AllocatorManager.AllocatorHandle allocator => __pipelines.GetUnsafeList()->Allocator;

        public NetworkServerSendBuffer(
            in AllocatorManager.AllocatorHandle allocator)
        {
            __payloads = new NativeList<byte>(allocator);
            
            __channels = new NativeList<Channel>(allocator);
            
            __pipelines = new NativeList<Pipeline>(allocator);

            __buffers = new NativeList<NetworkSendBuffer>(allocator);

            __indices = new NativeHashMap<uint, Index>(1, allocator);

            __ids = new NativeHashMap<NetworkConnection, uint>(1, allocator);
        }

        public void Dispose()
        {
            __payloads.Dispose();
            
            foreach (var channels in __channels)
                channels.Dispose();

            __channels.Dispose();

            __pipelines.Dispose();

            foreach (var buffer in __buffers)
                buffer.Dispose();

            __buffers.Dispose();

            __indices.Dispose();

            __ids.Dispose();
        }

        public void Clear()
        {
            int length = math.min(__pipelines.Length, __buffers.Length);
            for (int i = 0; i < length; ++i)
                __buffers.ElementAt(i).Clear();
        }

        public Concurrent AsConcurrent() => new Concurrent(ref this);

        public int CreatePipeline(NetworkServerPipelineType type, in NetworkPipeline value)
        {
            int result = __pipelines.Length;
            Pipeline pipeline;
            for (int i = 0; i < result; ++i)
            {
                pipeline = __pipelines[i];
                if (pipeline.type == type && pipeline.value == value)
                    return i;
            }

            Index.PipelineBuffer pipelineBuffer;
            pipelineBuffer.pipeline = __pipelines.Length;
            
            pipeline.type = type;
            pipeline.value = value;
            __pipelines.Add(pipeline);

            using (var keys = __indices.GetKeyArray(Allocator.Temp))
            {
                Index index;
                foreach (var key in keys)
                {
                    index = __indices[key];
                    
                    pipelineBuffer.buffer = __Alloc();
                    index.pipelineBuffers.Add(pipelineBuffer);
                    index.Sort(__pipelines);
                    
                    __indices[key] = index;
                }
            }

            return result;
        }

        public uint Connect(in NetworkConnection connection, in NativeArray<byte> payload)
        {
            uint id = new DataStreamReader(payload).ReadPackedUInt(StreamCompressionModel.Default);
            
            __ids.Add(connection, id);

            if (__indices.TryGetValue(id, out var index))
            {
                NativeArray<byte>.Copy(payload, 0, __payloads.AsArray(), index.payloadOffset, index.payloadSize);
                
                return id;
            }

            index.payloadOffset = __payloads.Length;
            index.payloadSize = payload.Length;
            
            __payloads.AddRange(payload);
            
            index.channel = __channels.Length;
                
            __channels.Add(new Channel(allocator));

            Index.PipelineBuffer pipelineBuffer;
            int numPipelines = __pipelines.Length;
            for (int i = 0; i < numPipelines; ++i)
            {
                pipelineBuffer.pipeline = i;
                pipelineBuffer.buffer = __Alloc();
                
                index.pipelineBuffers.Add(pipelineBuffer);
            }
            
            index.Sort(__pipelines);
            
            __indices.Add(id, index);

            return id;
        }

        public uint Disconnect(in NetworkConnection connection)
        {
            uint id = __ids[connection];
            __ids.Remove(connection);
            return id;
        }

        private int __Alloc()
        {
            int result = __buffers.Length;

            __buffers.Add(new NetworkSendBuffer(allocator));

            return result;
        }
    }

    public struct NetworkServer
    {
        [BurstCompile]
        private struct Clear : IJobParallelForDefer
        {
            [ReadOnly]
            public NativeArray<NetworkConnection> connections;

            public NetworkServerSendBuffer.Concurrent sendBuffer;

            public void Execute(int index)
            {
                sendBuffer.Clear(connections[index]);
            }
        }

        private NetworkDriver __driver;
        private NativeList<NetworkConnection> __connections;
        private NativeList<NetworkConnection> __connectionsToConnect;
        private NativeList<NetworkConnection> __connectionsToDisconnect;

        public NetworkServer(in NetworkSettings settings, in AllocatorManager.AllocatorHandle allocator)
        {
            __driver = NetworkDriver.Create(settings);

            __connections = new NativeList<NetworkConnection>(allocator);
            __connectionsToConnect = new NativeList<NetworkConnection>(allocator);
            __connectionsToDisconnect = new NativeList<NetworkConnection>(allocator);
        }

        public void Dispose()
        {
            __driver.Dispose();
            __connections.Dispose();
            __connectionsToConnect.Dispose();
            __connectionsToDisconnect.Dispose();
        }

        public NetworkPipeline CreatePipeline(in NativeArray<NetworkPipelineStageId> stages)
        {
            return __driver.CreatePipeline(stages);
        }

        public void Listen(ushort port, NetworkFamily family = NetworkFamily.Ipv4)
        {
            NetworkEndpoint endpoint;
            switch(family)
            {
                case NetworkFamily.Ipv4:
                    endpoint = NetworkEndpoint.AnyIpv4;// The local address to which the client will connect to is 127.0.0.1
                    break;
                case NetworkFamily.Ipv6:
                    endpoint = NetworkEndpoint.AnyIpv6;
                    break;
                default:
                    endpoint = default;
                    break;
            }

            endpoint.Port = port;
            if (__driver.Bind(endpoint) != 0 || __driver.Listen() != 0)
                UnityEngine.Debug.LogError($"Failed to bind to port {port}");
        }

        public void Disconnect(in NetworkConnection connection)
        {
            __driver.Disconnect(connection);
        }

        public JobHandle Schedule<TListener, THandler, TBufferHandler>(
            ref TListener listener, 
            ref THandler handler, 
            ref TBufferHandler bufferHandler, 
            ref NetworkServerSendBuffer sendBuffer,
            int innerloopBatchCount, 
            in JobHandle inputDeps) 
            where TListener : unmanaged, INetworkServerListener
            where THandler : unmanaged, INetworkServerHandler
            where TBufferHandler : unmanaged, INetworkServerBufferHandler
        {
            var driver = __driver.ToConcurrent();
            var sendBufferConcurrent = sendBuffer.AsConcurrent();

            NetworkServerInitJob<TListener> init;
            init.listener = listener;
            init.driver = __driver;
            init.sendBuffer = sendBuffer;
            init.connections = __connections;
            init.connectionsToConnect = __connectionsToConnect;
            init.connectionsToDisconnect = __connectionsToDisconnect;
            var jobHandle = init.ScheduleByRef(inputDeps);

            var connections = __connections.AsDeferredJobArray();
            
            jobHandle = __driver.ScheduleUpdate(jobHandle);

            NetworkServerPopEventsJob<THandler> popEvents;
            popEvents.handler = handler;
            popEvents.driver = driver;
            popEvents.sendBuffer = sendBufferConcurrent;
            popEvents.connectionsToConnect = __connectionsToConnect.AsDeferredJobArray();
            popEvents.connectionsToDisconnect = __connectionsToDisconnect.AsParallelWriter();
            popEvents.connections = connections;

            jobHandle = popEvents.ScheduleByRef(__connections, innerloopBatchCount, jobHandle);
            
            NetworkServerSendJob<TBufferHandler> send;
            send.bufferHandler = bufferHandler;
            send.connections = connections;
            send.driver = driver;
            send.sendBuffer = sendBufferConcurrent;
            jobHandle = send.ScheduleByRef(__connections, innerloopBatchCount, jobHandle);
            
            Clear clear;
            clear.connections = connections;
            clear.sendBuffer = sendBufferConcurrent;
            
            return clear.ScheduleByRef(__connections, innerloopBatchCount, jobHandle);
        }
    }
}
