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
        void Connect(in NetworkConnection connection);

        void Disconnect(in NetworkConnection connection);
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
        public readonly NetworkConnection Connection;
        
        private NetworkServerSendBuffer.Concurrent __sendBuffer;

        internal NetworkServerSendBufferWrapper(in NetworkConnection connection,
            ref NetworkServerSendBuffer.Concurrent sendBuffer)
        {
            Connection = connection;
            __sendBuffer = sendBuffer;
        }

        public bool AddChannel(int value)
        {
            return __sendBuffer.AddChannel(Connection, value);
        }
        
        public bool RemoveChannel(int value)
        {
            return __sendBuffer.RemoveChannel(Connection, value);
        }

        public bool BeginWrite(
            int pipelineIndex,
            out DataStreamWriter writer, short capacity = 1024)
        {
            return __sendBuffer.BeginWrite(pipelineIndex, Connection, out writer, capacity);
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
        public NativeList<NetworkConnection> connectionsToDisconnect;

        public void Execute()
        {
            int connectionIndex;
            foreach (var connectionToDisconnect in connectionsToDisconnect)
            {
                connectionIndex = connections.IndexOf(connectionToDisconnect);
                if(connectionIndex != -1)
                    connections.RemoveAtSwapBack(connectionIndex);
                    
                sendBuffer.Disconnect(connectionToDisconnect);
                
                listener.Disconnect(connectionToDisconnect);
            }
                
            connectionsToDisconnect.Clear();
                
            NetworkConnection connection;
            while ((connection = driver.Accept()) != default)
            {
                connections.Add(connection);
                    
                sendBuffer.Connect(connection);
                
                listener.Connect(connection);
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
        public NativeArray<NetworkConnection> connections;

        public void Execute(int index)
        {
            var connection = connections[index];

            var sendBuffer = new NetworkServerSendBufferWrapper(connection, ref this.sendBuffer);

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
                bool result = __values.IsEmpty;
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
                bool result = __values.IsEmpty;
                if (!result)
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
            public int pipeline;
            public int buffer;
        }

        public struct Concurrent
        {
            private struct Comparer : System.Collections.Generic.IComparer<Index>
            {
                public NativeList<Pipeline> pipelines;
                
                public int Compare(Index x, Index y)
                {
                    return pipelines[x.pipeline].type.CompareTo(pipelines[y.pipeline].type);
                }
            }
            
            [ReadOnly]
            private NativeParallelMultiHashMap<NetworkConnection, Index> __indices;
            
            [ReadOnly]
            private NativeHashMap<NetworkConnection, int> __channelIndices;

            [ReadOnly]
            private NativeList<Pipeline> __pipelines;

            [NativeDisableParallelForRestriction]
            private NativeArray<NetworkSendBuffer> __buffers;

            [NativeDisableParallelForRestriction]
            private NativeArray<Channel> __channels;

            public Concurrent(ref NetworkServerSendBuffer buffer)
            {
                __indices = buffer.__indices;
                __channelIndices = buffer.__channelIndices;
                __pipelines = buffer.__pipelines;
                __buffers = buffer.__buffers.AsDeferredJobArray();
                __channels = buffer.__channels.AsDeferredJobArray();
            }

            public bool AddChannel(in NetworkConnection connection, int value)
            {
                int channelIndex = __channelIndices[connection];
                Channel channel = __channels[channelIndex];

                if (channel.Add(value))
                {
                    __channels[channelIndex] = channel;

                    return true;
                }
                
                return false;
            }

            public bool RemoveChannel(in NetworkConnection connection, int value)
            {
                int channelIndex = __channelIndices[connection];
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
                in NetworkConnection connection, 
                out DataStreamWriter writer, short capacity = 1024)
            {
                if (__indices.TryGetFirstValue(connection, out var index, out var iterator))
                {
                    do
                    {
                        if (index.pipeline == pipelineIndex)
                        {
                            var buffer = __buffers[index.buffer];
                            bool result = buffer.BeginWrite(out writer, capacity);
                            __buffers[index.buffer] = buffer;

                            writer.m_SendHandleData = (IntPtr)index.buffer;

                            return result;
                        }
                    } while (__indices.TryGetNextValue(out index, ref iterator));
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

                Channel channel = __channels[__channelIndices[connection]], tempChannel;
                Pipeline pipeline, tempPipeline;
                NetworkSendBuffer buffer, tempBuffer;
                NetworkConnection tempConnection;
                NativeList<Index> indices = default;
                foreach (var channelIndex in __channelIndices)
                {
                    tempChannel = __channels[channelIndex.Value];
                    if (!tempChannel.Or(channel))
                        continue;

                    tempConnection = channelIndex.Key;

                    if (indices.IsCreated)
                        indices.Clear();
                    else
                        indices = new NativeList<Index>(Allocator.Temp);
                    
                    foreach (var index in __indices.GetValuesForKey(tempConnection))
                        indices.Add(index);

                    Comparer comparer;
                    comparer.pipelines = __pipelines;
                    indices.Sort(comparer);

                    foreach (var index in indices)
                    {
                        pipeline = __pipelines[index.pipeline];

                        switch (pipeline.type)
                        {
                            case NetworkServerPipelineType.Custom:
                                buffer = __buffers[index.buffer];
                                while (buffer.ReadNext(out var bytes))
                                {
                                    if (!handler.Apply(
                                            index.pipeline,
                                            new DataStreamReader(bytes),
                                            tempConnection,
                                            connection,
                                            ref driver,
                                            ref this))
                                    {
                                        foreach (var tempIndex in __indices.GetValuesForKey(connection))
                                        {
                                            tempPipeline = __pipelines[tempIndex.pipeline];
                                            if (NetworkServerPipelineType.SendSelfFromOthers == tempPipeline.type &&
                                                tempPipeline.value == pipeline.value)
                                            {
                                                tempBuffer = __buffers[tempIndex.buffer];
                                                if (tempBuffer.BeginWrite(out var writer))
                                                {
                                                    writer.WriteBytes(bytes);

                                                    tempBuffer.EndWrite(writer);

                                                    __buffers[tempIndex.buffer] = tempBuffer;
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
                                    (NetworkServerPipelineType.SendOthersFromChannel != pipeline.type ||
                                     tempChannel.And(channel)))
                                {
                                    buffer = __buffers[index.buffer];
                                    if (!buffer.Apply(connection, pipeline.value, ref driver))
                                    {
                                        foreach (var tempIndex in __indices.GetValuesForKey(connection))
                                        {
                                            tempPipeline = __pipelines[tempIndex.pipeline];
                                            if (NetworkServerPipelineType.SendSelfFromOthers == tempPipeline.type &&
                                                tempPipeline.value == pipeline.value)
                                            {
                                                tempBuffer = __buffers[tempIndex.buffer];
                                                tempBuffer.Append(buffer);

                                                __buffers[tempIndex.buffer] = tempBuffer;

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
                                    buffer = __buffers[index.buffer];
                                    buffer.Apply(connection, pipeline.value, ref driver);
                                    __buffers[index.buffer] = buffer;
                                }

                                break;
                        }
                    }
                }
                
                if(indices.IsCreated)
                    indices.Dispose();
            }

            public void Clear(in NetworkConnection connection)
            {
                if (__indices.TryGetFirstValue(connection, out var index, out var iterator))
                {
                    Pipeline pipeline;
                    NetworkSendBuffer buffer;
                    do
                    {
                        pipeline = __pipelines[index.pipeline];
                        switch (pipeline.type)
                        {
                            case NetworkServerPipelineType.SendOthers:
                            case NetworkServerPipelineType.SendOthersFromChannel:
                            case NetworkServerPipelineType.Custom:
                                buffer = __buffers[index.buffer];
                                buffer.Clear();
                                __buffers[index.buffer] = buffer;
                                break;
                        }

                    } while (__indices.TryGetNextValue(out index, ref iterator));
                }
            }
        }

        private NativeList<int> __bufferIndexPool;
        private NativeList<int> __channelIndexPool;
        private NativeList<Channel> __channels;
        private NativeList<Pipeline> __pipelines;
        private NativeList<NetworkSendBuffer> __buffers;
        private NativeHashMap<NetworkConnection, int> __channelIndices;
        private NativeParallelMultiHashMap<NetworkConnection, Index> __indices;

        public unsafe AllocatorManager.AllocatorHandle allocator => __pipelines.GetUnsafeList()->Allocator;

        public NetworkServerSendBuffer(
            in AllocatorManager.AllocatorHandle allocator)
        {
            __bufferIndexPool = new NativeList<int>(allocator);
            
            __channelIndexPool = new NativeList<int>(allocator);
            
            __channels = new NativeList<Channel>(allocator);
            
            __pipelines = new NativeList<Pipeline>(allocator);

            __buffers = new NativeList<NetworkSendBuffer>(allocator);
            
            __channelIndices = new NativeHashMap<NetworkConnection, int>(1, allocator);

            __indices = new NativeParallelMultiHashMap<NetworkConnection, Index>(1, allocator);
        }

        public void Dispose()
        {
            __bufferIndexPool.Dispose();

            __channelIndexPool.Dispose();

            foreach (var channels in __channels)
                channels.Dispose();

            __channels.Dispose();

            __pipelines.Dispose();

            foreach (var buffer in __buffers)
                buffer.Dispose();

            __buffers.Dispose();

            __channelIndices.Dispose();

            __indices.Dispose();
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
            
            pipeline.type = type;
            pipeline.value = value;
            __pipelines.Add(pipeline);

            Index index;
            index.pipeline = __pipelines.Length;
            index.buffer = __Alloc();
            
            using (var keys = __indices.GetKeyArray(Allocator.Temp))
            {
                foreach (var key in keys)
                    __indices.Add(key, index);
            }

            return result;
        }

        public void Connect(in NetworkConnection connection)
        {
            int channelIndex, length = __channelIndexPool.Length;
            if (length > 0)
            {
                channelIndex = __channelIndexPool[--length];
                __channelIndexPool.ResizeUninitialized(length);
                
                __channels.ElementAt(channelIndex).Clear();
            }
            else
            {
                channelIndex = __channels.Length;
                
                __channels.Add(new Channel(allocator));
            }
            
            __channelIndices.Add(connection, channelIndex);
            
            Index index;
            int numPipelines = __pipelines.Length;
            for (int i = 0; i < numPipelines; ++i)
            {
                index.pipeline = i;
                index.buffer = __Alloc();
                
                __indices.Add(connection, index);
            }
        }

        public void Disconnect(in NetworkConnection connection)
        {
            __channelIndexPool.Add(__channelIndices[connection]);
            __channelIndices.Remove(connection);
            
            foreach (var index in __indices.GetValuesForKey(connection))
                __bufferIndexPool.Add(index.buffer);
            
            __indices.Remove(connection);
        }

        private int __Alloc()
        {
            int result, length = __bufferIndexPool.Length;
            if (length > 0)
            {
                result = __bufferIndexPool[--length];
                __bufferIndexPool.ResizeUninitialized(length);
                
                __buffers.ElementAt(result).Clear();
            }
            else
            {
                result = __buffers.Length;
                
                __buffers.Add(new NetworkSendBuffer(allocator));
            }

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
        private NativeList<NetworkConnection> __connectionsToDisconnect;

        public NetworkServer(in NetworkSettings settings, in AllocatorManager.AllocatorHandle allocator)
        {
            __driver = NetworkDriver.Create(settings);

            __connections = new NativeList<NetworkConnection>(allocator);
            __connectionsToDisconnect = new NativeList<NetworkConnection>(allocator);
        }

        public void Dispose()
        {
            __driver.Dispose();
            __connections.Dispose();
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
            init.connectionsToDisconnect = __connectionsToDisconnect;
            var jobHandle = init.ScheduleByRef(inputDeps);

            var connections = __connections.AsDeferredJobArray();
            
            jobHandle = __driver.ScheduleUpdate(jobHandle);

            NetworkServerPopEventsJob<THandler> popEvents;
            popEvents.handler = handler;
            popEvents.driver = driver;
            popEvents.sendBuffer = sendBufferConcurrent;
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
