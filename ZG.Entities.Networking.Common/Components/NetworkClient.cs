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

    public enum NetworkClientMessageType
    {
        Connect, 
        Data, 
        Disconnect
    }

    public struct NetworkClientSendBuffer : IComponentData
    {
        public struct ParallelWriter
        {
            [NativeDisableParallelForRestriction]
            private NativeArray<NetworkSendBuffer> __buffers;

            public ParallelWriter(ref NetworkClientSendBuffer buffer)
            {
                __buffers = buffer.__buffers.AsDeferredJobArray();
            }
            
            public bool BeginWrite(int pipelineIndex, out DataStreamWriter writer, short capacity = 1024)
            {
                var buffer = __buffers[pipelineIndex];
                bool result = buffer.BeginWrite(out writer, capacity);

                writer.m_SendHandleData = (IntPtr)pipelineIndex;

                __buffers[pipelineIndex] = buffer;

                return result;
            }

            public void EndWrite(in DataStreamWriter writer)
            {
                int pipelineIndex = (int)writer.m_SendHandleData;
                var buffer = __buffers[pipelineIndex];
                buffer.EndWrite(writer);
                __buffers[pipelineIndex] = buffer;
            }

        }
        
        [ReadOnly]
        private NativeList<NetworkPipeline> __pipelines;
        private NativeList<NetworkSendBuffer> __buffers;
        
        public unsafe AllocatorManager.AllocatorHandle allocator => __pipelines.GetUnsafeList()->Allocator;

        public NetworkClientSendBuffer(in AllocatorManager.AllocatorHandle allocator)
        {
            __pipelines = new NativeList<NetworkPipeline>(allocator);

            __buffers = new NativeList<NetworkSendBuffer>(allocator);
        }

        public void Dispose()
        {
            __pipelines.Dispose();

            foreach (var buffer in __buffers)
                buffer.Dispose();
            
            __buffers.Dispose();
        }

        public void Clear()
        {
            NetworkSendBuffer buffer;
            int length = math.min(__pipelines.Length, __buffers.Length);
            for (int i = 0; i < length; ++i)
            {
                buffer = __buffers[i];
                buffer.Clear();

                __buffers[i] = buffer;
            }
        }

        public ParallelWriter AsParallelWriter()
        {
            return new ParallelWriter(ref this);
        }

        public NetworkPipeline GetPipeline(int pipelineIndex)
        {
            return  __pipelines[pipelineIndex];
        }

        public int CreatePipeline(in NetworkPipeline pipeline)
        {
            int result = __pipelines.Length;
            
            __pipelines.Add(pipeline);
            
            __buffers.Add(new NetworkSendBuffer(allocator));

            return result;
        }

        public bool BeginWrite(int pipelineIndex, out DataStreamWriter writer, short capacity = 1024)
        {
            bool result = __buffers.ElementAt(pipelineIndex).BeginWrite(out writer, capacity);

            writer.m_SendHandleData = (IntPtr)pipelineIndex;

            return result;
        }

        public void EndWrite(in DataStreamWriter writer)
        {
            __buffers.ElementAt((int)writer.m_SendHandleData).EndWrite(writer);
        }

        public void Apply(in NetworkConnection connection, ref NetworkDriver.Concurrent driver)
        {
            int length = math.min(__pipelines.Length, __buffers.Length);
            for (int i = 0; i < length; ++i)
                __buffers.ElementAt(i).Apply(connection, __pipelines[i], ref driver);
        }
    }
    
    public struct NetworkClient
    {
        private struct Header
        {
            public NetworkConnection connection;
            public NetworkEndpoint endpoint;
        }

        public struct Message : IComparable<Message>
        {
            public NetworkClientMessageType type;
            public int offset;
            public int size;

            public DataStreamReader Read(in NativeArray<byte> buffer)
            {
                return new DataStreamReader(buffer.GetSubArray(offset, size));
            }

            public int CompareTo(Message other)
            {
                int result = type.CompareTo(other.type);
                if(0 == result)
                    return offset.CompareTo(other.offset);

                return result;
            }
        }

        [BurstCompile]
        private struct Send : IJob
        {
            [ReadOnly]
            public NativeArray<Header> headers;
            public NetworkDriver.Concurrent driver;
            public NetworkClientSendBuffer sendBuffer;

            public void Execute()
            {
                var connection = headers[0].connection;
                if (NetworkConnection.State.Connected != driver.GetConnectionState(connection))
                    return;
                
                sendBuffer.Apply(connection, ref driver);
            }
        }

        [BurstCompile]
        private struct PopEvents : IJob
        {
            public NetworkDriver driver;
            public NetworkClientSendBuffer sendBuffer;
            public NativeList<byte> buffer;
            public NativeArray<Header> headers;
            public NativeParallelMultiHashMap<NetworkPipeline, Message> messages;

            public void Execute()
            {
                var header = headers[0];

                buffer.Clear();

                messages.Clear();

                bool isEmpty = false, isConnected = NetworkConnection.State.Connected == driver.GetConnectionState(header.connection);
                NetworkEvent.Type cmd;
                Message message;
                DataStreamReader stream;
                NetworkPipeline pipeline;
                do
                {
                    cmd = driver.PopEventForConnection(header.connection, out stream, out pipeline);
                    switch (cmd)
                    {
                        case NetworkEvent.Type.Empty:
                            isEmpty = true;
                            break;
                        case NetworkEvent.Type.Data:
                            message.type = NetworkClientMessageType.Data;
                            
                            do
                            {
                                message.offset = buffer.Length;
                                message.size = stream.ReadUShort();
                                buffer.ResizeUninitialized(message.offset + message.size);
                                stream.ReadBytes(buffer.AsArray().GetSubArray(message.offset, message.size));

                                messages.Add(pipeline, message);
                            } while (stream.GetBytesRead() < stream.Length);

                            break;
                        case NetworkEvent.Type.Connect:
                            isConnected = true;

                            message.type = NetworkClientMessageType.Connect;
                            message.offset = buffer.Length;
                            message.size = 0;

                            messages.Add(pipeline, message);
                            break;
                        case NetworkEvent.Type.Disconnect:
                            isConnected = false;

                            __LogDisconnectReason((DisconnectReason)stream.ReadByte());

                            header.connection = driver.Connect(header.endpoint);

                            var connections = headers.Reinterpret<NetworkConnection>(UnsafeUtility.SizeOf<Header>());
                            connections[0] = header.connection;
                            
                            message.type = NetworkClientMessageType.Disconnect;
                            message.offset = buffer.Length;
                            message.size = 0;

                            messages.Add(pipeline, message);
                            break;
                    }
                } while (!isEmpty);

                if (isConnected)
                {
                    var driver = this.driver.ToConcurrent();
                    sendBuffer.Apply(header.connection, ref driver);
                }
            }

            private void __LogDisconnectReason(DisconnectReason disconnectReason)
            {
                UnityEngine.Debug.LogError($"DisconnectReason: {disconnectReason}");
            }
        }

        public struct MessageIterator : IDisposable
        {
            public struct Element
            {
                private Message __message;
                
                private NativeList<byte> __buffer;
                
                public NetworkClientMessageType type => __message.type;

                public DataStreamReader reader => __message.Read(__buffer.AsArray());

                internal Element(in Message message, in NativeList<byte> buffer)
                {
                    __message = message;
                    __buffer = buffer;
                }
            }

            private bool __isCreatePipelines;
            private int __resultIndex;
            private int __pipelineIndex;
            private NativeArray<NetworkPipeline> __pipelines;
            private NativeList<Message> __results;
            private NativeList<byte> __buffer;
            private NativeParallelMultiHashMap<NetworkPipeline, Message> __messages;

            public Element Current => new Element(__results[__resultIndex], __buffer);

            public MessageIterator(in Messages messages, in AllocatorManager.AllocatorHandle allocator)
            {
                __isCreatePipelines = true;
                __resultIndex = -1;
                __pipelineIndex = -1;
                __pipelines = messages._values.GetKeyArray(allocator);
                __results = new NativeList<Message>(allocator);
                __buffer = messages._buffer;
                __messages = messages._values;
            }
            
            public MessageIterator(in NativeArray<NetworkPipeline> pipelines, in Messages messages, in AllocatorManager.AllocatorHandle allocator)
            {
                __isCreatePipelines = false;
                __resultIndex = -1;
                __pipelineIndex = -1;
                __pipelines = pipelines;
                __results = new NativeList<Message>(allocator);
                __buffer = messages._buffer;
                __messages = messages._values;
            }

            public void Dispose()
            {
                if(__isCreatePipelines)
                    __pipelines.Dispose();
                
                __results.Dispose();
            }

            public bool MoveNext()
            {
                if (++__resultIndex >= __results.Length)
                {
                    while (++__pipelineIndex < __pipelines.Length)
                    {
                        __results.Clear();
                        
                        foreach (var message in __messages.GetValuesForKey(__pipelines[__pipelineIndex]))
                            __results.Add(message);

                        if (!__results.IsEmpty)
                        {
                            __results.Sort();

                            break;
                        }
                    }

                    if (__pipelineIndex < __pipelines.Length)
                        __resultIndex = 0;
                    else
                        return false;
                }

                return true;
            }
        }

        public struct Messages
        {
            internal NativeList<byte> _buffer;
            internal NativeParallelMultiHashMap<NetworkPipeline, Message> _values;
            
            public Messages(in NetworkClient client)
            {
                _buffer = client.__buffer;
                _values = client.__messages;
            }

            public MessageIterator CreateIterator(AllocatorManager.AllocatorHandle allocator)
            {
                return new MessageIterator(in this, allocator);
            }
        }

        private NetworkDriver __driver;
        private NativeArray<Header> __headers;
        private NativeList<byte> __buffer;
        private NativeParallelMultiHashMap<NetworkPipeline, Message> __messages;

        public NetworkConnection.State connectionState => __driver.GetConnectionState(connection);

        public NetworkConnection connection => __headers.Reinterpret<NetworkConnection>(UnsafeUtility.SizeOf<Header>())[0];

        public NetworkClient(in NetworkSettings settings, in AllocatorManager.AllocatorHandle allocator)
        {
            __driver = NetworkDriver.Create(settings);
            __buffer = new NativeList<byte>(allocator);
            __headers = CollectionHelper.CreateNativeArray<Header>(1, allocator, NativeArrayOptions.ClearMemory);
            __messages = new NativeParallelMultiHashMap<NetworkPipeline, Message>(1, allocator);
        }

        public void Dispose()
        {
            __driver.Dispose();
            __buffer.Dispose();
            __headers.Dispose();
            __messages.Dispose();
        }

        public Messages AsMessages()
        {
            return new Messages(this);
        }

        public void Shutdown()
        {
            __driver.Disconnect(connection);

            //__identities.Clear();
        }

        public void Connect(in NetworkEndpoint endPoint)
        {
            if (NetworkConnection.State.Disconnected != connectionState)
                __driver.Disconnect(connection);

            Header header;
            header.connection = __driver.Connect(endPoint);
            header.endpoint = endPoint;
            __headers[0] = header;
        }

        public NetworkPipeline CreatePipeline(in NativeArray<NetworkPipelineStageId> stages)
        {
            return __driver.CreatePipeline(stages);
        }

        public NetworkPipeline CreatePipeline(params Type[] stages)
        {
            return __driver.CreatePipeline(stages);
        }
        
        public void GetPipelines(ref NativeList<NetworkPipeline> pipelines)
        {
            NetworkPipeline pipeline;
            foreach (var message in __messages)
            {
                pipeline = message.Key;
                if (pipelines.IndexOf(pipeline) != -1)
                    continue;

                pipelines.Add(pipeline);
            }
        }

        public void GetMessages(in NetworkPipeline pipeline, ref NativeList<Message> messages)
        {
            foreach (var message in __messages.GetValuesForKey(pipeline))
                messages.Add(message);

            messages.Sort();
        }

        public JobHandle Schedule(
            ref NetworkClientSendBuffer sendBuffer, 
            in JobHandle inputDeps)
        {
            var jobHandle = inputDeps;
            
            bool bound = __driver.Bound;
            if (bound)
            {
                Send send;
                send.headers = __headers;
                send.driver = __driver.ToConcurrent();
                send.sendBuffer = sendBuffer;

                jobHandle = send.ScheduleByRef(jobHandle);
            }
            
            jobHandle = __driver.ScheduleUpdate(jobHandle);

            PopEvents popEvents;
            popEvents.driver = __driver;
            popEvents.sendBuffer = sendBuffer;
            popEvents.buffer = __buffer;
            popEvents.headers = __headers;
            popEvents.messages = __messages;

            jobHandle = popEvents.ScheduleByRef(jobHandle);
            
            if(!bound)
                jobHandle = __driver.ScheduleFlushSend(jobHandle);
            
            return jobHandle;
        }
    }

    public struct NetworkClientDriver : IComponentData
    {
        private NetworkClient __instance;
        private NetworkClientSendBuffer __sendBuffer;
        
        public NetworkClientDriver(in NetworkSettings settings, in AllocatorManager.AllocatorHandle allocator)
        {
            __instance = new NetworkClient(settings, allocator);
            __sendBuffer = new NetworkClientSendBuffer(allocator);
        }

        public NetworkClientDriver(
            AllocatorManager.AllocatorHandle allocator, 
            int connectTimeoutMS, 
            int maxConnectAttempts, 
            int disconnectTimeoutMS = 30 * 1000, 
            int heartbeatTimeoutMS = 500, 
            int reconnectionTimeoutMS = 2000, 
            int maxFrameTimeMS = 0, 
            int fixedFrameTimeMS = 0, 
            int receiveQueueCapacity = 4096, 
            int sendQueueCapacity = 4096)
        {
            var settings = new NetworkSettings(Allocator.Temp);

            settings.WithNetworkConfigParameters(
                connectTimeoutMS,
                maxConnectAttempts,
                disconnectTimeoutMS,
                heartbeatTimeoutMS,
                reconnectionTimeoutMS,
                maxFrameTimeMS,
                fixedFrameTimeMS,
                receiveQueueCapacity,
                sendQueueCapacity);
            
            __instance = new NetworkClient(settings, allocator);
            __sendBuffer = new NetworkClientSendBuffer(allocator);
            
            settings.Dispose();
        }

        public void Dispose()
        {
            __instance.Dispose();
            __sendBuffer.Dispose();
        }
        
        public NetworkClient.Messages AsMessages() => __instance.AsMessages();

        public void Connect(in NetworkEndpoint endPoint)
        {
            __instance.Connect(endPoint);
        }

        public bool Connect(string address, ushort port)
        {
            if (NetworkEndpoint.TryParse(address, port, out var endpoint))
            {
                Connect(endpoint);

                return true;
            }

            return false;
        }

        public int CreatePipeline(in NativeArray<NetworkPipelineStageId> stages)
        {
            var pipeline = __instance.CreatePipeline(stages);
            
            return __sendBuffer.CreatePipeline(pipeline);
        }
        
        public int CreatePipeline(in NativeArray<NetworkPipelineStage> stages)
        {
            using var stageIDs = stages.ToPipelineStageIDs(Allocator.Temp);
            var pipeline = __instance.CreatePipeline(stageIDs);
            
            return __sendBuffer.CreatePipeline(pipeline);
        }

        public int CreatePipeline(int pipelineIndex)
        {
            var pipeline = __sendBuffer.GetPipeline(pipelineIndex);
            
            return __sendBuffer.CreatePipeline(pipeline);
        }

        public bool BeginWrite(int pipelineIndex, out DataStreamWriter writer, short capacity = 1024)
        {
            return __sendBuffer.BeginWrite(pipelineIndex, out writer, capacity);
        }

        public void EndWrite(in DataStreamWriter writer)
        {
            __sendBuffer.EndWrite(writer);
        }

        public JobHandle Schedule(in JobHandle inputDeps)
        {
            return __instance.Schedule(ref __sendBuffer, inputDeps);
        }
    }
}