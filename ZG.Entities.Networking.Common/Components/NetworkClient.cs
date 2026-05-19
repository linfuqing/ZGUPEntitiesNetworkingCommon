using System;
using Unity.Jobs;
using Unity.Burst;
using Unity.Entities;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Jobs.LowLevel.Unsafe;
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
        private struct Buffer
        {
            public int index;
            public NetworkSendBuffer value;

            public void Clear()
            {
                index = 0;
                value.Clear();
            }
        }

        public struct ParallelWriter
        {
            private NativeParallelHashSet<int>.ParallelWriter __bufferIndices;

            [NativeDisableParallelForRestriction]
            private NativeArray<Buffer> __buffers;

            [NativeSetThreadIndex]
            internal int _threadIndex;

            public ParallelWriter(ref NetworkClientSendBuffer buffer)
            {
                __bufferIndices = buffer.__bufferIndices.AsParallelWriter();

                __buffers = buffer.__buffers.AsDeferredJobArray();

                _threadIndex = 0;
            }
            
            public bool BeginWrite(int pipelineIndex, out DataStreamWriter writer, ushort capacity = 1024)
            {
                int bufferIndex = pipelineIndex * JobsUtility.MaxJobThreadCount + _threadIndex;
                var buffer = __buffers[bufferIndex];
                
                bool result = buffer.value.BeginWrite(out writer, capacity);
                if (result)
                {
                    writer.m_SendHandleData = (IntPtr)bufferIndex;

                    __buffers[bufferIndex] = buffer;
                }

                return result;
            }

            public void EndWrite(in DataStreamWriter writer)
            {
                int bufferIndex = (int)writer.m_SendHandleData;
                var buffer = __buffers[bufferIndex];
                buffer.value.EndWrite(writer);
                __buffers[bufferIndex] = buffer;

                __bufferIndices.Add(bufferIndex);
            }

        }

        [ReadOnly]
        private NativeList<NetworkPipeline> __pipelines;
        private NativeList<Buffer> __buffers;
        private NativeParallelHashSet<int> __bufferIndices;
        
        public bool isCreated => __buffers.IsCreated;
        
        public unsafe AllocatorManager.AllocatorHandle allocator => __pipelines.GetUnsafeList()->Allocator;

        public NetworkClientSendBuffer(in AllocatorManager.AllocatorHandle allocator)
        {
            __pipelines = new NativeList<NetworkPipeline>(allocator);

            __buffers = new NativeList<Buffer>(allocator);

            __bufferIndices = new NativeParallelHashSet<int>(1, allocator);
        }

        public void Dispose()
        {
            __pipelines.Dispose();

            foreach (var buffer in __buffers)
                buffer.value.Dispose();
            
            __buffers.Dispose();
            __bufferIndices.Dispose();
        }

        public void Clear()
        {
            Buffer buffer;
            int length = math.min(__pipelines.Length, __buffers.Length);
            for (int i = 0; i < length; ++i)
            {
                buffer = __buffers[i];
                buffer.Clear();

                __buffers[i] = buffer;
            }
            __bufferIndices.Clear();
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

            Buffer buffer;
            buffer.index = 0;
            for (int i = 0; i < JobsUtility.MaxJobThreadCount; ++i)
            {
                buffer.value = new NetworkSendBuffer(allocator);
                __buffers.Add(buffer);
            }
            
            __bufferIndices.Capacity = math.max(__bufferIndices.Capacity, __pipelines.Length * JobsUtility.MaxJobThreadCount);
            return result;
        }

        public bool BeginWrite(int pipelineIndex, out DataStreamWriter writer, ushort capacity = 1024)
        {
            int bufferIndex = pipelineIndex * JobsUtility.MaxJobThreadCount;
            
            bool result = __buffers.ElementAt(bufferIndex).value.BeginWrite(out writer, capacity);

            if(result)
                writer.m_SendHandleData = (IntPtr)bufferIndex;

            return result;
        }

        public void EndWrite(in DataStreamWriter writer)
        {
            int bufferIndex = (int)writer.m_SendHandleData;
            __buffers.ElementAt(bufferIndex).value.EndWrite(writer);
            
            __bufferIndices.Add(bufferIndex);
        }

        public void Apply(in NetworkConnection connection, ref NetworkDriver.Concurrent driver)
        {
            foreach (var bufferIndex in __bufferIndices)
            {
                ref var buffer = ref __buffers.ElementAt(bufferIndex);
                buffer.value.Apply(connection, __pipelines[bufferIndex / JobsUtility.MaxJobThreadCount], ref driver, ref buffer.index);
            }
            
            __bufferIndices.Clear();
        }
    }
    
    public struct NetworkClient
    {
        private struct Header
        {
            public NetworkConnection connection;
            public NetworkEndpoint endpoint;
            public double disconnectionTime;
        }

        public struct Message : IComparable<Message>
        {
            public NetworkClientMessageType type;
            public int offset;
            public int size;

            public NativeArray<byte> AsArray(in NativeArray<byte> buffer)
            {
                return buffer.GetSubArray(offset, size);
            }

            public DataStreamReader Read(in NativeArray<byte> buffer)
            {
                return new DataStreamReader(AsArray(buffer));
            }

            public int CompareTo(Message other)
            {
                int result = offset.CompareTo(other.offset);
                if(0 == result)
                    return ((int)type).CompareTo((int)other.type);

                return result;
            }
        }

        public struct MessageElement : IComparable<MessageElement>
        {
            public readonly Message Message;
                
            private NativeArray<byte> __buffer;
            
            public DataStreamReader reader => Message.Read(__buffer);

            public MessageElement(in Message message, in NativeArray<byte> buffer)
            {
                Message = message;
                __buffer = buffer;
            }

            public MessageElement(in Message message, in Messages messages)
            {
                Message = message;
                __buffer = messages._buffer.AsArray();
            }

            public NativeArray<byte> AsArray()
            {
                return Message.AsArray(__buffer);
            }
            
            public int CompareTo(MessageElement other)
            {
                return Message.offset.CompareTo(other.Message.offset);
            }
        }

        public struct MessageEnumerator
        {
            private NativeList<byte> __buffer;
            private NativeParallelMultiHashMap<NetworkPipeline, Message>.KeyValueEnumerator __enumerator;

            public MessageElement Current => new MessageElement(__enumerator.Current.Value, __buffer.AsArray());

            public MessageEnumerator(in Messages messages)
            {
                __buffer = messages._buffer;
                __enumerator = messages._values.GetEnumerator();
            }

            public bool MoveNext() => __enumerator.MoveNext();
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

            public MessageEnumerator GetEnumerator()
            {
                return new MessageEnumerator(in this);
            }
        }

        [BurstCompile]
        private struct Send : IJob
        {
            [ReadOnly]
            public NativeArray<byte> headers;
            public NetworkDriver.Concurrent driver;
            public NetworkClientSendBuffer sendBuffer;

            public void Execute()
            {
                var connection = headers.GetSubArray(0, UnsafeUtility.SizeOf<NetworkConnection>()).Reinterpret<NetworkConnection>(1)[0];
                if (NetworkConnection.State.Connected != driver.GetConnectionState(connection))
                    return;
                
                sendBuffer.Apply(connection, ref driver);
            }
        }

#if !DEBUG
        [BurstCompile]
#endif
        private struct PopEvents : IJob
        {
            public float reconnectionTime;
            public double time;
            public NetworkDriver driver;
            public NetworkClientSendBuffer sendBuffer;
            public NativeList<byte> buffer;
            public NativeArray<byte> headers;
            public NativeParallelMultiHashMap<NetworkPipeline, Message> messages;

            public void Execute()
            {
                int headerSize = UnsafeUtility.SizeOf<Header>();
                var headers = this.headers.Length < headerSize
                    ? default
                    : this.headers.GetSubArray(0, headerSize).Reinterpret<Header>(1);
                var header = headers.IsCreated ? headers[0] : default;
                if (header.disconnectionTime > math.DBL_MIN_NORMAL)
                {
                    if (time - header.disconnectionTime > reconnectionTime)
                    {
                        switch (driver.GetConnectionState(header.connection))
                        {
                            case NetworkConnection.State.Disconnecting:
                                return;
                            case NetworkConnection.State.Disconnected:
                                header.connection = driver.Connect(header.endpoint,
                                    this.headers.GetSubArray(headerSize, this.headers.Length - headerSize));

                                break;
                        }

                        header.disconnectionTime = 0.0;

                        headers[0] = header;
                    }

                    return;
                }

                buffer.Clear();

                messages.Clear();

                bool isEmpty = false;
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
                            /*int headersLength = headers.Length - headerSize;
                            if (headersLength > 0 && driver.BeginSend(header.connection, out var writer) >= 0)
                            {
                                writer.WriteUShort((ushort)headersLength);
                                writer.WriteBytes(headers.GetSubArray(headerSize, headersLength));

                                driver.EndSend(writer);
                            }*/

                            message.type = NetworkClientMessageType.Connect;
                            message.offset = buffer.Length;
                            message.size = 0;

                            messages.Add(pipeline, message);
                            break;
                        case NetworkEvent.Type.Disconnect:
                            var disconnectReason = (DisconnectReason)stream.ReadByte();
                            __LogDisconnectReason(disconnectReason);

                            header.disconnectionTime = time;
                            headers[0] = header;

                            /*driver.Disconnect(header.connection);
                            
                            header.connection = driver.Connect(header.endpoint, headers.GetSubArray(headerSize, headers.Length - headerSize));

                            var connections = headers.GetSubArray(0, UnsafeUtility.SizeOf<NetworkConnection>())
                                .Reinterpret<NetworkConnection>(1);
                            connections[0] = header.connection;*/
                    
                            message.type = NetworkClientMessageType.Disconnect;
                            message.offset = buffer.Length;
                            message.size = 0;

                            messages.Add(pipeline, message);
                            break;
                    }
                } while (!isEmpty);

                if (NetworkConnection.State.Connected == driver.GetConnectionState(header.connection))
                {
                    var driver = this.driver.ToConcurrent();
                    
                    sendBuffer.Apply(header.connection, ref driver);
                }
            }

            private void __LogDisconnectReason(DisconnectReason disconnectReason)
            {
                UnityEngine.Debug.LogError($"DisconnectReason: {(int)disconnectReason}");
            }
        }

        public readonly float ReconnectionTime;

        private NetworkDriver __driver;
        private NativeList<byte> __headers;
        private NativeList<byte> __buffer;
        private NativeParallelMultiHashMap<NetworkPipeline, Message> __messages;

        public bool isCreated => __driver.IsCreated;

        public NetworkConnection.State connectionState => __driver.GetConnectionState(connection);

        public NetworkConnection connection
        {
            get
            {
                int size = UnsafeUtility.SizeOf<NetworkConnection>();
                return size > __headers.Length ? default : __headers.AsArray()
                    .GetSubArray(0, size).Reinterpret<NetworkConnection>(1)[0];
            }
        }
        
        public NativeList<byte> buffer => __buffer;

        public NetworkClient(NetworkSettings settings, in AllocatorManager.AllocatorHandle allocator)
        {
            ReconnectionTime = settings.GetNetworkConfigParameters().reconnectionTimeoutMS * 0.001f;
            
/*#if UNITY_WEBGL && !UNITY_EDITOR
            __driver = NetworkDriver.Create(new WebSocketNetworkInterface(), settings);
#else
            __driver = NetworkDriver.Create(new UDPNetworkInterface(), settings);
#endif*/
            __driver = NetworkDriver.Create(settings);
            
            __headers = new NativeList<byte>(allocator);
            __buffer = new NativeList<byte>(allocator);
            __messages = new NativeParallelMultiHashMap<NetworkPipeline, Message>(1, allocator);
        }

        public void Dispose()
        {
            __driver.Dispose();
            __headers.Dispose();
            __buffer.Dispose();
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

        public void Connect(in NetworkEndpoint endPoint, in NativeArray<byte> payload)
        {
            if (NetworkConnection.State.Disconnected != connectionState)
                __driver.Disconnect(connection);

            int headerSize = UnsafeUtility.SizeOf<Header>(), headersSize = payload.IsCreated ? payload.Length : 0;
            __headers.ResizeUninitialized(headerSize + headersSize);
            var headersArray = __headers.AsArray();
            var temp = headersArray.GetSubArray(0, headerSize).Reinterpret<Header>(1);
            
            Header header;
            header.connection = __driver.Connect(endPoint, payload);
            header.endpoint = endPoint;
            header.disconnectionTime = 0.0;
            temp[0] = header;
            
            if(headersSize > 0)
                NativeArray<byte>.Copy(payload, 0, headersArray, headerSize, headersSize);
        }
        
        public bool Connect(in FixedString128Bytes address, ushort port, in NativeArray<byte> headers)
        {
            if (NetworkEndpoint.TryParse(address, port, out var endpoint))
            {
                Connect(endpoint, headers);

                return true;
            }

            return false;
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
            double time, 
            ref NetworkClientSendBuffer sendBuffer, 
            in JobHandle inputDeps)
        {
            var jobHandle = inputDeps;

            var headers = __headers.AsArray();
            
            bool bound = __driver.Bound;
            if (bound)
            {
                Send send;
                send.headers = headers;
                send.driver = __driver.ToConcurrent();
                send.sendBuffer = sendBuffer;

                jobHandle = send.ScheduleByRef(jobHandle);
            }
            
            jobHandle = __driver.ScheduleUpdate(jobHandle);

            PopEvents popEvents;
            popEvents.reconnectionTime = ReconnectionTime;
            popEvents.time = time;
            popEvents.driver = __driver;
            popEvents.sendBuffer = sendBuffer;
            popEvents.buffer = __buffer;
            popEvents.headers = headers;
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
        
        public NetworkClient instance => __instance;
        
        public NetworkClientSendBuffer sendBuffer => __sendBuffer;

        public bool isCreated => __instance.isCreated;

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

        public JobHandle Schedule(double time, in JobHandle inputDeps)
        {
            return __instance.Schedule(time, ref __sendBuffer, inputDeps);
        }
    }
}