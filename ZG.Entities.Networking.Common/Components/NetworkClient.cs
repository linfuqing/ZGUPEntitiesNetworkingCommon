using System;
using Unity.Jobs;
using Unity.Burst;
using Unity.Entities;
using Unity.Collections;
using Unity.Mathematics;
using Unity.Networking.Transport;
using Unity.Networking.Transport.Error;

namespace ZG
{

    public enum NetworkClientType
    {
        Data, 
        Connect, 
        Disconnect
    }

    public struct NetworkClientSendBuffer : IComponentData
    {
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
    
    public struct NetworkClient : IComponentData
    {
        private struct Header
        {
            public NetworkConnection connection;
            public NetworkEndpoint endpoint;
        }

        public struct Message : IComparable<Message>
        {
            public NetworkClientType type;
            public int offset;
            public int size;

            public int CompareTo(Message other)
            {
                return other.offset.CompareTo(other.offset);
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
                    message.offset = buffer.Length;
                    
                    cmd = driver.PopEventForConnection(header.connection, out stream, out pipeline);
                    switch (cmd)
                    {
                        case NetworkEvent.Type.Empty:
                            isEmpty = true;
                            break;
                        case NetworkEvent.Type.Data:
                            message.type = NetworkClientType.Data;
                            
                            do
                            {
                                message.size = stream.ReadUShort();
                                buffer.ResizeUninitialized(message.offset + message.size);
                                stream.ReadBytes(buffer.AsArray().GetSubArray(message.offset, message.size));

                                messages.Add(pipeline, message);
                            } while (stream.GetBytesRead() < stream.Length);

                            break;
                        case NetworkEvent.Type.Connect:
                            isConnected = true;

                            message.type = NetworkClientType.Connect;
                            message.size = 0;

                            messages.Add(pipeline, message);
                            break;
                        case NetworkEvent.Type.Disconnect:
                            isConnected = false;

                            __LogDisconnectReason((DisconnectReason)stream.ReadByte());

                            header.connection = driver.Connect(header.endpoint);

                            var connections = headers.Reinterpret<NetworkConnection>();
                            connections[0] = header.connection;
                            
                            message.type = NetworkClientType.Disconnect;
                            message.size = 0;

                            messages.Add(pipeline, message);
                            break;
                    }
                } while (isEmpty);

                if (isConnected)
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

        private NetworkDriver __driver;
        private NativeArray<Header> __headers;
        private NativeList<byte> __buffer;
        private NativeParallelMultiHashMap<NetworkPipeline, Message> __messages;

        public NetworkConnection.State connectionState => __driver.GetConnectionState(connection);

        public NetworkConnection connection => __headers.Reinterpret<NetworkConnection>()[0];

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
}