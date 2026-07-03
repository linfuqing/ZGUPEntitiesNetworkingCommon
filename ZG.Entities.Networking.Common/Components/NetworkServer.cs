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
    public interface INetworkServerListener
    {
        void Connect(
            uint id, 
            in NativeArray<byte> payload, 
            ref NetworkServerSendBuffer sendBuffer);

        void Disconnect(uint id, ref NetworkServerSendBuffer sendBuffer);
        
        void Reconnect(uint id, ref NetworkServerSendBuffer sendBuffer);
    }
    
    public interface INetworkServerHandler
    {
        void Connect(ref NetworkServerSendBuffer.ParallelIdentity sendBuffer);

        void Disconnect(ref NetworkServerSendBuffer.ParallelIdentity sendBuffer);

        void Read(ref DataStreamReader reader,
            ref NetworkServerSendBuffer.ParallelIdentity sendBuffer);
    }

    public interface INetworkServerScheduler
    {
        JobHandle Schedule(in JobHandle dependsOn);
    }

    [BurstCompile]
    public struct NetworkServerInitJob<T> : IJob where T : unmanaged, INetworkServerListener
    {
        public T listener;
        public MultiNetworkDriver driver;

        public NetworkServerSendBuffer sendBuffer;

        public NativeList<NetworkConnection> connectionsToConnect;
        public NativeList<NetworkConnection> connectionsToDisconnect;

        public void Execute()
        {
            sendBuffer.Clear();

            foreach (var connectionToDisconnect in connectionsToDisconnect)
                __Disconnect(false, connectionToDisconnect);
                
            connectionsToDisconnect.Clear();
                
            connectionsToConnect.Clear();

            //int connectionIndex, channelIndex;
            uint id;
            NetworkConnection connection, temp;
            while ((connection = driver.Accept(out var payload)) != default)
            {
                temp = connection;
                id = sendBuffer.Connect(ref temp, payload);
                if (id == 0)
                {
                    if (NetworkConnection.State.Connected == driver.GetConnectionState(temp))
                    {
                        /*driver.Disconnect(connection);
                        
                        continue;*/
                        
                        driver.Disconnect(temp);
                    }
                    
                    __Disconnect(true, temp);
                    
                    temp = connection;
                    id = sendBuffer.Connect(ref temp, payload);
                }

                if (id == 0)
                {
                    if(temp == connection)
                        driver.Disconnect(connection);
                    
                    continue;
                }

                connectionsToConnect.Add(connection);

                listener.Connect(id, payload, ref sendBuffer);
            }

            connectionsToDisconnect.Capacity = math.max(connectionsToDisconnect.Capacity, sendBuffer.connections.Length);
        }
        
        private void __Disconnect(bool isConnected, in NetworkConnection connection)
        {
            uint id = sendBuffer.connectionIDs[connection];
            
            if(isConnected)
                listener.Reconnect(id, ref sendBuffer);
            else
                listener.Disconnect(id, ref sendBuffer);
            
            sendBuffer.Disconnect(connection);
        }
    }

    [BurstCompile]
    public struct NetworkServerPopEventsJob<T> : IJobParallelForDefer where T : unmanaged, INetworkServerHandler
    {
        public T handler;
        public MultiNetworkDriver.Concurrent driver;
        public NetworkServerSendBuffer.ParallelWriter sendBuffer;

        public NativeList<NetworkConnection>.ParallelWriter connectionsToDisconnect;

        [ReadOnly]
        public NativeArray<NetworkConnection> connectionsToConnect;

        [ReadOnly]
        public NativeArray<NetworkConnection> connections;

        [ReadOnly]
        public NativeHashMap<NetworkConnection, uint> connectionIDs;

        public void Execute(int index)
        {
            var connection = connections[index];
            
            var sendBuffer = new NetworkServerSendBuffer.ParallelIdentity(connectionIDs[connection], ref this.sendBuffer);

            if(connectionsToConnect.IndexOf(connection) != -1)
                handler.Connect(ref sendBuffer);
            
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
                                
                                handler.Read(ref stream, ref sendBuffer);
                            }
                        } while (reader.GetBytesRead() < reader.Length);

                        break;
                    case NetworkEvent.Type.Connect:
                        handler.Connect(ref sendBuffer);
                        
                        break;
                    case NetworkEvent.Type.Disconnect:

                        var disconnectReason = (DisconnectReason)reader.ReadByte();
                        __LogDisconnectReason(connection, disconnectReason);

                        handler.Disconnect(ref sendBuffer);

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

    public struct NetworkServer
    {
        [BurstCompile]
        private struct Send : IJobParallelForDefer
        {
            public NetworkPipeline pipeline;

            [ReadOnly]
            public NativeHashMap<uint, NetworkPipeline> pipelines;

            [ReadOnly]
            public NativeArray<NetworkConnection> connections;
            
            public MultiNetworkDriver.Concurrent driver;

            public NetworkServerSendBuffer.Sender sender;

            public void Execute(int index)
            {
                sender.Send(connections[index], pipeline, pipelines, ref driver);
            }
        }

        private NetworkDriver __udpDriver;
        private NetworkDriver __wsDriver;
        private MultiNetworkDriver __driver;
        private NativeList<NetworkConnection> __connectionsToConnect;
        private NativeList<NetworkConnection> __connectionsToDisconnect;

        public bool isCreated => __driver.IsCreated;

        public NetworkServer(in NetworkSettings settings, in AllocatorManager.AllocatorHandle allocator)
        {
            __udpDriver = NetworkDriver.Create(settings);
            __wsDriver = NetworkDriver.Create(new WebSocketNetworkInterface(), settings);
            __driver = MultiNetworkDriver.Create();
            __driver.AddDriver(__udpDriver);
            __driver.AddDriver(__wsDriver);

            __connectionsToConnect = new NativeList<NetworkConnection>(allocator);
            __connectionsToDisconnect = new NativeList<NetworkConnection>(allocator);
        }

        public void Dispose()
        {
            __driver.Dispose();
            __connectionsToConnect.Dispose();
            __connectionsToDisconnect.Dispose();
        }

        public NetworkPipeline CreatePipeline(in NativeArray<NetworkPipelineStageId> stages)
        {
            return __driver.CreatePipeline(stages);
        }

        public int AddDriver(ref NetworkDriver driver)
        {
            return __driver.AddDriver(driver);
        }

        public void Listen(ushort udpPort, ushort wsPort, NetworkFamily family = NetworkFamily.Ipv4)
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

            if (__udpDriver.Bind(endpoint.WithPort(udpPort)) != 0 || __udpDriver.Listen() != 0)
                UnityEngine.Debug.LogError($"Failed to bind to port {udpPort}");
            
            if (__wsDriver.Bind(endpoint.WithPort(wsPort)) != 0 || __wsDriver.Listen() != 0)
                UnityEngine.Debug.LogError($"Failed to bind to port {wsPort}");
        }

        public void Disconnect(in NetworkConnection connection)
        {
            __driver.Disconnect(connection);
        }

        public JobHandle Schedule<TListener, THandler, TScheduler>(
            ref TListener listener, 
            ref THandler handler, 
            ref TScheduler scheduler,
            ref NetworkServerSendBuffer sendBuffer,
            in NativeHashMap<uint, NetworkPipeline> pipelines, 
            in NetworkPipeline pipeline, 
            in JobHandle inputDeps, 
            int innerloopBatchCount = 4) 
            where TListener : unmanaged, INetworkServerListener
            where THandler : unmanaged, INetworkServerHandler
            where TScheduler : unmanaged, INetworkServerScheduler
        {
            var driver = __driver.ToConcurrent();
            
            var jobHandle = __driver.ScheduleUpdate(inputDeps);

            var sendBufferParallelWriter = sendBuffer.AsParallelWriter();

            NetworkServerInitJob<TListener> init;
            init.listener = listener;
            init.driver = __driver;
            init.sendBuffer = sendBuffer;
            init.connectionsToConnect = __connectionsToConnect;
            init.connectionsToDisconnect = __connectionsToDisconnect;
            jobHandle = init.ScheduleByRef(jobHandle);

            var connectionList = sendBuffer.connections;
            var connections = connectionList.AsDeferredJobArray();
            
            NetworkServerPopEventsJob<THandler> popEvents;
            popEvents.handler = handler;
            popEvents.driver = driver;
            popEvents.sendBuffer = sendBufferParallelWriter;
            popEvents.connectionsToConnect = __connectionsToConnect.AsDeferredJobArray();
            popEvents.connectionsToDisconnect = __connectionsToDisconnect.AsParallelWriter();
            popEvents.connections = connections;
            popEvents.connectionIDs = sendBuffer.connectionIDs;

            jobHandle = popEvents.ScheduleByRef(connectionList, innerloopBatchCount, jobHandle);

            jobHandle = scheduler.Schedule(jobHandle);
            
            Send send;
            send.pipeline = pipeline;
            send.pipelines = pipelines;
            send.connections = connections;
            send.driver = driver;
            send.sender = sendBuffer.AsSender();
            jobHandle = send.ScheduleByRef(connectionList, innerloopBatchCount, jobHandle);

            jobHandle = __driver.ScheduleFlushSend(jobHandle);
            return jobHandle;
        }
    }
}
