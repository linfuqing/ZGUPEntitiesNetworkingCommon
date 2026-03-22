using System;
using System.Threading;
using Unity.Jobs;
using Unity.Entities;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Networking.Transport;
using Unity.Networking.Transport.Error;
using ZG;
using Unity.Burst;

[assembly:RegisterGenericJobType(typeof(NetworkServerInitJob<NetworkRelayServerListener>))]
[assembly:RegisterGenericJobType(typeof(NetworkServerPopEventsJob<NetworkRelayServerHandler>))]

namespace ZG
{
    public struct NetworkRelayServerIdentity
    {
        public const int CHANNEL_NULL = -1;

        public readonly uint ID;

        //private UnsafeList<byte> __bytes;

        public bool isOnline
        {
            get => channelFlag.HasFlag(NetworkRelayChannelFlag.Online);

            set
            {
                if (value)
                    channelFlag |= NetworkRelayChannelFlag.Online;
                else
                    channelFlag &= ~NetworkRelayChannelFlag.Online;
            }
        }

        public int channel
        {
            get;

            private set;
        }

        public NetworkRelayChannelFlag channelFlag
        {
            get;

            private set;
        }

        public static void SendRelay(
            int type,
            int relayType,
            uint id,
            ref DataStreamReader reader,
            ref DataStreamWriter writer)
        {
            var streamCompressionModel = StreamCompressionModel.Default;
            writer.WritePackedInt(type, streamCompressionModel);
            writer.WritePackedInt(relayType, streamCompressionModel);
            writer.WritePackedUInt(id, streamCompressionModel);
            writer.Write(ref reader);
        }

        public NetworkRelayServerIdentity(uint id, in AllocatorManager.AllocatorHandle allocator)
        {
            ID = id;
            
            channel = CHANNEL_NULL;

            channelFlag = 0;
            //__bytes = new UnsafeList<byte>(1, allocator);
        }

        public void Dispose()
        {
            //__bytes.Dispose();
        }

        public void Clear()
        {
            channel = CHANNEL_NULL;
            channelFlag = 0;
            //__bytes.Clear();
        }

        /*public void Init(ref DataStreamReader reader)
        {
            __bytes.Resize(reader.Length - reader.GetBytesRead(), NativeArrayOptions.UninitializedMemory);
            reader.ReadBytes(AsArray());
        }*/

        public void SendHeader(
            int type,
            ref NetworkServerSendBuffer.Identity sendBuffer)
        {
            if (sendBuffer.BeginWrite(ID, out var writer))
            {
                __WriteHeader(sendBuffer.ID != ID, type, sendBuffer.GetPayload(ID), ref writer);

                sendBuffer.EndWrite(writer);
            }
        }

        public void SendHeader(
            int channel, 
            int type,
            ref NetworkServerSendBuffer.Identity sendBuffer)
        {
            if (sendBuffer.BeginWrite(channel, out var writer))
            {
                __WriteHeader(sendBuffer.ID != ID, type, sendBuffer.GetPayload(ID), ref writer);

                sendBuffer.EndWrite(writer);
            }
        }
        
        public void Create(
            int channel,
            ref NetworkServerSendBuffer.Identity sendBuffer)
        {
            if(__CreateOrJoin((int)NetworkRelayMessageType.Create, 
                channel, 
                ref sendBuffer))
                channelFlag |= NetworkRelayChannelFlag.Creator;
        }
        
        public void Join(
            int channel,
            ref NetworkServerSendBuffer.Identity sendBuffer)
        {
            __CreateOrJoin((int)NetworkRelayMessageType.Join, 
                channel, 
                ref sendBuffer);
        }

        public void Leave(ref NetworkServerSendBuffer.Identity sendBuffer)
        {
            __DropOrLeave((int)NetworkRelayMessageType.Leave, ref sendBuffer);
        }

        public void Drop(
            ref NetworkServerSendBuffer.Identity sendBuffer)
        {
            __DropOrLeave((int)NetworkRelayMessageType.Drop, ref sendBuffer);
        }
        
        public void Relay(
            int type,
            NetworkRelayType relayType,
            ref DataStreamReader reader,
            ref NetworkServerSendBuffer.Identity sendBuffer)
        {
            DataStreamWriter writer;
            switch(relayType)
            {
                case NetworkRelayType.All:
                    if (!sendBuffer.BeginWrite(out writer))
                        return;
                    break;
                case NetworkRelayType.Channel:
                    if (!sendBuffer.BeginWrite(channel, out writer))
                        return;
                    break;
                case NetworkRelayType.Identity:
                    if (!sendBuffer.BeginWrite(relayType.RelayID(), out writer))
                        return;
                    break;
                default:
                    return;
            }

            SendRelay(type, (int)relayType, sendBuffer.ID, ref reader, ref writer);

            sendBuffer.EndWrite(writer);
        }

        private void __WriteHeader(bool isSendOthers, int type, in NativeArray<byte> payload, ref DataStreamWriter writer)
        {
            var streamCompressionModel = StreamCompressionModel.Default;
            writer.WritePackedInt(type, streamCompressionModel);
            //writer.WritePackedInt(identityIndex, streamCompressionModel);
            writer.WritePackedInt(channel, streamCompressionModel);

            if (isSendOthers)
            {
                writer.WritePackedInt((int)channelFlag, streamCompressionModel);
                
                writer.WriteBytes(payload);
            }
        }
        
        private bool __CreateOrJoin(
            int type, 
            int channel,
            ref NetworkServerSendBuffer.Identity sendBuffer)
        {
            if (!sendBuffer.AddChannel(channel))
                return false;

            Leave(ref sendBuffer);

            this.channel = channel;

            SendHeader(type, ref sendBuffer);
            SendHeader(channel, type, ref sendBuffer);

            return true;
        }

        private void __DropOrLeave(
            int type, 
            ref NetworkServerSendBuffer.Identity sendBuffer)
        {
            if (channel == CHANNEL_NULL)
                return;
            
            if (sendBuffer.RemoveChannel(channel))
            {
                SendHeader(type, ref sendBuffer);
                SendHeader(channel, type, ref sendBuffer);
            }

            channelFlag = 0;
            channel = CHANNEL_NULL;
        }
    }

    public struct NetworkRelayServerListener : INetworkServerListener
    {
        public NativeList<NetworkRelayServerIdentity> identities;
        public NativeParallelHashMap<uint, int> idChannels;

        public void Connect(in NetworkConnection connection, uint id, int connectionIndex, int channelIndex, NativeArray<byte> payload)
        {
            if (channelIndex < identities.Length)
                UnityEngine.Assertions.Assert.AreEqual(id, identities[channelIndex].ID);
            else
            {
                UnityEngine.Assertions.Assert.AreEqual(channelIndex, identities.Length);

                identities.Add(new NetworkRelayServerIdentity(id, Allocator.Persistent));

                idChannels.Capacity = Unity.Mathematics.math.max(idChannels.Capacity, identities.Length);
            }
        }

        public void Disconnect(in NetworkConnection connection, uint id)
        {
            //identityIndices.Remove(connection);
        }
    }

    public struct NetworkRelayServerHandler : INetworkServerHandler
    {
        [NativeDisableParallelForRestriction] 
        public NativeArray<NetworkRelayServerIdentity> identities;
        public NativeParallelHashMap<uint, int>.ParallelWriter idChannels;

        public void Connect(ref NetworkServerSendBuffer.Identity sendBuffer)
        {
            var identityIndex = sendBuffer.channelIndex;
            var identity = identities[identityIndex];
            identity.isOnline = true;
            identities[identityIndex] = identity;

            int channel = identity.channel;
            if (channel != NetworkRelayServerIdentity.CHANNEL_NULL)
            {
                if (sendBuffer.BeginWrite(channel, out var writer))
                {
                    var streamCompressionModel = StreamCompressionModel.Default;
                    writer.WritePackedInt((int)NetworkRelayMessageType.Connect, streamCompressionModel);
                    writer.WritePackedUInt(sendBuffer.ID, streamCompressionModel);
                        
                    sendBuffer.EndWrite(writer);
                }
                
                identity.SendHeader((identity.channelFlag & NetworkRelayChannelFlag.Creator) ==
                    NetworkRelayChannelFlag.Creator
                        ? (int)NetworkRelayMessageType.Create
                        : (int)NetworkRelayMessageType.Join,
                    ref sendBuffer);

                __SendChannelJoins(identityIndex, identity.channel, ref sendBuffer);
            }
        }

        public void Disconnect(ref NetworkServerSendBuffer.Identity sendBuffer)
        {
            var identityIndex = sendBuffer.channelIndex;
            var identity = identities[identityIndex];
            int channel = identity.channel;
            if (channel != 0)
            {
                if (sendBuffer.BeginWrite(channel, out var writer))
                {
                    var streamCompressionModel = StreamCompressionModel.Default;
                    writer.WritePackedInt((int)NetworkRelayMessageType.Disconnect, streamCompressionModel);
                    writer.WritePackedUInt(sendBuffer.ID, streamCompressionModel);

                    sendBuffer.EndWrite(writer);
                }
            }

            identity.isOnline = false;
            identities[identityIndex] = identity;
        }

        public void Read(ref DataStreamReader reader,
            ref NetworkServerSendBuffer.Identity sendBuffer)
        {
            var identityIndex = sendBuffer.channelIndex;
            var identity = identities[identityIndex];

            NetworkRelayServerIdentity channelIdentity;
            DataStreamWriter writer;
            var streamCompressionModel = StreamCompressionModel.Default;
            int type = reader.ReadPackedInt(streamCompressionModel), channel, numIdentities;
            switch ((NetworkRelayMessageType)type)
            {
                case NetworkRelayMessageType.Init:
                    if (sendBuffer.BeginWrite(sendBuffer.ID, out writer))
                    {
                        writer.WritePackedInt(type, streamCompressionModel);
                        sendBuffer.EndWrite(writer);
                    }

                    break;
                case NetworkRelayMessageType.Create:
                    identity.Create(
                        sendBuffer.channelIndex,
                        ref sendBuffer);

                    identities[identityIndex] = identity;

                    __SendChannelJoins(identityIndex, identity.channel, ref sendBuffer);
                    break;
                case NetworkRelayMessageType.Join:
                    channel = reader.ReadPackedInt(streamCompressionModel);
                    identity.Join(
                        channel, ref sendBuffer);

                    identities[identityIndex] = identity;

                    __SendChannelJoins(identityIndex, identity.channel, ref sendBuffer);
                    break;
                case NetworkRelayMessageType.Leave:
                    identity.Leave(
                        ref sendBuffer);

                    identities[identityIndex] = identity;
                    break;
                case NetworkRelayMessageType.Drop:
                    var id = reader.ReadPackedUInt(streamCompressionModel);
                    identityIndex = sendBuffer.GetChannelIndex(id);
                    if (identityIndex != -1)
                        idChannels.TryAdd(id, identities[identityIndex].channel);
                    /*if (identities[identityIndices[id]].channel == identity.channel && 
                        sendBuffer.BeginWrite(pipelineIndexCustom, out writer))
                    {
                        writer.WritePackedInt(type, streamCompressionModel);
                        writer.WritePackedUInt(id, streamCompressionModel);
                        sendBuffer.EndWrite(writer);
                    }*/
                    break;
                case NetworkRelayMessageType.Query:
                    channel = reader.ReadPackedInt(streamCompressionModel);
                    numIdentities = identities.Length;
                    for(int i = 0; i < numIdentities; ++i)
                    {
                        channelIdentity = identities[i];
                        if (channelIdentity.channel != channel)
                            continue;

                        channelIdentity.SendHeader((int)NetworkRelayMessageType.Query,
                            ref sendBuffer);
                    }

                    break;
                default:
                    NetworkRelayType relayType = (NetworkRelayType)reader.ReadPackedInt(streamCompressionModel);

                    identity.Relay(type, relayType, ref reader,
                        ref sendBuffer);

                    break;
            }
        }

        private void __SendChannelJoins(int identityIndex, int channel, ref NetworkServerSendBuffer.Identity sendBuffer)
        {
            int numIdentities = identities.Length;
            NetworkRelayServerIdentity channelIdentity;
            for (int i = 0; i < numIdentities; ++i)
            {
                if (i == identityIndex)
                    continue;

                channelIdentity = identities[i];
                if (channelIdentity.channel != channel)
                    continue;

                channelIdentity.SendHeader(
                    (int)NetworkRelayMessageType.Join,
                    ref sendBuffer);
            }
        }
    }

    public struct NetworkRelayServer : IComponentData
    {
        [BurstCompile]
        private struct Drop : IJobParallelFor
        {
            [ReadOnly, DeallocateOnJobCompletion]
            public NativeArray<uint> ids;

            [ReadOnly]
            public NativeParallelHashMap<uint, int> idChannels;

            public NetworkServerSendBuffer.Concurrent sendBuffer;

            [NativeDisableParallelForRestriction]
            public NativeArray<NetworkRelayServerIdentity> identities;

            public void Execute(int index)
            {
                uint id = ids[index];
                var sendBuffer = new NetworkServerSendBuffer.Identity(id, ref this.sendBuffer);

                var identity = identities[sendBuffer.channelIndex];
                if (identity.channel != idChannels[id])
                    return;

                identity.Drop(ref sendBuffer);
            }
        }

        [BurstCompile]
        private struct Clear : IJob
        {
            public NativeParallelHashMap<uint, int> idChannels;

            public void Execute()
            {
                idChannels.Clear();
            }
        }

        public readonly NetworkPipeline Pipeline;

        private NetworkServer __instance;
        private NetworkServerSendBuffer __sendBuffer;

        private NativeList<NetworkRelayServerIdentity> __identities;

        private NativeParallelHashMap<uint, int> __idChannels;

        public NetworkRelayServer(
            in NetworkSettings settings,
            in NativeArray<NetworkPipelineStageId> stages,
            in AllocatorManager.AllocatorHandle allocator)
        {
            __instance = new NetworkServer(settings, allocator);

            __sendBuffer = new NetworkServerSendBuffer(allocator);

            __identities = new NativeList<NetworkRelayServerIdentity>(allocator);

            __idChannels = new NativeParallelHashMap<uint, int>(1, allocator);

            Pipeline = __instance.CreatePipeline(stages);
        }

        public NetworkRelayServer(
            in NativeArray<NetworkPipelineStage> stages,
            in AllocatorManager.AllocatorHandle allocator, 
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
            using (var stageIDs = stages.ToPipelineStageIDs(Allocator.Temp))
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

                this = new NetworkRelayServer(settings, stageIDs, allocator);
                settings.Dispose();
            }
        }

        public void Dispose()
        {
            __instance.Dispose();
            __sendBuffer.Dispose();
            __identities.Dispose();
            __idChannels.Dispose();
        }

        public void Listen(ushort port, NetworkFamily family = NetworkFamily.Ipv4)
        {
            __instance.Listen(port, family);
        }

        public void Disconnect(in NetworkConnection connection)
        {
            __instance.Disconnect(connection);
        }

        public JobHandle Schedule(
            int innerloopBatchCount,
            in JobHandle inputDeps)
        {
            Drop drop;
            drop.ids = __idChannels.GetKeyArray(Allocator.TempJob);
            drop.idChannels = __idChannels;
            drop.sendBuffer = __sendBuffer.AsConcurrent();
            drop.identities = __identities.AsArray();
            var jobHandle = drop.ScheduleByRef(drop.ids.Length, innerloopBatchCount, inputDeps);

            Clear clear;
            clear.idChannels = __idChannels;
            jobHandle = clear.ScheduleByRef(jobHandle);

            NetworkRelayServerListener listener;
            listener.identities = __identities;
            listener.idChannels = __idChannels;

            NetworkRelayServerHandler handler;
            handler.identities = __identities.AsDeferredJobArray();
            handler.idChannels = __idChannels.AsParallelWriter();

            return __instance.Schedule(ref listener, ref handler, ref __sendBuffer, 
                innerloopBatchCount, Pipeline, 
                in jobHandle);
        }
    }
}
