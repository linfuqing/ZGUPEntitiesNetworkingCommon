using System;
using System.Threading;
using Unity.Jobs;
using Unity.Entities;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Networking.Transport;
using Unity.Networking.Transport.Error;
using ZG;

[assembly:RegisterGenericJobType(typeof(NetworkServerInitJob<NetworkRelayServerListener>))]
[assembly:RegisterGenericJobType(typeof(NetworkServerPopEventsJob<NetworkRelayServerHandler>))]
[assembly:RegisterGenericJobType(typeof(NetworkServerSendJob<NetworkRelayServerBufferHandler>))]

namespace ZG
{
    public struct NetworkRelayServerIdentity
    {
        [Flags]
        public enum ChannelFlag
        {
            Creator = 0x01
        }
        
        public readonly uint ID;

        //private UnsafeList<byte> __bytes;

        public int channel
        {
            get;

            private set;
        }

        public ChannelFlag channelFlag
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
            writer.Flush();
            
            int byteOffset = reader.GetBytesRead(), numBytes = reader.Length - byteOffset;

            NativeArray<byte> bytes;
            unsafe
            {
                bytes = CollectionHelper.ConvertExistingDataToNativeArray<byte>((byte*)reader.GetUnsafeReadOnlyPtr() + byteOffset, numBytes,
                    Allocator.None, true);
            }

            writer.WriteBytes(bytes);
            
            reader.SeekSet(reader.Length);
        }

        public NetworkRelayServerIdentity(uint id, in AllocatorManager.AllocatorHandle allocator)
        {
            ID = id;
            
            channel = 0;

            channelFlag = 0;
            //__bytes = new UnsafeList<byte>(1, allocator);
        }

        public void Dispose()
        {
            //__bytes.Dispose();
        }

        public void Clear()
        {
            channel = 0;
            channelFlag = 0;
            //__bytes.Clear();
        }

        /*public void Init(ref DataStreamReader reader)
        {
            __bytes.Resize(reader.Length - reader.GetBytesRead(), NativeArrayOptions.UninitializedMemory);
            reader.ReadBytes(AsArray());
        }*/

        public void SendHeader(
            bool isSendOthers,
            int pipelineIndex,
            int type,
            NetworkServerSendBufferWrapper sendBuffer)
        {
            if (sendBuffer.BeginWrite(pipelineIndex, out var writer))
            {
                __WriteHeader(isSendOthers, type, sendBuffer.GetPayload(ID), ref writer);

                sendBuffer.EndWrite(writer);
            }
        }
        
        public void Create(
            int pipelineIndexToSelf,
            int pipelineIndexToOthers,
            int channel,
            NetworkServerSendBufferWrapper sendBuffer)
        {
            if(__CreateOrJoin((int)NetworkRelayMessageType.Create, 
                pipelineIndexToSelf, 
                pipelineIndexToOthers, 
                channel, 
                sendBuffer))
                channelFlag |= ChannelFlag.Creator;
        }
        
        public void Join(
            int pipelineIndexToSelf,
            int pipelineIndexToOthers,
            int channel,
            NetworkServerSendBufferWrapper sendBuffer)
        {
            __CreateOrJoin((int)NetworkRelayMessageType.Join, 
                pipelineIndexToSelf, 
                pipelineIndexToOthers, 
                channel, 
                sendBuffer);
        }

        public void Leave(
            int pipelineIndexToSelf,
            int pipelineIndexToOthers,
            NetworkServerSendBufferWrapper sendBuffer)
        {
            __DropOrLeave((int)NetworkRelayMessageType.Leave, pipelineIndexToSelf, pipelineIndexToOthers, sendBuffer);
        }

        public void Drop(
            int pipelineIndexToSelf,
            int pipelineIndexToOthers,
            NetworkServerSendBufferWrapper sendBuffer)
        {
            __DropOrLeave((int)NetworkRelayMessageType.Drop, pipelineIndexToSelf, pipelineIndexToOthers, sendBuffer);
        }
        
        public void Relay(
            int pipelineIndex,
            int type,
            int relayType,
            ref DataStreamReader reader,
            NetworkServerSendBufferWrapper sendBuffer)
        {
            if (sendBuffer.BeginWrite(pipelineIndex, out var writer))
            {
                SendRelay(type, relayType, sendBuffer.ID, ref reader, ref writer);

                sendBuffer.EndWrite(writer);
            }
        }

        /*public NativeArray<byte> AsArray()
        {
            NativeArray<byte> bytes;
            unsafe
            {
                bytes = CollectionHelper.ConvertExistingDataToNativeArray<byte>(__bytes.Ptr,
                    __bytes.Length, Allocator.None, true);
            }

            return bytes;
        }*/

        private void __WriteHeader(bool isSendOthers, int type, in NativeArray<byte> payload, ref DataStreamWriter writer)
        {
            var streamCompressionModel = StreamCompressionModel.Default;
            writer.WritePackedInt(type, streamCompressionModel);
            //writer.WritePackedInt(identityIndex, streamCompressionModel);
            writer.WritePackedInt(channel, streamCompressionModel);

            if (isSendOthers)
                writer.WriteBytes(payload);
        }
        
        private bool __CreateOrJoin(
            int type, 
            int pipelineIndexToSelf,
            int pipelineIndexToOthers,
            int channel,
            NetworkServerSendBufferWrapper sendBuffer)
        {
            if (!sendBuffer.AddChannel(channel))
                return false;

            Leave(pipelineIndexToSelf, pipelineIndexToOthers, sendBuffer);

            this.channel = channel;

            SendHeader(false, pipelineIndexToSelf, type, sendBuffer);
            SendHeader(true, pipelineIndexToOthers, type, sendBuffer);

            return true;
        }

        private void __DropOrLeave(
            int type, 
            int pipelineIndexToSelf,
            int pipelineIndexToOthers, 
            NetworkServerSendBufferWrapper sendBuffer)
        {
            if (channel == 0)
                return;
            
            if (sendBuffer.RemoveChannel(channel))
            {
                SendHeader(false, pipelineIndexToSelf, type, sendBuffer);
                SendHeader(true, pipelineIndexToOthers, type, sendBuffer);
            }

            channelFlag = 0;
            channel = 0;
        }
    }

    /*public struct NetworkRelayServerChannel
    {
        private int  __ownerIdentityIndex;

        private int __identitySlotCount;

        private int __maxIdentitySlotCount;

        public bool Alloc(int maxIdentitySlotCount, int ownerIdentityIndex)
        {
            if (Interlocked.CompareExchange(ref __ownerIdentityIndex, ownerIdentityIndex, -1) == ownerIdentityIndex)
            {
                __maxIdentitySlotCount = maxIdentitySlotCount;

                __identitySlotCount = maxIdentitySlotCount - 1;
                
                return true;
            }

            return false;
        }
        
        public bool AddIdentity()
        {
            if (Interlocked.Decrement(ref __identitySlotCount) > 0)
                return true;

            Interlocked.Increment(ref __identitySlotCount);

            return false;
        }
        
        public void RemoveIdentity()
        {
            if (Interlocked.Increment(ref __identitySlotCount) == __maxIdentitySlotCount)
                __ownerIdentityIndex = -1;
        }
    }*/

    public struct NetworkRelayServerListener : INetworkServerListener
    {
        public NativeHashMap<uint, int> identityIndices;

        public NativeList<NetworkRelayServerIdentity> identities;

        public void Connect(in NetworkConnection connection, uint id)
        {
            if (identityIndices.ContainsKey(id))
                return;

            int index = identities.Length;
            identities.Add(new NetworkRelayServerIdentity(id, Allocator.Persistent));
            identityIndices.Add(id, index);
        }

        public void Disconnect(in NetworkConnection connection, uint id)
        {
            //identityIndices.Remove(connection);
        }
    }

    public struct NetworkRelayServerHandler : INetworkServerHandler
    {
        public int pipelineIndexCustom;
        public int pipelineIndexSendSelf;
        public int pipelineIndexSendOthers;
        public int pipelineIndexSendOthersFromChannel;

        [ReadOnly] 
        public NativeHashMap<uint, int> identityIndices;

        [NativeDisableParallelForRestriction] 
        public NativeArray<NetworkRelayServerIdentity> identities;
        
        /*[NativeDisableParallelForRestriction]
        public NativeArray<NetworkRelayServerChannel> channels;
        
        [NativeDisableParallelForRestriction]
        public NativeArray<int> channelPool;*/

        [NativeDisableParallelForRestriction] 
        public NativeArray<int> channelCount;

        public void Connect(NetworkServerSendBufferWrapper sendBuffer)
        {
            //空的时候才能Or
            sendBuffer.AddChannel(0);
            
            var identityIndex = identityIndices[sendBuffer.ID];
            var identity = identities[identityIndex];
            if (identity.channel != 0)
            {
                identity.SendHeader(false, pipelineIndexSendSelf,
                    (identity.channelFlag & NetworkRelayServerIdentity.ChannelFlag.Creator) ==
                    NetworkRelayServerIdentity.ChannelFlag.Creator
                        ? (int)NetworkRelayMessageType.Create
                        : (int)NetworkRelayMessageType.Join,
                    sendBuffer);
                
                int numIdentities = identities.Length;
                NetworkRelayServerIdentity channelIdentity;
                for(int i = 0; i < numIdentities; ++i)
                {
                    channelIdentity = identities[i];
                    if (channelIdentity.channel != identity.channel || i == identityIndex)
                        continue;

                    channelIdentity.SendHeader(true, 
                        pipelineIndexSendSelf,
                        (int)NetworkRelayMessageType.Join,
                        sendBuffer);
                }
            }
        }

        public void Disconnect(NetworkServerSendBufferWrapper sendBuffer)
        {
            sendBuffer.RemoveChannel(0);
            
            /*var identityIndex = identityIndices[sendBuffer.ID];
            var identity = identities[identityIndex];
            identity.Leave(
                pipelineIndexSendSelf,
                pipelineIndexSendOthersFromChannel,
                sendBuffer);

            identities[identityIndex] = identity;*/
        }

        public void Read(ref DataStreamReader reader,
            NetworkServerSendBufferWrapper sendBuffer)
        {
            var identityIndex = identityIndices[sendBuffer.ID];
            var identity = identities[identityIndex];

            NetworkRelayServerIdentity channelIdentity;
            DataStreamWriter writer;
            var streamCompressionModel = StreamCompressionModel.Default;
            int type = reader.ReadPackedInt(streamCompressionModel), channel, numIdentities;
            switch ((NetworkRelayMessageType)type)
            {
                case NetworkRelayMessageType.Init:
                    //identity.Init(ref reader);
                    //identities[identityIndex] = identity;

                    if (sendBuffer.BeginWrite(pipelineIndexSendSelf, out writer))
                    {
                        writer.WritePackedInt(type, streamCompressionModel);
                        //writer.WritePackedInt(identityIndex, streamCompressionModel);
                        sendBuffer.EndWrite(writer);
                    }

                    break;
                case NetworkRelayMessageType.Create:
                    identity.Create(
                        pipelineIndexSendSelf,
                        pipelineIndexSendOthersFromChannel,
                        Interlocked.Increment(ref channelCount.AsSpan()[0]),
                        sendBuffer);

                    identities[identityIndex] = identity;
                    break;
                case NetworkRelayMessageType.Join:
                    channel = reader.ReadPackedInt(streamCompressionModel);
                    identity.Join(
                        pipelineIndexSendSelf,
                        pipelineIndexSendOthersFromChannel,
                        channel, sendBuffer);

                    identities[identityIndex] = identity;

                    numIdentities = identities.Length;
                    for(int i = 0; i < numIdentities; ++i)
                    {
                        if(i == identityIndex)
                            continue;
                        
                        channelIdentity = identities[i];
                        if (channelIdentity.channel != channel)
                            continue;

                        channelIdentity.SendHeader(true, pipelineIndexSendSelf, (int)NetworkRelayMessageType.Join,
                            sendBuffer);
                    }

                    break;
                case NetworkRelayMessageType.Leave:
                    identity.Leave(
                        pipelineIndexSendSelf,
                        pipelineIndexSendOthersFromChannel,
                        sendBuffer);

                    identities[identityIndex] = identity;
                    break;
                case NetworkRelayMessageType.Drop:
                    var id = reader.ReadPackedUInt(streamCompressionModel);
                    if (identities[identityIndices[id]].channel == identity.channel && 
                        sendBuffer.BeginWrite(pipelineIndexCustom, out writer))
                    {
                        writer.WritePackedInt(type, streamCompressionModel);
                        writer.WritePackedUInt(id, streamCompressionModel);
                        sendBuffer.EndWrite(writer);
                    }
                    break;
                case NetworkRelayMessageType.Query:
                    channel = reader.ReadPackedInt(streamCompressionModel);
                    numIdentities = identities.Length;
                    for(int i = 0; i < numIdentities; ++i)
                    {
                        channelIdentity = identities[i];
                        if (channelIdentity.channel != channel)
                            continue;

                        channelIdentity.SendHeader(true, pipelineIndexSendSelf, (int)NetworkRelayMessageType.Query,
                            sendBuffer);
                    }

                    break;
                default:
                    int relayType = reader.ReadPackedInt(streamCompressionModel);
                    
                    //UnityEngine.Debug.LogError($"Relay {type} :{(NetworkRelayType)relayType} : {identityIndex}");
                    switch ((NetworkRelayType)relayType)
                    {
                        case NetworkRelayType.All:
                            identity.Relay(pipelineIndexSendOthers, type, relayType, ref reader,
                                sendBuffer);
                            break;
                        case NetworkRelayType.Channel:
                            identity.Relay(pipelineIndexSendOthersFromChannel, type, relayType,
                                ref reader, sendBuffer);
                            break;
                        default:
                            identity.Relay(pipelineIndexCustom, type, relayType, ref reader, sendBuffer);
                            break;
                    }

                    break;
            }
        }
    }

    public struct NetworkRelayServerBufferHandler : INetworkServerBufferHandler
    {
        public int pipelineIndexCustom;
        public int pipelineIndexSendSelf;
        public int pipelineIndexSendOthersFromChannel;

        [ReadOnly] 
        public NativeHashMap<uint, int> identityIndices;

        [NativeDisableParallelForRestriction] 
        public NativeArray<NetworkRelayServerIdentity> identities;

        public bool Apply(
            int pipelineIndex, 
            DataStreamReader reader,
            in NetworkConnection source,
            in NetworkConnection destination,
            ref NetworkDriver.Concurrent driver, 
            ref NetworkServerSendBuffer.Concurrent sendBuffer)
        {
            var sendBufferWrapper = new NetworkServerSendBufferWrapper(destination, ref sendBuffer);
            var streamCompressionModel = StreamCompressionModel.Default;
            if (pipelineIndex == pipelineIndexCustom)
            {
                int type = reader.ReadPackedInt(streamCompressionModel);
                switch ((NetworkRelayMessageType)type)
                {
                    case NetworkRelayMessageType.Drop:
                        if (sendBufferWrapper.ID == reader.ReadPackedUInt(streamCompressionModel))
                        {
                            int identityIndex = identityIndices[sendBufferWrapper.ID];
                            var identity = identities[identityIndex];
                            identity.Drop(
                                pipelineIndexSendSelf,
                                pipelineIndexSendOthersFromChannel,
                                sendBufferWrapper);

                            identities[identityIndex] = identity;
                        }
                        break;
                    default:
                        int relayType = reader.ReadPackedInt(streamCompressionModel);
                        if (((NetworkRelayType)relayType).RelayID() == sendBufferWrapper.ID)
                        {
                            int result = driver.BeginSend(destination, out var writer);
                            if (result < 0)
                            {
                                NetworkSendBuffer.LogError((StatusCode)result);

                                return false;
                            }

                            NetworkRelayServerIdentity.SendRelay(
                                type,
                                relayType,
                                sendBuffer[source],
                                ref reader,
                                ref writer);

                            result = driver.EndSend(writer);
                            if (result < 0)
                            {
                                NetworkSendBuffer.LogError((StatusCode)result);

                                return false;
                            }

                            return true;
                        }
                        break;
                }
            }

            return false;
        }
    }

    public struct NetworkRelayServer : IComponentData
    {
        private int __pipelineIndexCustom;
        private int __pipelineIndexSendSelf;
        private int __pipelineIndexSendOthers;
        private int __pipelineIndexSendOthersFromChannel;

        private NetworkServer __instance;
        private NetworkServerSendBuffer __sendBuffer;

        private NativeArray<int> __channelCount;

        //private NativeList<int> __identityIndexPool;

        private NativeList<NetworkRelayServerIdentity> __identities;

        private NativeHashMap<uint, int> __identityIndices;

        public NetworkRelayServer(
            in NetworkSettings settings,
            in NativeArray<NetworkPipelineStageId> stages,
            in AllocatorManager.AllocatorHandle allocator)
        {
            __instance = new NetworkServer(settings, allocator);

            __sendBuffer = new NetworkServerSendBuffer(allocator);

            var pipeline = __instance.CreatePipeline(stages);

            __pipelineIndexCustom = __sendBuffer.CreatePipeline(NetworkServerPipelineType.Custom, pipeline);
            __pipelineIndexSendSelf = __sendBuffer.CreatePipeline(NetworkServerPipelineType.SendSelf, pipeline);
            __pipelineIndexSendOthers = __sendBuffer.CreatePipeline(NetworkServerPipelineType.SendOthers, pipeline);
            __pipelineIndexSendOthersFromChannel =
                __sendBuffer.CreatePipeline(NetworkServerPipelineType.SendOthersFromChannel, pipeline);
            __sendBuffer.CreatePipeline(NetworkServerPipelineType.SendSelfFromOthers, pipeline);

            __channelCount = CollectionHelper.CreateNativeArray<int>(1, allocator);

            __channelCount[0] = 0;

            //__identityIndexPool = new NativeList<int>(allocator);

            __identities = new NativeList<NetworkRelayServerIdentity>(allocator);

            __identityIndices = new NativeHashMap<uint, int>(1, allocator);
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
            __channelCount.Dispose();
            //__identityIndexPool.Dispose();
            __identities.Dispose();
            __identityIndices.Dispose();
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
            NetworkRelayServerListener listener;
            listener.identities = __identities;
            listener.identityIndices = __identityIndices;
            //listener.identityIndexPool = __identityIndexPool;

            var identities = __identities.AsDeferredJobArray();
            NetworkRelayServerHandler handler;
            handler.pipelineIndexCustom = __pipelineIndexCustom;
            handler.pipelineIndexSendSelf = __pipelineIndexSendSelf;
            handler.pipelineIndexSendOthers = __pipelineIndexSendOthers;
            handler.pipelineIndexSendOthersFromChannel = __pipelineIndexSendOthersFromChannel;
            handler.identities = identities;
            handler.identityIndices = __identityIndices;
            handler.channelCount = __channelCount;

            NetworkRelayServerBufferHandler bufferHandler;
            bufferHandler.pipelineIndexCustom = __pipelineIndexCustom;
            bufferHandler.pipelineIndexSendSelf = __pipelineIndexSendSelf;
            bufferHandler.pipelineIndexSendOthersFromChannel = __pipelineIndexSendOthersFromChannel;
            bufferHandler.identityIndices = __identityIndices;
            bufferHandler.identities = identities;

            return __instance.Schedule(ref listener, ref handler, ref bufferHandler, ref __sendBuffer,
                innerloopBatchCount,
                in inputDeps);
        }
    }
}
