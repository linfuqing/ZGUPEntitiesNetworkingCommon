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
        private int __channel;
        private UnsafeList<byte> __bytes;

        public int channel => __channel;

        public static void SendRelay(
            int type,
            int relayType,
            int identityIndex,
            ref DataStreamReader reader,
            ref DataStreamWriter writer)
        {
            var streamCompressionModel = StreamCompressionModel.Default;
            writer.WritePackedInt(type, streamCompressionModel);
            writer.WritePackedInt(relayType, streamCompressionModel);
            writer.WritePackedInt(identityIndex, streamCompressionModel);

            int numBytes = reader.Length - reader.GetBytesRead();
            using (var bytes = new NativeArray<byte>(numBytes, Allocator.Temp))
            {
                reader.ReadBytes(bytes);
                writer.WriteBytes(bytes);
            }
        }

        public NetworkRelayServerIdentity(in AllocatorManager.AllocatorHandle allocator)
        {
            __channel = 0;
            __bytes = new UnsafeList<byte>(1, allocator);
        }

        public void Dispose()
        {
            __bytes.Dispose();
        }

        public void Clear()
        {
            __channel = 0;
            __bytes.Clear();
        }

        public void Init(ref DataStreamReader reader)
        {
            __bytes.Resize(reader.Length - reader.GetBytesRead(), NativeArrayOptions.UninitializedMemory);
            reader.ReadBytes(AsArray());
        }

        public void SendHeader(
            bool isSendOthers,
            int pipelineIndex,
            int type,
            int identityIndex,
            NetworkServerSendBufferWrapper sendBuffer)
        {
            if (sendBuffer.BeginWrite(pipelineIndex, out var writer))
            {
                __WriteHeader(isSendOthers, type, identityIndex, ref writer);

                sendBuffer.EndWrite(writer);
            }
        }
        
        public void Create(
            int pipelineIndexToSelf,
            int pipelineIndexToOthers,
            int identityIndex,
            int channel,
            NetworkServerSendBufferWrapper sendBuffer)
        {
            __CreateOrJoin((int)NetworkRelayMessageType.Create, 
                pipelineIndexToSelf, 
                pipelineIndexToOthers, 
                identityIndex, 
                channel, 
                sendBuffer);
        }
        
        public void Join(
            int pipelineIndexToSelf,
            int pipelineIndexToOthers,
            int identityIndex,
            int channel,
            NetworkServerSendBufferWrapper sendBuffer)
        {
            __CreateOrJoin((int)NetworkRelayMessageType.Join, 
                pipelineIndexToSelf, 
                pipelineIndexToOthers, 
                identityIndex, 
                channel, 
                sendBuffer);
        }

        public void Leave(
            int pipelineIndexToSelf,
            int pipelineIndexToOthers,
            int identityIndex,
            NetworkServerSendBufferWrapper sendBuffer)
        {
            if (sendBuffer.RemoveChannel(__channel))
            {
                SendHeader(false, pipelineIndexToSelf, (int)NetworkRelayMessageType.Leave, identityIndex, sendBuffer);
                SendHeader(true, pipelineIndexToOthers, (int)NetworkRelayMessageType.Leave, identityIndex, sendBuffer);
            }

            __channel = 0;
        }
        
        public void Relay(
            int pipelineIndex,
            int type,
            int relayType,
            int identityIndex,
            ref DataStreamReader reader,
            NetworkServerSendBufferWrapper sendBuffer)
        {
            if (sendBuffer.BeginWrite(pipelineIndex, out var writer))
            {
                SendRelay(type, relayType, identityIndex, ref reader, ref writer);

                sendBuffer.EndWrite(writer);
            }
        }

        public NativeArray<byte> AsArray()
        {
            NativeArray<byte> bytes;
            unsafe
            {
                bytes = CollectionHelper.ConvertExistingDataToNativeArray<byte>(__bytes.Ptr,
                    __bytes.Length, Allocator.None, true);
            }

            return bytes;
        }

        private void __WriteHeader(bool isSendOthers, int type, int identityIndex, ref DataStreamWriter writer)
        {
            var streamCompressionModel = StreamCompressionModel.Default;
            writer.WritePackedInt(type, streamCompressionModel);
            writer.WritePackedInt(identityIndex, streamCompressionModel);
            writer.WritePackedInt(__channel, streamCompressionModel);

            if (isSendOthers)
                writer.WriteBytes(AsArray());
        }
        
        private void __CreateOrJoin(
            int type, 
            int pipelineIndexToSelf,
            int pipelineIndexToOthers,
            int identityIndex,
            int channel,
            NetworkServerSendBufferWrapper sendBuffer)
        {
            if (!sendBuffer.AddChannel(channel))
                return;

            Leave(pipelineIndexToSelf, pipelineIndexToOthers, identityIndex, sendBuffer);

            __channel = channel;

            SendHeader(false, pipelineIndexToSelf, type, identityIndex, sendBuffer);
            SendHeader(true, pipelineIndexToOthers, type, identityIndex, sendBuffer);
        }
    }

    public struct NetworkRelayServerListener : INetworkServerListener
    {
        public NativeHashMap<NetworkConnection, int> identityIndices;

        public NativeList<NetworkRelayServerIdentity> identities;

        public NativeList<int> identityIndexPool;

        public void Connect(in NetworkConnection connection)
        {
            int index, length = identityIndexPool.Length;
            if (length > 0)
            {
                index = identityIndexPool[--length];
                identityIndexPool.ResizeUninitialized(length);

                identities.ElementAt(index).Clear();
            }
            else
            {
                index = identities.Length;
                identities.Add(new NetworkRelayServerIdentity(Allocator.Persistent));
            }

            identityIndices.Add(connection, index);
        }

        public void Disconnect(in NetworkConnection connection)
        {
            identityIndexPool.Add(identityIndices[connection]);

            identityIndices.Remove(connection);
        }
    }

    public struct NetworkRelayServerHandler : INetworkServerHandler
    {
        public int pipelineIndexDrop;
        public int pipelineIndexRelay;
        public int pipelineIndexSendSelf;
        public int pipelineIndexSendOthers;
        public int pipelineIndexSendOthersFromChannel;

        [ReadOnly] 
        public NativeHashMap<NetworkConnection, int> identityIndices;

        [NativeDisableParallelForRestriction] 
        public NativeArray<NetworkRelayServerIdentity> identities;

        [NativeDisableParallelForRestriction] 
        public NativeArray<int> channelCount;

        public void Connect(NetworkServerSendBufferWrapper sendBuffer)
        {
            sendBuffer.AddChannel(0);
        }

        public void Disconnect(NetworkServerSendBufferWrapper sendBuffer)
        {
            var identityIndex = identityIndices[sendBuffer.Connection];
            var identity = identities[identityIndex];
            identity.Leave(
                pipelineIndexSendSelf,
                pipelineIndexSendOthersFromChannel,
                identityIndex,
                sendBuffer);

            identities[identityIndex] = identity;
        }

        public void Read(ref DataStreamReader reader,
            NetworkServerSendBufferWrapper sendBuffer)
        {
            var identityIndex = identityIndices[sendBuffer.Connection];
            var identity = identities[identityIndex];

            DataStreamWriter writer;
            var streamCompressionModel = StreamCompressionModel.Default;
            int type = reader.ReadPackedInt(streamCompressionModel);
            switch ((NetworkRelayMessageType)type)
            {
                case NetworkRelayMessageType.Init:
                    identity.Init(ref reader);
                    identities[identityIndex] = identity;

                    if (sendBuffer.BeginWrite(pipelineIndexSendSelf, out writer))
                    {
                        writer.WritePackedInt(type, streamCompressionModel);
                        writer.WritePackedInt(identityIndex, streamCompressionModel);
                        sendBuffer.EndWrite(writer);
                    }

                    break;
                case NetworkRelayMessageType.Create:
                    identity.Create(
                        pipelineIndexSendSelf,
                        pipelineIndexSendOthersFromChannel,
                        identityIndex,
                        System.Threading.Interlocked.Increment(ref channelCount.AsSpan()[0]),
                        sendBuffer);

                    identities[identityIndex] = identity;
                    break;
                case NetworkRelayMessageType.Join:
                    identity.Join(
                        pipelineIndexSendSelf,
                        pipelineIndexSendOthersFromChannel,
                        identityIndex,
                        reader.ReadPackedInt(streamCompressionModel), sendBuffer);

                    identities[identityIndex] = identity;

                    break;
                case NetworkRelayMessageType.Leave:
                    identity.Leave(
                        pipelineIndexSendSelf,
                        pipelineIndexSendOthersFromChannel,
                        identityIndex,
                        sendBuffer);

                    identities[identityIndex] = identity;
                    break;
                case NetworkRelayMessageType.Drop:
                    if (sendBuffer.BeginWrite(pipelineIndexDrop, out writer))
                    {
                        writer.WritePackedInt(type, streamCompressionModel);
                        writer.WritePackedInt(identityIndex, streamCompressionModel);
                        sendBuffer.EndWrite(writer);
                    }
                    break;
                case NetworkRelayMessageType.Query:
                    int channel = reader.ReadPackedInt(streamCompressionModel), numIdentities = identities.Length;
                    for (int i = 0; i < numIdentities; ++i)
                    {
                        identity = identities[i];
                        if (identity.channel != channel)
                            continue;

                        identity.SendHeader(true, pipelineIndexSendSelf, (int)NetworkRelayMessageType.Query, i,
                            sendBuffer);
                    }

                    break;
                default:
                    int relayType = reader.ReadPackedInt(streamCompressionModel);
                    switch ((NetworkRelayType)relayType)
                    {
                        case NetworkRelayType.All:
                            identity.Relay(pipelineIndexSendOthers, type, relayType, identityIndex, ref reader,
                                sendBuffer);
                            break;
                        case NetworkRelayType.Channel:
                            identity.Relay(pipelineIndexSendOthersFromChannel, type, relayType, identityIndex,
                                ref reader, sendBuffer);
                            break;
                        default:
                            identity.Relay(pipelineIndexRelay, type, relayType, identityIndex, ref reader, sendBuffer);
                            break;
                    }

                    break;
            }
        }
    }

    public struct NetworkRelayServerBufferHandler : INetworkServerBufferHandler
    {
        public int pipelineIndexDrop;
        public int pipelineIndexRelay;
        public int pipelineIndexSendSelf;
        public int pipelineIndexSendOthersFromChannel;

        [ReadOnly] 
        public NativeHashMap<NetworkConnection, int> identityIndices;

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
            var streamCompressionModel = StreamCompressionModel.Default;
            if (pipelineIndex == pipelineIndexDrop)
            {
                if (identityIndices.TryGetValue(destination, out var identityIndex) &&
                    identityIndex == reader.ReadPackedInt(streamCompressionModel))
                {
                    var identity = identities[identityIndex];
                    identity.Leave(
                        pipelineIndexSendSelf,
                        pipelineIndexSendOthersFromChannel,
                        identityIndex,
                        new NetworkServerSendBufferWrapper(destination, ref sendBuffer));

                    identities[identityIndex] = identity;
                }
            }
            else if(pipelineIndex == pipelineIndexRelay)
            {
                int type = reader.ReadPackedInt(streamCompressionModel),
                    relayType = reader.ReadPackedInt(streamCompressionModel);

                if (relayType >= (int)NetworkRelayType.Identity &&
                    identityIndices.TryGetValue(destination, out var identityIndex) &&
                    identityIndex == relayType)
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
                        reader.ReadPackedInt(streamCompressionModel),
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
            }

            return false;
        }
    }

    public struct NetworkRelayServer : IComponentData
    {
        private int __pipelineIndexDrop;
        private int __pipelineIndexRelay;
        private int __pipelineIndexSendSelf;
        private int __pipelineIndexSendOthers;
        private int __pipelineIndexSendOthersFromChannel;

        private NetworkServer __instance;
        private NetworkServerSendBuffer __sendBuffer;

        private NativeArray<int> __channelCount;

        private NativeList<int> __identityIndexPool;

        private NativeList<NetworkRelayServerIdentity> __identities;

        private NativeHashMap<NetworkConnection, int> __identityIndices;

        public NetworkRelayServer(
            in NetworkSettings settings,
            in NativeArray<NetworkPipelineStageId> stages,
            in AllocatorManager.AllocatorHandle allocator)
        {
            __instance = new NetworkServer(settings, allocator);

            __sendBuffer = new NetworkServerSendBuffer(allocator);

            var pipeline = __instance.CreatePipeline(stages);

            __pipelineIndexDrop = __sendBuffer.CreatePipeline(NetworkServerPipelineType.Custom, pipeline);
            __pipelineIndexRelay = __sendBuffer.CreatePipeline(NetworkServerPipelineType.Custom, pipeline);
            __pipelineIndexSendSelf = __sendBuffer.CreatePipeline(NetworkServerPipelineType.SendSelf, pipeline);
            __pipelineIndexSendOthers = __sendBuffer.CreatePipeline(NetworkServerPipelineType.SendOthers, pipeline);
            __pipelineIndexSendOthersFromChannel =
                __sendBuffer.CreatePipeline(NetworkServerPipelineType.SendOthersFromChannel, pipeline);
            __sendBuffer.CreatePipeline(NetworkServerPipelineType.SendSelfFromOthers, pipeline);

            __channelCount = CollectionHelper.CreateNativeArray<int>(1, allocator);

            __channelCount[0] = 0;

            __identityIndexPool = new NativeList<int>(allocator);

            __identities = new NativeList<NetworkRelayServerIdentity>(allocator);

            __identityIndices = new NativeHashMap<NetworkConnection, int>(1, allocator);
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
            __identityIndexPool.Dispose();
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
            listener.identityIndexPool = __identityIndexPool;

            var identities = __identities.AsDeferredJobArray();
            NetworkRelayServerHandler handler;
            handler.pipelineIndexDrop = __pipelineIndexDrop;
            handler.pipelineIndexRelay = __pipelineIndexRelay;
            handler.pipelineIndexSendSelf = __pipelineIndexSendSelf;
            handler.pipelineIndexSendOthers = __pipelineIndexSendOthers;
            handler.pipelineIndexSendOthersFromChannel = __pipelineIndexSendOthersFromChannel;
            handler.identities = identities;
            handler.identityIndices = __identityIndices;
            handler.channelCount = __channelCount;

            NetworkRelayServerBufferHandler bufferHandler;
            bufferHandler.pipelineIndexDrop = __pipelineIndexDrop;
            bufferHandler.pipelineIndexRelay = __pipelineIndexRelay;
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
