using System.Threading;
using Unity.Jobs;
using Unity.Entities;
using Unity.Collections;
using Unity.Networking.Transport;
using ZG;
using Unity.Burst;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Mathematics;

[assembly:RegisterGenericJobType(typeof(NetworkServerInitJob<NetworkRelayServerListener>))]
[assembly:RegisterGenericJobType(typeof(NetworkServerPopEventsJob<NetworkRelayServerHandler>))]

namespace ZG
{
    public struct NetworkRelayServerChannelModifier
    {
        public enum Type
        {
            Create = NetworkRelayMessageType.Create,
            Join = NetworkRelayMessageType.Join,
            Leave = NetworkRelayMessageType.Leave,
            Drop = NetworkRelayMessageType.Drop,
            Matching = NetworkRelayMessageType.Matching,
            Match = NetworkRelayMessageType.Match,
            Mismatch = NetworkRelayMessageType.Mismatch
        }

        public Type type;
        public int source;
        public int destination;
        public uint id;
    }

    public struct NetworkRelayServerMatch
    {
        public int index;
        public double startTime;
        public NetworkRelayMatch value;
    }

    public struct NetworkRelayServerChannel
    {
        private int __slot;

        public int capacity
        {
            get;

            private set;
        }

        public int count => capacity - __slot;

        public static ref NetworkRelayServerChannel ElementAt(ref NativeArray<NetworkRelayServerChannel> channels, int index)
        {
            return ref channels.AsSpan()[index];
        }

        public void Create(int capacity)
        {
            Interlocked.Add(ref __slot, capacity - 1 - this.capacity);

            this.capacity = capacity;
        }

        public void Leave()
        {
            Interlocked.Increment(ref  __slot);
        }
        
        public bool Join(out int slot)
        {
            slot = Interlocked.Decrement(ref __slot);
            if (slot < 0)
            {
                Interlocked.Increment(ref  __slot);

                return false;
            }

            return true;
        }
    }

    public struct NetworkRelayServerIdentity
    {
        public const int CHANNEL_NULL = -1;

        public readonly uint ID;

        //private UnsafeList<byte> __bytes;

        public bool isOnline
        {
            get => (channelFlag & NetworkRelayChannelFlag.Online) == NetworkRelayChannelFlag.Online;

            set
            {
                if (value)
                    channelFlag |= NetworkRelayChannelFlag.Online;
                else
                    channelFlag &= ~NetworkRelayChannelFlag.Online;
            }
        }

        public bool canMatch => (channelFlag & NetworkRelayChannelFlag.Online) ==
                                  NetworkRelayChannelFlag.Online &&
                                  ((int)channelFlag >> (int)NetworkRelayChannelFlag.ShiftToStatus) == 0;

        public int match
        {
            get;

            private set;
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

            match = 0;
            
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

        public void SetStatus(int value, ref NetworkServerSendBuffer.Identity sendBuffer)
        {
            var channelFlag = this.channelFlag;
            channelFlag &= NetworkRelayChannelFlag.All;
            channelFlag |= (NetworkRelayChannelFlag)(value << (int)NetworkRelayChannelFlag.ShiftToStatus);
            if (channelFlag == this.channelFlag)
                return;

            if (value == 0 && (channelFlag & NetworkRelayChannelFlag.Temp) == NetworkRelayChannelFlag.Temp)
            {
                channelFlag &= ~NetworkRelayChannelFlag.Temp;

                Leave(ref sendBuffer);
            }

            this.channelFlag = channelFlag;

            var channel = this.channel;
            if (channel != CHANNEL_NULL && sendBuffer.BeginWrite(channel, out var writer))
            {
                var streamCompressionModel = StreamCompressionModel.Default;
                writer.WritePackedInt((int)NetworkRelayMessageType.Status, streamCompressionModel);
                writer.WritePackedInt((int)channelFlag, streamCompressionModel);
                writer.WritePackedUInt(ID, streamCompressionModel);

                sendBuffer.EndWrite(writer);
            }
        }

        public void SendHeader(
            int type,
            ref NetworkServerSendBuffer.Identity sendBuffer)
        {
            if (sendBuffer.BeginWrite(sendBuffer.ID, out var writer))
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
                __WriteHeader(true, type, sendBuffer.GetPayload(ID), ref writer);

                sendBuffer.EndWrite(writer);
            }
        }
        
        public bool Create(
            int channel,
            ref NetworkServerSendBuffer.Identity sendBuffer)
        {
            return __CreateOrJoin(NetworkRelayChannelFlag.Creator,
                (int)NetworkRelayMessageType.Create,
                channel,
                ref sendBuffer);
        }
        
        public bool Join(
            bool isTemp, 
            int channel,
            ref NetworkServerSendBuffer.Identity sendBuffer)
        {
            return __CreateOrJoin(
                isTemp ? NetworkRelayChannelFlag.Temp : 0, 
                (int)NetworkRelayMessageType.Join,
                channel,
                ref sendBuffer);
        }

        public bool Leave(ref NetworkServerSendBuffer.Identity sendBuffer)
        {
            return __DropOrLeave((int)NetworkRelayMessageType.Leave, ref sendBuffer);
        }

        public bool Drop(
            ref NetworkServerSendBuffer.Identity sendBuffer)
        {
            return __DropOrLeave((int)NetworkRelayMessageType.Drop, ref sendBuffer);
        }

        public bool Matching(int value, ref NetworkServerSendBuffer.Identity sendBuffer)
        {
            if (match != 0 || 
                channel != CHANNEL_NULL && (channelFlag & NetworkRelayChannelFlag.Creator) != NetworkRelayChannelFlag.Creator || 
                !canMatch)
                return false;
            
            match = value;
            
            __Match((int)NetworkRelayMessageType.Matching, ref sendBuffer);
            
            return true;
        }

        public bool Match(int match, int distance, ref NetworkServerSendBuffer.Identity sendBuffer)
        {
            if (this.match != 0)
            {
                if (sendBuffer.BeginWrite(sendBuffer.ID, out var writer))
                {
                    var streamCompressionModel = StreamCompressionModel.Default;

                    writer.WritePackedInt((int)NetworkRelayMessageType.Match, streamCompressionModel);
                    writer.WritePackedInt(match, streamCompressionModel);
                    writer.WritePackedInt(distance, streamCompressionModel);
                    sendBuffer.EndWrite(writer);
                }
                
                this.match = 0;

                return true;
            }

            return false;
        }
        
        public bool Mismatch(ref NetworkServerSendBuffer.Identity sendBuffer)
        {
            if (match != 0)
            {
                __Match((int)NetworkRelayMessageType.Mismatch, ref sendBuffer);

                match = 0;

                return true;
            }

            return false;
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
                    if (channel == CHANNEL_NULL || !sendBuffer.BeginWrite(channel, out writer))
                        return;
                    break;
                default:
                    if (!sendBuffer.BeginWrite(relayType.RelayID(), out writer))
                        return;
                    break;
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
            writer.WritePackedInt((int)channelFlag, streamCompressionModel);

            if (isSendOthers)
                writer.WriteBytes(payload);
        }

        private bool __CreateOrJoin(
            NetworkRelayChannelFlag channelFlag, 
            int type, 
            int channel,
            ref NetworkServerSendBuffer.Identity sendBuffer)
        {
            if (!sendBuffer.AddChannel(ID, channel))
                return false;

            Leave(ref sendBuffer);

            this.channelFlag |= channelFlag;
            this.channel = channel;

            SendHeader(type, ref sendBuffer);
            SendHeader(channel, type, ref sendBuffer);

            Mismatch(ref sendBuffer);

            return true;
        }

        private bool __DropOrLeave(
            int type, 
            ref NetworkServerSendBuffer.Identity sendBuffer)
        {
            if (channel == CHANNEL_NULL)
                return false;
            
            if (sendBuffer.RemoveChannel(ID, channel))
            {
                SendHeader(type, ref sendBuffer);
                SendHeader(channel, type, ref sendBuffer);
            }

            channelFlag &= ~(NetworkRelayChannelFlag.Creator | NetworkRelayChannelFlag.Temp);
            channel = CHANNEL_NULL;
            
            Mismatch(ref sendBuffer);

            return true;
        }
        
        private void __Match(
            int type,
            ref NetworkServerSendBuffer.Identity sendBuffer)
        {
            var streamCompressionModel = StreamCompressionModel.Default;

            var channel = this.channel;
            if (channel != CHANNEL_NULL && sendBuffer.BeginWrite(channel, out var writer))
            {
                writer.WritePackedInt(type, streamCompressionModel);
                writer.WritePackedInt(match, streamCompressionModel);
                writer.WritePackedUInt(ID, streamCompressionModel);
                sendBuffer.EndWrite(writer);
            }

            if (sendBuffer.BeginWrite(sendBuffer.ID, out writer))
            {
                writer.WritePackedInt(type, streamCompressionModel);
                writer.WritePackedInt(match, streamCompressionModel);
                sendBuffer.EndWrite(writer);
            }
        }
    }

    public struct NetworkRelayServerListener : INetworkServerListener
    {
        public NativeList<NetworkRelayServerIdentity> identities;

        public NativeList<NetworkRelayServerChannel> channels;

        public NativeList<NetworkRelayServerMatch> matches;

        public void Connect(in NetworkConnection connection, uint id, int connectionIndex, int channelIndex, NativeArray<byte> payload)
        {
            if (channelIndex < identities.Length)
                UnityEngine.Assertions.Assert.AreEqual(id, identities[channelIndex].ID);
            else
            {
                UnityEngine.Assertions.Assert.AreEqual(channelIndex, identities.Length);

                identities.Add(new NetworkRelayServerIdentity(id, Allocator.Persistent));

                int numIdentities = identities.Length;
                channels.Resize(numIdentities, NativeArrayOptions.ClearMemory);
                matches.Resize(numIdentities, NativeArrayOptions.UninitializedMemory);
            }
        }

        public void Disconnect(in NetworkConnection connection, uint id)
        {
            //identityIndices.Remove(connection);
        }
    }

    public struct NetworkRelayServerHandler : INetworkServerHandler
    {
        public double time;

        [ReadOnly]
        public NativeParallelMultiHashMap<int, uint> channelIDs;

        [NativeDisableParallelForRestriction] 
        public NativeArray<NetworkRelayServerIdentity> identities;

        [NativeDisableParallelForRestriction] 
        public NativeArray<NetworkRelayServerChannel> channels;

        [NativeDisableParallelForRestriction]
        public NativeArray<NetworkRelayServerMatch> matches;

        [NativeDisableParallelForRestriction]
        public NativeArray<int> matchCount;

        public NativeQueue<NetworkRelayServerChannelModifier>.ParallelWriter channelModifiers;

        public void Connect(ref NetworkServerSendBuffer.Identity sendBuffer)
        {
            var identityIndex = sendBuffer.channelIndex;
            var identity = identities[identityIndex];
            identity.isOnline = true;
            identities[identityIndex] = identity;

            int channel = identity.channel;
            if (channel != NetworkRelayServerIdentity.CHANNEL_NULL)
            {
                var channelFlag = identity.channelFlag;
                if (sendBuffer.BeginWrite(channel, out var writer))
                {
                    var streamCompressionModel = StreamCompressionModel.Default;
                    writer.WritePackedInt((int)NetworkRelayMessageType.Connect, streamCompressionModel);
                    writer.WritePackedInt((int)channelFlag, streamCompressionModel);
                    writer.WritePackedUInt(sendBuffer.ID, streamCompressionModel);
                        
                    sendBuffer.EndWrite(writer);
                }
                
                identity.SendHeader((channelFlag & NetworkRelayChannelFlag.Creator) ==
                    NetworkRelayChannelFlag.Creator
                        ? (int)NetworkRelayMessageType.Create
                        : (int)NetworkRelayMessageType.Join,
                    ref sendBuffer);

                __SendChannelJoins(channel, ref sendBuffer);
            }
        }

        public void Disconnect(ref NetworkServerSendBuffer.Identity sendBuffer)
        {
            var identityIndex = sendBuffer.channelIndex;
            var identity = identities[identityIndex];
            int channel = identity.channel;
            if (channel != NetworkRelayServerIdentity.CHANNEL_NULL)
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
            var streamCompressionModel = StreamCompressionModel.Default;
            int type = reader.ReadPackedInt(streamCompressionModel);
            switch ((NetworkRelayMessageType)type)
            {
                case NetworkRelayMessageType.Status:
                    {
                        var identityIndex = sendBuffer.channelIndex;
                        var identity = identities[identityIndex];

                        int source = identity.channel;
                        identity.SetStatus(reader.ReadPackedInt(streamCompressionModel), ref sendBuffer);
                        int destination = identity.channel;
                        if (destination != source)
                        {
                            if (source != NetworkRelayServerIdentity.CHANNEL_NULL)
                                NetworkRelayServerChannel.ElementAt(ref channels, source).Leave();
                            
                            if (destination != NetworkRelayServerIdentity.CHANNEL_NULL)
                                NetworkRelayServerChannel.ElementAt(ref channels, source).Join(out _);

                            __ModifyChannel(NetworkRelayServerChannelModifier.Type.Leave, source, destination,
                                sendBuffer.ID, ref sendBuffer);
                        }

                        identities[identityIndex] = identity;
                    }
                    break;
                case NetworkRelayMessageType.Create:
                    {
                        var identityIndex = sendBuffer.channelIndex;
                        var identity = identities[identityIndex];
                        int channel = identity.channel;
                        if (identity.Create(
                            sendBuffer.channelIndex,
                            ref sendBuffer))
                        {
                            int targetChannel = identity.channel;

                            NetworkRelayServerChannel.ElementAt(ref channels, targetChannel)
                                .Create(reader.ReadPackedInt(streamCompressionModel));

                            identities[identityIndex] = identity;

                            __SendChannelJoins(targetChannel, ref sendBuffer);

                            __ModifyChannel(NetworkRelayServerChannelModifier.Type.Create, channel, targetChannel,
                                sendBuffer.ID, ref sendBuffer);
                        }
                    }
                    break;
                case NetworkRelayMessageType.Join:
                    {
                        var identityIndex = sendBuffer.channelIndex;
                        var identity = identities[identityIndex];
                        int channelIndex = identity.channel, targetChannelIndex = reader.ReadPackedInt(streamCompressionModel);
                        bool isCreator = (identity.channelFlag &
                                         NetworkRelayChannelFlag.Creator) == NetworkRelayChannelFlag.Creator, result = false;
                        if (targetChannelIndex >= 0 && targetChannelIndex < channels.Length)
                        {
                            ref var channel = ref NetworkRelayServerChannel
                                .ElementAt(ref channels, targetChannelIndex);
                            if (channel.Join(out _))
                            {
                                if (identity.Join(
                                        false, 
                                        targetChannelIndex, 
                                        ref sendBuffer))
                                {
                                    identities[identityIndex] = identity;

                                    __SendChannelJoins(targetChannelIndex, ref sendBuffer);

                                    __ModifyChannel(NetworkRelayServerChannelModifier.Type.Join, channelIndex,
                                        targetChannelIndex,
                                        sendBuffer.ID, ref sendBuffer);

                                    result = true;
                                }
                                else
                                    channel.Leave();
                            }
                        }

                        if (channelIndex != NetworkRelayServerIdentity.CHANNEL_NULL && channelIndex != identity.channel)
                        {
                            ref var channel = ref NetworkRelayServerChannel
                                .ElementAt(ref channels, channelIndex);
                            channel.Leave();

                            if (isCreator)
                            {
                                foreach (uint channelID in channelIDs.GetValuesForKey(channelIndex))
                                {
                                    if (channelID == sendBuffer.ID)
                                        continue;

                                    __ModifyChannel(NetworkRelayServerChannelModifier.Type.Drop, channelIndex,
                                        NetworkRelayServerIdentity.CHANNEL_NULL,
                                        channelID,
                                        ref sendBuffer);
                                }
                            }
                        }

                        if (!result)
                        {
                            if (sendBuffer.BeginWrite(sendBuffer.ID, out var writer))
                            {
                                writer.WritePackedInt((int)NetworkRelayMessageType.JoinFailed, streamCompressionModel);
                                writer.WritePackedInt(targetChannelIndex, streamCompressionModel);
                                
                                sendBuffer.EndWrite(writer);
                            }
                        }
                    }
                    break;
                case NetworkRelayMessageType.Leave:
                    {
                        var identityIndex = sendBuffer.channelIndex;
                        var identity = identities[identityIndex];
                        int channel = identity.channel;
                        if (identity.Leave(
                            ref sendBuffer))
                        {
                            NetworkRelayServerChannel
                                .ElementAt(ref channels, channel).Leave();

                            identities[identityIndex] = identity;

                            __ModifyChannel(NetworkRelayServerChannelModifier.Type.Leave, channel,
                                NetworkRelayServerIdentity.CHANNEL_NULL, sendBuffer.ID,
                                ref sendBuffer);
                        }
                    }
                    break;
                case NetworkRelayMessageType.Drop:
                    {
                        uint id = reader.ReadPackedUInt(streamCompressionModel);
                        var identityIndex = sendBuffer.GetChannelIndex(id);
                        if (identityIndex != -1)
                        {
                            var identity = identities[identityIndex];
                            int channel = identity.channel;
                            __ModifyChannel(NetworkRelayServerChannelModifier.Type.Drop, channel,
                                NetworkRelayServerIdentity.CHANNEL_NULL, id,
                                ref sendBuffer);
                        }
                    }
                    break;
                case NetworkRelayMessageType.Matching:
                case NetworkRelayMessageType.Match:
                    {
                        var identityIndex = sendBuffer.channelIndex;
                        var identity = identities[identityIndex];
                        if (identity.Matching(Interlocked.Increment(ref matchCount.AsSpan()[0]), ref sendBuffer))
                        {
                            NetworkRelayServerMatch match;
                            match.index = identity.match;
                            match.startTime = time;
                            match.value = new NetworkRelayMatch(ref reader, streamCompressionModel);
                            matches[identityIndex] = match;

                            int channel = identity.channel;
                            identities[identityIndex] = identity;

                            __ModifyChannel(NetworkRelayServerChannelModifier.Type.Matching, channel, channel,
                                sendBuffer.ID, ref sendBuffer);
                        }
                    }
                    break;
                case NetworkRelayMessageType.Mismatch:
                    {
                        var identityIndex = sendBuffer.channelIndex;
                        var identity = identities[identityIndex];
                        if (identity.Mismatch(ref sendBuffer))
                        {
                            int channel = identity.channel;
                            identities[identityIndex] = identity;

                            __ModifyChannel(NetworkRelayServerChannelModifier.Type.Mismatch, channel, channel,
                                sendBuffer.ID, ref sendBuffer);
                        }
                    }
                    break;
                case NetworkRelayMessageType.Query:
                    {
                        int channel = reader.ReadPackedInt(streamCompressionModel), identityIndex;
                        NetworkRelayServerIdentity identity;
                        foreach (var id in channelIDs.GetValuesForKey(channel))
                        {
                            identityIndex = sendBuffer.GetChannelIndex(id);
                            identity = identities[identityIndex];
                            identity.SendHeader((int)NetworkRelayMessageType.Query,
                                ref sendBuffer);
                        }
                    }
                    break;
                default:
                    {
                        var identityIndex = sendBuffer.channelIndex;
                        var identity = identities[identityIndex];

                        NetworkRelayType relayType = (NetworkRelayType)reader.ReadPackedInt(streamCompressionModel);

                        identity.Relay(type, relayType, ref reader,
                            ref sendBuffer);
                    }
                    break;
            }
        }

        private void __ModifyChannel(NetworkRelayServerChannelModifier.Type type, int source, int destination, uint id,
            ref NetworkServerSendBuffer.Identity sendBuffer)
        {
            /*if (id != sendBuffer.ID)
            {
                int identityIndex = sendBuffer.GetChannelIndex(id);
                if (identityIndex == -1)
                    return;

                var channelIdentity = identities[identityIndex];
                source = channelIdentity.channel;
                if (source != NetworkRelayServerIdentity.CHANNEL_NULL)
                {
                    if (!channelIdentity.isOnline)
                    {
                        channelIdentity.SendHeader((int)type,
                            ref sendBuffer);
                        channelIdentity.SendHeader(source, (int)type,
                            ref sendBuffer);
                    }
                }
            }*/

            NetworkRelayServerChannelModifier channelModifier;
            channelModifier.type = type;
            channelModifier.source = source;
            channelModifier.destination = destination;
            channelModifier.id = id;
            channelModifiers.Enqueue(channelModifier);
        }

        private void __SendChannelJoins(int channel, ref NetworkServerSendBuffer.Identity sendBuffer)
        {
            int identityIndex;
            NetworkRelayServerIdentity identity;
            foreach (var id in channelIDs.GetValuesForKey(channel))
            {
                if (id == sendBuffer.ID)
                    continue;

                identityIndex = sendBuffer.GetChannelIndex(id);
                identity = identities[identityIndex];
                identity.SendHeader((int)NetworkRelayMessageType.Join,
                    ref sendBuffer);
            }
        }
    }

    public struct NetworkRelayServer : IComponentData
    {
        [BurstCompile]
        private struct Match : IJobParallelForDefer
        {
            public double time;

            public NetworkServerSendBuffer.ReadOnly sendBuffer;

            [ReadOnly]
            public NativeList<NetworkRelayServerIdentity> identities;

            [ReadOnly]
            public NativeList<NetworkRelayServerMatch> matches;

            [ReadOnly]
            public NativeList<uint> matchIDs;

            [ReadOnly]
            public NativeParallelMultiHashMap<int, uint> matchDistanceIDs;

            [ReadOnly]
            public NativeParallelMultiHashMap<int, uint> channelIDs;

            public NativeQueue<NetworkRelayServerChannelModifier>.ParallelWriter channelModifiers;

            public void Execute(int index)
            {
                NetworkRelayServerChannelModifier channelModifier;

                uint id = matchIDs[index];
                if (!__CanMatch(true, id, out int identityIndex, out channelModifier.source))
                {
                    channelModifier.type = NetworkRelayServerChannelModifier.Type.Mismatch;
                    channelModifier.destination = NetworkRelayServerIdentity.CHANNEL_NULL;
                    channelModifier.id = id;
                    channelModifiers.Enqueue(channelModifier);

                    return;
                }

                var match = matches[identityIndex];

                int distance = (int)math.ceil((time - match.startTime) / match.value.distanceTime), playerCount = 1, channelPlayerCount, channel;
                for (int i = 0; i < distance; ++i)
                {
                    foreach (var distanceID in matchDistanceIDs.GetValuesForKey(match.value.distance + i))
                    {
                        if (distanceID == id || !__CanMatch(true, distanceID, out _, out channel))
                            continue;

                        channelPlayerCount = 0;
                        foreach (uint channelID in channelIDs.GetValuesForKey(channel))
                        {
                            if (channelID == id || !__CanMatch(false, channelID, out _, out _))
                                continue;

                            if (++channelPlayerCount + playerCount >= match.value.playerCount)
                                break;
                        }

                        playerCount += channelPlayerCount > 0 ? channelPlayerCount : 1;
                        if (playerCount >= match.value.playerCount)
                            break;
                    }

                    if (playerCount >= match.value.playerCount)
                        break;
                }

                if (playerCount < match.value.playerCount)
                    return;

                channelModifier.type = NetworkRelayServerChannelModifier.Type.Match;
                channelModifier.id = id;
                channelModifier.destination = identityIndex;
                channelModifiers.Enqueue(channelModifier);
                
                playerCount = 1;

                NetworkRelayServerChannelModifier drop;
                drop.type =  NetworkRelayServerChannelModifier.Type.Drop;
                drop.destination = NetworkRelayServerIdentity.CHANNEL_NULL;

                for (int i = 0; i < distance; ++i)
                {
                    foreach (var distanceID in matchDistanceIDs.GetValuesForKey(match.value.distance + i))
                    {
                        if (distanceID == id || !__CanMatch(true, distanceID, out _, out channel))
                            continue;

                        channelPlayerCount = 0;
                        foreach (uint channelID in channelIDs.GetValuesForKey(channel))
                        {
                            if (channelID == id || __CanMatch(false, channelID, out _, out drop.source))
                            {
                                channelModifier.id = channelID;
                                channelModifiers.Enqueue(channelModifier);
                                
                                if (++channelPlayerCount + playerCount >= match.value.playerCount)
                                    break;
                            }
                            else
                            {
                                drop.id = channelID;
                                channelModifiers.Enqueue(drop);
                            }
                        }

                        if (channelPlayerCount == 0)
                        {
                            channelModifier.id = distanceID;
                            channelModifiers.Enqueue(channelModifier);

                            ++playerCount;
                        }
                        else
                            playerCount += channelPlayerCount;
                        
                        if (playerCount >= match.value.playerCount)
                            break;
                    }
                }
            }

            private bool __CanMatch(bool isMain, uint id, out int identityIndex, out int channel)
            {
                if (sendBuffer.GetConnection(id, out int connectionIndex, out identityIndex, out _) &&
                    connectionIndex != -1)
                {
                    var identity = identities[identityIndex];
                    channel = (identity.channelFlag & NetworkRelayChannelFlag.Creator) ==
                              NetworkRelayChannelFlag.Creator
                        ? identity.channel
                        : NetworkRelayServerIdentity.CHANNEL_NULL;

                    return (!isMain || identity.match != 0) && identity.canMatch;
                }

                channel = NetworkRelayServerIdentity.CHANNEL_NULL;

                return false;
            }
        }
        
        [BurstCompile]
        private struct ModifyChannels : IJob
        {
            public NetworkServerSendBuffer.Concurrent sendBuffer;

            public NativeQueue<NetworkRelayServerChannelModifier> channelModifiers;
            public NativeParallelMultiHashMap<int, uint> channelIDs;

            public NativeList<NetworkRelayServerIdentity> identities;

            public NativeList<NetworkRelayServerChannel> channels;
            
            [ReadOnly]
            public NativeList<NetworkRelayServerMatch> matches;

            public NativeList<uint> matchIDs;

            public NativeArray<int> matchCount;

            public NativeParallelMultiHashMap<int, uint> matchDistanceIDs;

            public void Execute()
            {
                int numChannelModifiers = this.channelModifiers.Count;
                if (numChannelModifiers < 1)
                    return;
                
                int index = 0;
                NetworkRelayServerChannelModifier channelModifier;
                var channelModifiers =
                    new NativeArray<NetworkRelayServerChannelModifier>(numChannelModifiers, Allocator.Temp);
                while (this.channelModifiers.TryDequeue(out channelModifier))
                    channelModifiers[index++] = channelModifier;
                
                bool isContains;
                uint id;
                UnsafeHashMap<int, int> matches = default;
                for(int i = 0; i < numChannelModifiers; ++i)
                {
                    channelModifier = channelModifiers[i];
                    switch(channelModifier.type)
                    {
                        case NetworkRelayServerChannelModifier.Type.Drop:
                            {
                                bool result = false;
                                int identityIndex = sendBuffer.GetChannelIndex(channelModifier.id);
                                var identity = identities[identityIndex];
                                if (identity.channel == channelModifier.source)
                                {
                                    var sendBuffer = new NetworkServerSendBuffer.Identity(channelModifier.id, ref this.sendBuffer);
                                    if (!identity.isOnline)
                                    {
                                        foreach (uint channelID in channelIDs.GetValuesForKey(channelModifier.source))
                                        {
                                            if (identities[sendBuffer.GetChannelIndex(channelID)].isOnline)
                                            {
                                                sendBuffer =
                                                    new NetworkServerSendBuffer.Identity(channelID,
                                                        ref this.sendBuffer);

                                                break;
                                            }
                                        }
                                    }
                                    
                                    if (identity.Drop(ref sendBuffer))
                                    {
                                        channels.ElementAt(channelModifier.source).Leave();
                                        
                                        identities[identityIndex] = identity;

                                        result = true;
                                    }
                                }
                                
                                if(!result)
                                    continue;
                            }
                            break;
                        case NetworkRelayServerChannelModifier.Type.Matching:
                            {
                                int identityIndex = this.sendBuffer.GetChannelIndex(channelModifier.id);
                                var identity = identities[identityIndex];

                                channelModifier.source = identity.channel;
                                int matchIndex = matchIDs.IndexOf(channelModifier.id);
                                if (matchIndex == -1 && channelModifier.source == NetworkRelayServerIdentity.CHANNEL_NULL || 
                                    (identity.channelFlag & NetworkRelayChannelFlag.Creator) == NetworkRelayChannelFlag.Creator)
                                {
                                    matchIDs.Add(channelModifier.id);

                                    int distance = this.matches[identityIndex].value.distance;

                                    matchDistanceIDs.Add(distance, channelModifier.id);
                                }

                                var sendBuffer = new NetworkServerSendBuffer.Identity(channelModifier.id, ref this.sendBuffer);
                                if(identity.Matching(Interlocked.Increment(ref matchCount.AsSpan()[0]), ref sendBuffer))
                                    identities[identityIndex] = identity;
                            }

                            continue;
                        case NetworkRelayServerChannelModifier.Type.Match:
                        case NetworkRelayServerChannelModifier.Type.Mismatch:
                        {
                            int matchIndex = matchIDs.IndexOf(channelModifier.id);
                            
                            if (!matches.IsCreated)
                                matches = new UnsafeHashMap<int, int>(1, Allocator.Temp);

                            if (matches.TryGetValue(channelModifier.destination, out int match))
                            {
                                if(match != channelModifier.destination)
                                    continue;
                            }
                            else if(matchIndex == -1)
                                continue;
                            else
                            {
                                isContains = false;
                                NetworkRelayServerChannelModifier temp;
                                for (int j = i + 1; j < numChannelModifiers; ++j)
                                {
                                    temp = channelModifiers[j];
                                    switch (temp.type)
                                    {
                                        case NetworkRelayServerChannelModifier.Type.Match:
                                        case NetworkRelayServerChannelModifier.Type.Mismatch:
                                            if (temp.id == channelModifier.id &&
                                                matches.TryGetValue(channelModifier.destination, out match) &&
                                                match != channelModifier.destination)
                                                isContains = true;
                                            break;
                                    }

                                    if (isContains)
                                        break;
                                }
                                
                                if(isContains)
                                    continue;
                                
                                matches.Add(channelModifier.destination, channelModifier.destination);
                            }

                            int identityIndex = this.sendBuffer.GetChannelIndex(channelModifier.id);
                            var identity = identities[identityIndex];

                            if (matchIndex != -1)
                            {
                                matchIDs.RemoveAtSwapBack(matchIndex);

                                if (matchDistanceIDs.TryGetFirstValue(this.matches[identityIndex].value.distance, out id,
                                        out var iterator))
                                {
                                    do
                                    {
                                        if (id == channelModifier.id)
                                        {
                                            matchDistanceIDs.Remove(iterator);
                                            break;
                                        }
                                    } while (matchDistanceIDs.TryGetNextValue(out id, ref iterator));
                                }
                            }

                            var sendBuffer =
                                new NetworkServerSendBuffer.Identity(channelModifier.id, ref this.sendBuffer);
                            if (NetworkRelayServerChannelModifier.Type.Match == channelModifier.type)
                            {
                                var temp = this.matches[channelModifier.destination];
                                if (identity.Match(temp.index, temp.value.distance, ref sendBuffer))
                                {
                                    channelModifier.source = identity.channel;

                                    ref var channel = ref channels.ElementAt(channelModifier.destination);
                                    if (channelModifier.destination == identityIndex)
                                    {
                                        if(!identity.Create(channelModifier.destination, ref sendBuffer))
                                            channel.Leave();
                                        
                                        channel.Create(math.max(channel.capacity,
                                            this.matches[channelModifier.destination].value.playerCount));
                                    }
                                    else if (channel.Join(out _))
                                    {
                                        if(!identity.Join(true, channelModifier.destination, ref sendBuffer))
                                            channel.Leave();
                                    }

                                    channelModifier.destination = identity.channel;
                                    if (channelModifier.destination == channelModifier.source)
                                        continue;
                                    
                                    if(channelModifier.source != NetworkRelayServerIdentity.CHANNEL_NULL)
                                        channels.ElementAt(channelModifier.source).Leave();
                                    
                                    identities[identityIndex] = identity;
                                }
                                else
                                    continue;
                            }
                            else
                            {
                                if (identity.Mismatch(ref sendBuffer))
                                    identities[identityIndex] = identity;

                                continue;
                            }
                        }
                            break;
                    }

                    {
                        if (channelIDs.TryGetFirstValue(channelModifier.source, out id, out var iterator))
                        {
                            do
                            {
                                if (id == channelModifier.id)
                                {
                                    channelIDs.Remove(iterator);
                                    break;
                                }
                            }
                            while (channelIDs.TryGetNextValue(out id, ref iterator));
                        }
                    }

                    if(channelModifier.destination != NetworkRelayServerIdentity.CHANNEL_NULL)
                        channelIDs.Add(channelModifier.destination, channelModifier.id);
                }

                if (matches.IsCreated)
                    matches.Dispose();

                channelModifiers.Dispose();
                
#if DEBUG
                foreach (var pair in channelIDs)
                {
                    UnityEngine.Assertions.Assert.AreEqual(identities[sendBuffer.GetChannelIndex(pair.Value)].channel,
                        pair.Key);
                    
                    UnityEngine.Assertions.Assert.AreEqual(channelIDs.CountValuesForKey(pair.Key), channels[pair.Key].count);
                }

                foreach (var identity in identities)
                {
                    if (identity.channel != NetworkRelayServerIdentity.CHANNEL_NULL)
                    {
                        isContains = false;
                        foreach (var channelID in channelIDs.GetValuesForKey(identity.channel))
                        {
                            if (identity.ID == channelID)
                            {
                                isContains = true;
                                
                                break;
                            }
                        }
                        
                        UnityEngine.Assertions.Assert.IsTrue(isContains);
                    }
                }

                foreach (var pair in matchDistanceIDs)
                {
                    UnityEngine.Assertions.Assert.IsTrue(matchIDs.Contains(pair.Value));
                }
                
                UnityEngine.Assertions.Assert.AreEqual(matchIDs.Length, matchDistanceIDs.Count());
#endif
            }
        }

        private struct Scheduler : INetworkServerScheduler
        {
            public int innerloopBatchCount;
            public double time;
            public NetworkServerSendBuffer sendBuffer;
            public NativeQueue<NetworkRelayServerChannelModifier> channelModifiers;

            public NativeParallelMultiHashMap<int, uint> channelIDs;

            public NativeList<NetworkRelayServerChannel> channels;
            
            public NativeList<NetworkRelayServerIdentity> identities;

            public NativeList<NetworkRelayServerMatch> matches;

            public NativeList<uint> matchIDs;

            public NativeArray<int> matchCount;

            public NativeParallelMultiHashMap<int, uint> matchDistanceIDs;

            public JobHandle Schedule(in JobHandle dependsOn)
            {
                Match match;
                match.time = time;
                match.sendBuffer = sendBuffer.AsReadOnly();
                match.identities = identities;
                match.matches = matches;
                match.matchIDs = matchIDs;
                match.matchDistanceIDs = matchDistanceIDs;
                match.channelIDs = channelIDs;
                match.channelModifiers = channelModifiers.AsParallelWriter();

                var jobHandle = match.ScheduleByRef(matchIDs, innerloopBatchCount, dependsOn);

                ModifyChannels modifyChannels;
                modifyChannels.channelIDs = channelIDs;
                modifyChannels.channelModifiers = channelModifiers;
                modifyChannels.sendBuffer = sendBuffer.AsConcurrent();
                modifyChannels.identities = identities;
                modifyChannels.channels = channels;
                modifyChannels.matches = matches;
                modifyChannels.matchIDs = matchIDs;
                modifyChannels.matchCount = matchCount;
                modifyChannels.matchDistanceIDs = matchDistanceIDs;
                return modifyChannels.ScheduleByRef(jobHandle);
            }
        }

        public readonly NetworkPipeline Pipeline;

        private NetworkServer __instance;
        private NetworkServerSendBuffer __sendBuffer;

        private NativeList<NetworkRelayServerChannel> __channels;

        private NativeList<NetworkRelayServerIdentity> __identities;

        private NativeList<NetworkRelayServerMatch> __matches;

        private NativeList<uint> __matchIDs;

        private NativeArray<int> __matchCount;

        private NativeParallelMultiHashMap<int, uint> __matchDistanceIDs;

        private NativeParallelMultiHashMap<int, uint> __channelIDs;

        private NativeQueue<NetworkRelayServerChannelModifier> __channelModifiers;

        public NetworkRelayServer(
            in NetworkSettings settings,
            in NativeArray<NetworkPipelineStageId> stages,
            in AllocatorManager.AllocatorHandle allocator)
        {
            __instance = new NetworkServer(settings, allocator);

            __sendBuffer = new NetworkServerSendBuffer(allocator);

            __channels = new NativeList<NetworkRelayServerChannel>(allocator);
            __identities = new NativeList<NetworkRelayServerIdentity>(allocator);

            __matches = new NativeList<NetworkRelayServerMatch>(allocator);

            __matchIDs = new NativeList<uint>(allocator);

            __matchCount = CollectionHelper.CreateNativeArray<int>(1, allocator, NativeArrayOptions.ClearMemory);

            __matchDistanceIDs = new NativeParallelMultiHashMap<int, uint>(1, allocator);

            __channelIDs = new NativeParallelMultiHashMap<int, uint>(1, allocator);

            __channelModifiers = new NativeQueue<NetworkRelayServerChannelModifier>(allocator);

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
            __channels.Dispose();
            __identities.Dispose();
            __matches.Dispose();
            __matchIDs.Dispose();
            __matchCount.Dispose();
            __matchDistanceIDs.Dispose();
            __channelIDs.Dispose();
            __channelModifiers.Dispose();
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
            double time, 
            in JobHandle inputDeps)
        {
            NetworkRelayServerListener listener;
            listener.identities = __identities;
            listener.channels = __channels;
            listener.matches = __matches;

            NetworkRelayServerHandler handler;
            handler.time = time;
            handler.channelModifiers = __channelModifiers.AsParallelWriter();
            handler.channelIDs = __channelIDs;
            handler.channels = __channels.AsDeferredJobArray();
            handler.identities = __identities.AsDeferredJobArray();
            handler.matches = __matches.AsDeferredJobArray();
            handler.matchCount = __matchCount;

            Scheduler scheduler;
            scheduler.innerloopBatchCount = innerloopBatchCount;
            scheduler.time = time;
            scheduler.sendBuffer = __sendBuffer;
            scheduler.channelModifiers = __channelModifiers;
            scheduler.channelIDs = __channelIDs;
            scheduler.channels = __channels;
            scheduler.identities = __identities;
            scheduler.matches = __matches;
            scheduler.matchIDs = __matchIDs;
            scheduler.matchCount = __matchCount;
            scheduler.matchDistanceIDs = __matchDistanceIDs;

            return __instance.Schedule(ref listener, ref handler, ref scheduler, ref __sendBuffer, 
                innerloopBatchCount, Pipeline, 
                in inputDeps);
        }
    }
}
