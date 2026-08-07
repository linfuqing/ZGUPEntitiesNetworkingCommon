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
    public struct NetworkRelayServerModifier
    {
        public enum Type
        {
            Add = NetworkRelayMessageType.Add,
            Remove = NetworkRelayMessageType.Remove,
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

    public struct NetworkRelayServerInjectSingleton : IComponentData
    {
        public struct Inject
        {
            public uint id;
            public int byteOffset;
            public int byteCount;
        }

        public NativeList<Inject> injects;
        public NativeList<byte> bytes;
        public NativeHashMap<uint, NetworkPipeline> pipelines;

        public NetworkRelayServerInjectSingleton(in AllocatorManager.AllocatorHandle allocator)
        {
            injects = new NativeList<Inject>(allocator);
            bytes = new NativeList<byte>(allocator);
            pipelines = new NativeHashMap<uint, NetworkPipeline>(1, allocator);
        }

        public void Dispose()
        {
            injects.Dispose();
            bytes.Dispose();
            pipelines.Dispose();
        }
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
            if (slot < 0 || slot + 1 == capacity)
            {
                Interlocked.Increment(ref  __slot);

                return false;
            }

            return true;
        }
    }

    public struct NetworkRelayServerListener : INetworkServerListener
    {
        public NativeList<NetworkRelayServerIdentity> identities;

        public NativeList<NetworkRelayServerChannel> channels;

        public NativeList<NetworkRelayServerMatch> matches;

        public NativeQueue<NetworkRelayServerModifier> modifiers;

        public void Connect(uint id, 
            //int connectionIndex, 
            //int channelIndex, 
            in NativeArray<byte> payload, 
            ref NetworkServerSendBuffer sendBuffer)
        {
            sendBuffer.AsReadOnly().GetConnection(id, out _, out int channelIndex, out _);
            
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

        public void Disconnect(uint id, ref NetworkServerSendBuffer sendBuffer)
        {
            //identityIndices.Remove(connection);
        }

        public void Reconnect(uint id, ref NetworkServerSendBuffer sendBuffer)
        {
            var sendBufferIdentity = new NetworkServerSendBuffer.Identity(id, ref sendBuffer);
            var identityIndex = sendBufferIdentity.channelIndex;
            var identity = identities[identityIndex];
            int source = identity.channel;
            identity.Disconnect(sendBufferIdentity.ID, identities.AsArray(), ref sendBufferIdentity);
            
            if (source != NetworkRelayServerIdentity.CHANNEL_NULL && channels.ElementAt(source).count == 1)
                identity.Leave(sendBufferIdentity.ID, ref sendBufferIdentity);
            
            int destination = identity.channel;
            if (destination != source)
            {
                if (source != NetworkRelayServerIdentity.CHANNEL_NULL)
                    channels.ElementAt(source).Leave();

                if (destination != NetworkRelayServerIdentity.CHANNEL_NULL)
                    channels.ElementAt(destination).Join(out _);

                __Modify(
                    destination == NetworkRelayServerIdentity.CHANNEL_NULL
                        ? NetworkRelayServerModifier.Type.Leave
                        : NetworkRelayServerModifier.Type.Join,
                    source,
                    destination,
                    id);
            }
            identities[identityIndex] = identity;
        }
        
        private void __Modify(NetworkRelayServerModifier.Type type, int source, int destination, uint id)
        {
            NetworkRelayServerModifier modifier;
            modifier.type = type;
            modifier.source = source;
            modifier.destination = destination;
            modifier.id = id;
            modifiers.Enqueue(modifier);
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

        public NativeQueue<NetworkRelayServerModifier>.ParallelWriter modifiers;

        public void Connect(ref NetworkServerSendBuffer.ParallelIdentity sendBuffer)
        {
            var identityIndex = sendBuffer.channelIndex;
            var identity = identities[identityIndex];
            identity.Connect(sendBuffer.ID, identities, channelIDs, ref sendBuffer);
            identities[identityIndex] = identity;
        }

        public void Disconnect(ref NetworkServerSendBuffer.ParallelIdentity sendBuffer)
        {
            var identityIndex = sendBuffer.channelIndex;
            var identity = identities[identityIndex];
            int source = identity.channel;
            identity.Disconnect(sendBuffer.ID, identities, ref sendBuffer);
            
            if (source != NetworkRelayServerIdentity.CHANNEL_NULL && NetworkRelayServerChannel.ElementAt(ref channels, source).count == 1)
                identity.Leave(sendBuffer.ID, ref sendBuffer);

            int destination = identity.channel;
            if (destination != source)
            {
                if (source != NetworkRelayServerIdentity.CHANNEL_NULL)
                    NetworkRelayServerChannel.ElementAt(ref channels, source).Leave();

                if (destination != NetworkRelayServerIdentity.CHANNEL_NULL)
                    NetworkRelayServerChannel.ElementAt(ref channels, destination).Join(out _);

                __Modify(NetworkRelayServerModifier.Type.Leave, source, destination,
                    sendBuffer.ID, ref sendBuffer);
            }
            
            identities[identityIndex] = identity;
        }

        public void Read(ref DataStreamReader reader,
            ref NetworkServerSendBuffer.ParallelIdentity sendBuffer)
        {
            var streamCompressionModel = StreamCompressionModel.Default;
            int type = reader.ReadPackedInt(streamCompressionModel);
            //UnityEngine.Debug.Log($"NetworkRelayServerRead type={type} id={sendBuffer.ID} ch={sendBuffer.channelIndex}");
            switch ((NetworkRelayMessageType)type)
            {
                case NetworkRelayMessageType.Status:
                    {
                        var identityIndex = sendBuffer.channelIndex;
                        var identity = identities[identityIndex];

                        int source = identity.channel;
                        if (identity.SetStatus(
                                source != NetworkRelayServerIdentity.CHANNEL_NULL &&
                                NetworkRelayServerChannel.ElementAt(ref channels, source).count == 1,
                                reader.ReadPackedInt(streamCompressionModel),
                                sendBuffer.ID,
                                identities,
                                ref sendBuffer))
                        {
                            int destination = identity.channel;
                            if (destination != source)
                            {
                                if (source != NetworkRelayServerIdentity.CHANNEL_NULL)
                                    NetworkRelayServerChannel.ElementAt(ref channels, source).Leave();

                                if (destination != NetworkRelayServerIdentity.CHANNEL_NULL)
                                    NetworkRelayServerChannel.ElementAt(ref channels, source).Join(out _);

                                __Modify(NetworkRelayServerModifier.Type.Leave, source, destination,
                                    sendBuffer.ID, ref sendBuffer);
                            }

                            identities[identityIndex] = identity;
                        }
                    }
                    break;
                case NetworkRelayMessageType.Add:
                {
                    uint id = reader.ReadPackedUInt(streamCompressionModel);
                    int channelIndex = id == sendBuffer.ID ? -1 : sendBuffer.GetChannelIndex(id);
                    if (channelIndex != -1)
                    {
                        var identityIndex = sendBuffer.channelIndex;
                        var identity = identities[identityIndex];
                        if (identity.AddFriend(id, ref sendBuffer))
                        {
                            __Modify(NetworkRelayServerModifier.Type.Add, identityIndex, channelIndex,
                                sendBuffer.ID, ref sendBuffer);

                            identities[identityIndex] = identity;
                        }
                    }
                }
                    break;
                case NetworkRelayMessageType.Remove:
                {
                    uint id = reader.ReadPackedUInt(streamCompressionModel);
                    int channelIndex = id == sendBuffer.ID ? -1 : sendBuffer.GetChannelIndex(id);
                    if (channelIndex != -1)
                    {
                        var identityIndex = sendBuffer.channelIndex;
                        var identity = identities[identityIndex];
                        if (identity.RemoveFriend(id, ref sendBuffer))
                        {
                            __Modify(NetworkRelayServerModifier.Type.Remove, identityIndex, channelIndex,
                                sendBuffer.ID, ref sendBuffer);
                            
                            identities[identityIndex] = identity;
                        }
                    }
                }
                    break;
                case NetworkRelayMessageType.Create:
                    {
                        var identityIndex = sendBuffer.channelIndex;
                        var identity = identities[identityIndex];
                        int channel = identity.channel;
                        bool isCreator = (identity.channelFlag &
                                          NetworkRelayChannelFlag.Creator) == NetworkRelayChannelFlag.Creator, 
                            result = identity.Create(
                                false, 
                                sendBuffer.ID, 
                                //sendBuffer.channelIndex,
                                identities, 
                                channelIDs, 
                                ref sendBuffer);
                        int targetChannel = identity.channel;
                        if (result)
                        {
                            NetworkRelayServerChannel.ElementAt(ref channels, targetChannel)
                                .Create(reader.ReadPackedInt(streamCompressionModel));

                            //__SendChannelJoins(targetChannel, ref sendBuffer);

                            __Modify(NetworkRelayServerModifier.Type.Create, channel, targetChannel,
                                sendBuffer.ID, ref sendBuffer);
                        }

                        if(__CreateOrJoin(result, isCreator, channel, targetChannel, ref sendBuffer))
                            identities[identityIndex] = identity;
                    }
                    break;
                case NetworkRelayMessageType.Join:
                {
                    int targetChannelIndex = reader.ReadPackedInt(streamCompressionModel);
                    if (targetChannelIndex >= 0 && targetChannelIndex < channels.Length)
                    {
                        bool result = false;
                        if (channelIDs.ContainsKey(targetChannelIndex))
                        {
                            var identityIndex = sendBuffer.channelIndex;
                            var identity = identities[identityIndex];
                            int channelIndex = identity.channel;
                            bool isCreator = (identity.channelFlag &
                                              NetworkRelayChannelFlag.Creator) == NetworkRelayChannelFlag.Creator;
                            ref var channel = ref NetworkRelayServerChannel
                                .ElementAt(ref channels, targetChannelIndex);
                            if (channel.Join(out _))
                            {
                                if (identity.Join(
                                        false,
                                        targetChannelIndex,
                                        sendBuffer.ID, 
                                        identities,
                                        channelIDs,
                                        ref sendBuffer))
                                {
                                    //__SendChannelJoins(targetChannelIndex, ref sendBuffer);

                                    __Modify(NetworkRelayServerModifier.Type.Join, channelIndex,
                                        targetChannelIndex,
                                        sendBuffer.ID, ref sendBuffer);

                                    result = true;
                                }
                                else
                                    channel.Leave();
                            }

                            if (__CreateOrJoin(result, isCreator, channelIndex, identity.channel, ref sendBuffer))
                                identities[identityIndex] = identity;
                        }

                        if (!result)
                        {
                            if (sendBuffer.BeginWrite(sendBuffer.ID, out var writer,
                                    (ushort)(2 * UnsafeUtility.SizeOf<int>())))
                            {
                                writer.WritePackedInt((int)NetworkRelayMessageType.JoinFailed, streamCompressionModel);
                                writer.WritePackedInt(targetChannelIndex, streamCompressionModel);

                                sendBuffer.EndWrite(writer);
                            }
                        }
                    }
                }
                    break;
                case NetworkRelayMessageType.Leave:
                    {
                        var identityIndex = sendBuffer.channelIndex;
                        var identity = identities[identityIndex];
                        int channel = identity.channel;
                        if (identity.Leave(sendBuffer.ID,  ref sendBuffer))
                        {
                            NetworkRelayServerChannel
                                .ElementAt(ref channels, channel).Leave();

                            identities[identityIndex] = identity;

                            __Modify(NetworkRelayServerModifier.Type.Leave, channel,
                                identity.channel, sendBuffer.ID,
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
                            __Modify(NetworkRelayServerModifier.Type.Drop, channel,
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
                        int channel = identity.channel;
                        bool isLeave = false;
                        if (channel != NetworkRelayServerIdentity.CHANNEL_NULL &&
                            channelIDs.CountValuesForKey(channel) == 1)
                        {
                            isLeave = identity.Leave(sendBuffer.ID, ref sendBuffer);
                            if(isLeave)
                                NetworkRelayServerChannel.ElementAt(ref channels, channel).Leave();
                        }
                        
                        if (identity.Matching(Interlocked.Increment(ref matchCount.AsSpan()[0]), sendBuffer.ID, ref sendBuffer))
                        {
                            //UnityEngine.Debug.Log("Matching ID: " + identity.ID);
                            
                            NetworkRelayServerMatch match;
                            match.index = identity.match;
                            match.startTime = time;
                            match.value = new NetworkRelayMatch(ref reader, streamCompressionModel);
                            matches[identityIndex] = match;

                            identities[identityIndex] = identity;
                            
                            __Modify(NetworkRelayServerModifier.Type.Matching, channel, identity.channel,
                                sendBuffer.ID, ref sendBuffer);
                        }
                        else if(isLeave)
                            __Modify(NetworkRelayServerModifier.Type.Leave, channel, identity.channel,
                                sendBuffer.ID, ref sendBuffer);
                    }
                    break;
                case NetworkRelayMessageType.Mismatch:
                    {
                        var identityIndex = sendBuffer.channelIndex;
                        var identity = identities[identityIndex];
                        if (identity.Mismatch(sendBuffer.ID, ref sendBuffer))
                        {
                            int channel = identity.channel;
                            identities[identityIndex] = identity;

                            __Modify(NetworkRelayServerModifier.Type.Mismatch, channel, channel,
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
                                sendBuffer.ID, ref sendBuffer);
                        }
                    }
                    break;
                default:
                    {
                        var identityIndex = sendBuffer.channelIndex;
                        var identity = identities[identityIndex];

                        NetworkRelayType relayType = (NetworkRelayType)reader.ReadPackedInt(streamCompressionModel);

                        identity.Relay(type, relayType, sendBuffer.ID, ref reader,
                            ref sendBuffer);
                    }
                    break;
            }
        }

        private bool __CreateOrJoin(
            bool result,
            bool isCreator,
            int channel,
            int targetChannel,
            ref NetworkServerSendBuffer.ParallelIdentity sendBuffer)
        {
            if (targetChannel == channel)
                return false;

            if (channel != NetworkRelayServerIdentity.CHANNEL_NULL)
                NetworkRelayServerChannel
                    .ElementAt(ref channels, channel).Leave();

            if (!result)
                __Modify(NetworkRelayServerModifier.Type.Leave, channel, targetChannel,
                    sendBuffer.ID, ref sendBuffer);

            if (isCreator)
            {
                foreach (uint channelID in channelIDs.GetValuesForKey(channel))
                {
                    if (channelID == sendBuffer.ID)
                        continue;

                    __Modify(NetworkRelayServerModifier.Type.Drop, 
                        channel,
                        NetworkRelayServerIdentity.CHANNEL_NULL,
                        channelID,
                        ref sendBuffer);
                }
            }

            return true;
        }

        private void __Modify(NetworkRelayServerModifier.Type type, int source, int destination, uint id,
            ref NetworkServerSendBuffer.ParallelIdentity sendBuffer)
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

            NetworkRelayServerModifier modifier;
            modifier.type = type;
            modifier.source = source;
            modifier.destination = destination;
            modifier.id = id;
            modifiers.Enqueue(modifier);
        }

        /*private void __SendChannelJoins(int channel, ref NetworkServerSendBuffer.Identity sendBuffer)
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
        }*/
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
            public NativeList<int> matchDistances;

            [ReadOnly]
            public NativeParallelMultiHashMap<int, uint> matchDistanceIDs;

            [ReadOnly]
            public NativeParallelMultiHashMap<int, uint> channelIDs;

            public NativeQueue<NetworkRelayServerModifier>.ParallelWriter modifiers;

            public void Execute(int index)
            {
                NetworkRelayServerModifier modifier;

                uint id = matchIDs[index];
                if (!__CanMatch(true, id, out int identityIndex, out modifier.source))
                {
                    modifier.type = NetworkRelayServerModifier.Type.Mismatch;
                    modifier.destination = NetworkRelayServerIdentity.CHANNEL_NULL;
                    modifier.id = id;
                    modifiers.Enqueue(modifier);

                    return;
                }

                var match = matches[identityIndex];
                int distanceIndex = matchDistances.BinarySearch(match.value.distance);
                if (distanceIndex == -1)
                    return;

                int length = matchDistances.Length, 
                    step = math.min((int)math.ceil((time - match.startTime) / match.value.distanceTime), 
                        length), 
                    x = distanceIndex, 
                    y = distanceIndex - 1, 
                    playerCount = 1, 
                    channelPlayerCount, 
                    channel, 
                    temp;
                for (int i = 0; i < step; ++i)
                {
                    if (!__StepDistanceIndex(i, length, ref x, ref y, out temp))
                        break;

                    foreach (var distanceID in matchDistanceIDs.GetValuesForKey(matchDistances[temp]))
                    {
                        if (/*distanceID == id || */!__CanMatch(true, distanceID, out _, out channel))
                            continue;

                        channelPlayerCount = 0;
                        foreach (uint channelID in channelIDs.GetValuesForKey(channel))
                        {
                            if (channelID == id || !__CanMatch(false, channelID, out _, out _))
                                continue;

                            if (++channelPlayerCount + playerCount > match.value.playerCount)
                                break;
                        }

                        if (channelPlayerCount > 0)
                        {
                            if(channelPlayerCount + playerCount > match.value.playerCount)
                                continue;
                            
                            playerCount += channelPlayerCount;
                        }
                        else if(distanceID != id)
                            ++playerCount;
                        
                        if (playerCount >= match.value.playerCount)
                            break;
                    }

                    if (playerCount >= match.value.playerCount)
                        break;
                }

                if (playerCount < match.value.playerCount)
                    return;

                modifier.type = NetworkRelayServerModifier.Type.Match;
                modifier.id = id;
                modifier.destination = identityIndex;
                modifiers.Enqueue(modifier);
                
                playerCount = 1;

                NetworkRelayServerModifier drop;
                drop.type =  NetworkRelayServerModifier.Type.Drop;
                drop.destination = NetworkRelayServerIdentity.CHANNEL_NULL;

                x = distanceIndex;
                y = distanceIndex - 1;
                for (int i = 0; i < step; ++i)
                {
                    if (!__StepDistanceIndex(i, length, ref x, ref y, out temp))
                        break;

                    foreach (var distanceID in matchDistanceIDs.GetValuesForKey(matchDistances[temp]))
                    {
                        if (/*distanceID == id || */!__CanMatch(true, distanceID, out _, out channel))
                            continue;

                        channelPlayerCount = 0;
                        foreach (uint channelID in channelIDs.GetValuesForKey(channel))
                        {
                            if (channelID == id || !__CanMatch(false, channelID, out _, out _))
                                continue;

                            if (++channelPlayerCount + playerCount > match.value.playerCount)
                                break;
                        }

                        if (channelPlayerCount > 0)
                        {
                            if(channelPlayerCount + playerCount > match.value.playerCount)
                                continue;

                            channelPlayerCount = 0;
                            foreach (uint channelID in channelIDs.GetValuesForKey(channel))
                            {
                                if(channelID == id)
                                    continue;
                                
                                if (__CanMatch(false, channelID, out _, out drop.source) && 
                                    channelPlayerCount + playerCount < match.value.playerCount)
                                {
                                    modifier.id = channelID;
                                    modifiers.Enqueue(modifier);

                                    ++channelPlayerCount;
                                }
                                else
                                {
                                    drop.id = channelID;
                                    modifiers.Enqueue(drop);
                                }
                            }
                            
                            playerCount += channelPlayerCount;
                        }
                        else if (distanceID != id)
                        {
                            modifier.id = distanceID;
                            modifiers.Enqueue(modifier);

                            ++playerCount;
                        }
                        
                        if (playerCount >= match.value.playerCount)
                            break;
                    }
                }
            }

            private bool __StepDistanceIndex(int i, int length, ref int x, ref int y, out int result)
            {
                if ((i & 1) == 0)
                {
                    if (x < length)
                        result = x++;
                    else if (y < 0)
                    {
                        result = -1;

                        return false;
                    }
                    else
                        result = y--;
                }
                else if (y < 0)
                {
                    if (x < length)
                        result = x++;
                    else
                    {
                        result = -1;

                        return false;
                    }
                }
                else
                    result = y--;

                return true;
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
            public NetworkServerSendBuffer.Writer sendBuffer;

            public NativeQueue<NetworkRelayServerModifier> modifiers;
            public NativeParallelMultiHashMap<int, uint> channelIDs;

            public NativeList<NetworkRelayServerIdentity> identities;

            public NativeList<NetworkRelayServerChannel> channels;
            
            [ReadOnly]
            public NativeList<NetworkRelayServerMatch> matches;

            public NativeList<uint> matchIDs;

            public NativeList<int> matchDistances;

            public NativeArray<int> matchCount;

            public NativeParallelMultiHashMap<int, uint> matchDistanceIDs;

            public void Execute()
            {
                int numModifiers = this.modifiers.Count;
                if (numModifiers < 1)
                    return;
                
                NetworkRelayServerModifier modifier;
                var modifiers =
                    new NativeList<NetworkRelayServerModifier>(numModifiers, Allocator.Temp);
                while (this.modifiers.TryDequeue(out modifier))
                    modifiers.Add(modifier);
                
                bool isContains;
                uint id;
                UnsafeHashMap<int, int> matches = default;
                for(int i = 0; i < numModifiers; ++i)
                {
                    modifier = modifiers[i];
                    switch(modifier.type)
                    {
                        case NetworkRelayServerModifier.Type.Add:
                        {
                            var identity = identities[modifier.destination];
                            
                            var sendBuffer =
                                new NetworkServerSendBuffer.Identity(modifier.id, ref this.sendBuffer);
                            if (identity.AddFriend(modifier.id, ref sendBuffer))
                                identities[modifier.destination] = identity;
                        }
                            continue;
                        case NetworkRelayServerModifier.Type.Remove:
                        {
                            var identity = identities[modifier.destination];
                            
                            var sendBuffer =
                                new NetworkServerSendBuffer.Identity(modifier.id, ref this.sendBuffer);
                            if (identity.RemoveFriend(modifier.id, ref sendBuffer))
                                identities[modifier.destination] = identity;
                        }
                            continue;
                        case NetworkRelayServerModifier.Type.Create:
                        case NetworkRelayServerModifier.Type.Join:
                        case NetworkRelayServerModifier.Type.Leave:
                            if (modifier.source != NetworkRelayServerIdentity.CHANNEL_NULL)
                            {
                                if (sendBuffer.GetChannelIndex(modifier.id) == modifier.source)
                                    numModifiers += __Drop(i, modifier.source, modifier.id, ref modifiers);
                                else
                                {
                                    ref var channel = ref channels.ElementAt(modifier.source);
                                    if (channel.count == 1)
                                        numModifiers += __Drop(i, modifier.source, modifier.id, ref modifiers);
                                }
                            }

                            break;
                        case NetworkRelayServerModifier.Type.Drop:
                        {
                            /*UnityEngine.Assertions.Assert.AreEqual(NetworkRelayServerIdentity.CHANNEL_NULL,
                                modifier.destination);*/

                            if (!__Drop(modifier.id, modifier.source, out int identityIndex))
                                continue;

                            if (identityIndex == modifier.source)
                                numModifiers += __Drop(i, modifier.source, modifier.id, ref modifiers);
                            else
                            {
                                ref var channel = ref channels.ElementAt(modifier.source);
                                if(channel.count == 1)
                                    numModifiers += __Drop(i, modifier.source, modifier.id, ref modifiers);
                            }
                        }
                            break;
                        case NetworkRelayServerModifier.Type.Matching:
                            {
                                int identityIndex = this.sendBuffer.GetChannelIndex(modifier.id);
                                var identity = identities[identityIndex];

                                var sendBuffer = new NetworkServerSendBuffer.Identity(modifier.id, ref this.sendBuffer);
                                
                                int matchIndex = matchIDs.IndexOf(modifier.id);
                                if (matchIndex == -1 && identity.channel == NetworkRelayServerIdentity.CHANNEL_NULL || 
                                    (identity.channelFlag & NetworkRelayChannelFlag.Creator) == NetworkRelayChannelFlag.Creator)
                                {
                                    matchIDs.Add(modifier.id);

                                    int distance = this.matches[identityIndex].value.distance;

                                    if (!matchDistanceIDs.ContainsKey(distance))
                                    {
                                        matchDistances.Add(distance);
                                        
                                        matchDistances.Sort();
                                    }

                                    matchDistanceIDs.Add(distance, modifier.id);
                                    
                                    if(identity.Matching(Interlocked.Increment(ref matchCount.AsSpan()[0]), sendBuffer.ID, ref sendBuffer))
                                        identities[identityIndex] = identity;
                                }
                                else if(identity.Mismatch(modifier.id, ref sendBuffer))
                                    identities[identityIndex] = identity;
                            }

                            if(modifier.source == modifier.destination)
                                continue;
                            
                            break;
                        case NetworkRelayServerModifier.Type.Match:
                        case NetworkRelayServerModifier.Type.Mismatch:
                        {
                            int matchIndex = matchIDs.IndexOf(modifier.id);
                            
                            if (!matches.IsCreated)
                                matches = new UnsafeHashMap<int, int>(1, Allocator.Temp);

                            if (matches.TryGetValue(modifier.destination, out int match))
                            {
                                if(match != modifier.destination)
                                    continue;
                            }
                            else if(matchIndex == -1)
                                continue;
                            else
                            {
                                isContains = false;
                                NetworkRelayServerModifier temp;
                                for (int j = i + 1; j < numModifiers; ++j)
                                {
                                    temp = modifiers[j];
                                    switch (temp.type)
                                    {
                                        case NetworkRelayServerModifier.Type.Match:
                                        case NetworkRelayServerModifier.Type.Mismatch:
                                            if (temp.id == modifier.id &&
                                                matches.TryGetValue(modifier.destination, out match) &&
                                                match != modifier.destination)
                                                isContains = true;
                                            break;
                                    }

                                    if (isContains)
                                        break;
                                }
                                
                                if(isContains)
                                    continue;
                                
                                matches.Add(modifier.destination, modifier.destination);
                            }

                            int identityIndex = this.sendBuffer.GetChannelIndex(modifier.id);
                            var identity = identities[identityIndex];

                            if (matchIndex != -1)
                            {
                                matchIDs.RemoveAtSwapBack(matchIndex);

                                int distance = this.matches[identityIndex].value.distance;
                                if (matchDistanceIDs.TryGetFirstValue(distance, out id,
                                        out var iterator))
                                {
                                    do
                                    {
                                        if (id == modifier.id)
                                        {
                                            matchDistanceIDs.Remove(iterator);
                                            
                                            if(!matchDistanceIDs.ContainsKey(distance))
                                                matchDistances.RemoveAt(matchDistances.IndexOf(distance));
                                            
                                            break;
                                        }
                                    } while (matchDistanceIDs.TryGetNextValue(out id, ref iterator));
                                }
                            }

                            var sendBuffer =
                                new NetworkServerSendBuffer.Identity(modifier.id, ref this.sendBuffer);
                            if (NetworkRelayServerModifier.Type.Match == modifier.type)
                            {
                                var temp = this.matches[modifier.destination];
                                if (modifier.destination != identityIndex || temp.index == identity.match)
                                {
                                    identity.Match(temp.index, temp.value.distance, sendBuffer.ID, ref sendBuffer);

                                    modifier.source = identity.channel;

                                    ref var channel = ref channels.ElementAt(modifier.destination);
                                    if (modifier.destination == identityIndex)
                                    {
                                        if (!identity.Create( //channelModifier.destination, 
                                                true,
                                                sendBuffer.ID,
                                                identities.AsArray(),
                                                channelIDs,
                                                ref sendBuffer))
                                        {
                                            //numModifiers += __Drop(i, modifier.destination, modifier.id, ref modifiers);

                                            channel.Leave();

                                            if (channel.count == 0)
                                                identity.SetTemp();
                                        }

                                        channel.Create(math.max(channel.capacity,
                                            this.matches[modifier.destination].value.playerCount));
                                    }
                                    else if (channel.Join(out _))
                                    {
                                        if (!identity.Join(true,
                                                modifier.destination,
                                                sendBuffer.ID,
                                                identities.AsArray(),
                                                channelIDs,
                                                ref sendBuffer))
                                            channel.Leave();
                                    }

                                    identities[identityIndex] = identity;

                                    modifier.destination = identity.channel;
                                    if (modifier.destination == modifier.source)
                                        continue;

                                    if (modifier.source != NetworkRelayServerIdentity.CHANNEL_NULL)
                                        channels.ElementAt(modifier.source).Leave();

                                    foreach (var channelID in channelIDs.GetValuesForKey(modifier.destination))
                                    {
                                        if (channelID == sendBuffer.ID)
                                            continue;

                                        identities[sendBuffer.GetChannelIndex(channelID)].SendHeader(
                                            (int)NetworkRelayMessageType.Join,
                                            sendBuffer.ID,
                                            ref sendBuffer);
                                    }
                                }
                            }
                            else
                            {
                                if (identity.Mismatch(sendBuffer.ID, ref sendBuffer))
                                    identities[identityIndex] = identity;

                                continue;
                            }
                        }
                            break;
                    }

                    {
                        if (channelIDs.TryGetFirstValue(modifier.source, out id, out var iterator))
                        {
                            do
                            {
                                if (id == modifier.id)
                                {
                                    channelIDs.Remove(iterator);
                                    break;
                                }
                            }
                            while (channelIDs.TryGetNextValue(out id, ref iterator));
                        }
                    }

                    if(modifier.destination != NetworkRelayServerIdentity.CHANNEL_NULL)
                        channelIDs.Add(modifier.destination, modifier.id);
                }

                if (matches.IsCreated)
                    matches.Dispose();

                modifiers.Dispose();
                
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

            private bool __Drop(uint id, int source, out int identityIndex)
            {
                identityIndex = sendBuffer.GetChannelIndex(id);
                
                var identity = identities[identityIndex];
                if (identity.channel == source)
                {
                    var sendBuffer =
                        new NetworkServerSendBuffer.Identity(id, ref this.sendBuffer);
                    if (!identity.isOnline)
                    {
                        foreach (uint channelID in channelIDs.GetValuesForKey(source))
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

                    if (identity.Drop(sendBuffer.ID, ref sendBuffer))
                    {
                        channels.ElementAt(source).Leave();

                        identities[identityIndex] = identity;

                        return true;
                    }
                }

                return false;
            }

            private int __Drop(
                int index, 
                int channel, 
                uint id, 
                ref NativeList<NetworkRelayServerModifier> modifiers)
            {
                int numChannelIDs = channelIDs.CountValuesForKey(channel) - 1;
                if (numChannelIDs > 0)
                {
                    modifiers.InsertRange(++index, numChannelIDs);
                 
                    NetworkRelayServerModifier drop;
                    drop.type = NetworkRelayServerModifier.Type.Drop;
                    drop.source = channel;
                    drop.destination = NetworkRelayServerIdentity.CHANNEL_NULL;
                    foreach (uint channelID in channelIDs.GetValuesForKey(channel))
                    {
                        if (channelID == id)
                            continue;

                        drop.id = channelID;

                        modifiers[index++] = drop;
                    }
                }

                return numChannelIDs;
            }
        }

        internal struct InjectGroup
        {
            public uint id;
            public int offset;
            public int count;
        }

        [BurstCompile]
        internal struct BuildInjectGroups : IJob
        {
            [ReadOnly]
            public NativeList<NetworkRelayServerInjectSingleton.Inject> injects;

            public NativeList<InjectGroup> groups;
            public NativeList<int> groupedInjectIndices;
            public NativeHashMap<uint, int> groupIndices;

            public void Execute()
            {
                groups.Clear();
                groupedInjectIndices.Clear();
                groupIndices.Clear();

                int injectCount = injects.Length;
                if (injectCount < 1)
                    return;

                groups.Capacity = math.max(groups.Capacity, injectCount);
                groupedInjectIndices.ResizeUninitialized(injectCount);
                groupIndices.Capacity = math.max(groupIndices.Capacity, injectCount);

                int groupIndex;
                InjectGroup group;
                for (int i = 0; i < injectCount; ++i)
                {
                    uint id = injects[i].id;
                    if (groupIndices.TryGetValue(id, out groupIndex))
                    {
                        group = groups[groupIndex];
                        ++group.count;
                        groups[groupIndex] = group;
                    }
                    else
                    {
                        groupIndex = groups.Length;
                        groupIndices.Add(id, groupIndex);
                        groups.Add(new InjectGroup
                        {
                            id = id,
                            offset = 0,
                            count = 1
                        });
                    }
                }

                int offset = 0;
                int groupCount = groups.Length;
                for (int i = 0; i < groupCount; ++i)
                {
                    group = groups[i];
                    group.offset = offset;
                    groups[i] = group;
                    offset += group.count;
                }

                // Use offset as a temporary write cursor. Scanning injects in their original order
                // preserves the original per-user message order inside every group.
                for (int i = 0; i < injectCount; ++i)
                {
                    groupIndex = groupIndices[injects[i].id];
                    group = groups[groupIndex];
                    groupedInjectIndices[group.offset++] = i;
                    groups[groupIndex] = group;
                }

                for (int i = 0; i < groupCount; ++i)
                {
                    group = groups[i];
                    group.offset -= group.count;
                    groups[i] = group;
                }
            }
        }

        [BurstCompile]
        private struct Inject : IJobParallelForDefer
        {
            public NetworkRelayServerHandler handler;

            public NetworkServerSendBuffer.ParallelWriter sendBuffer;

            [ReadOnly]
            public NativeArray<InjectGroup> groups;

            [ReadOnly]
            public NativeArray<int> groupedInjectIndices;

            [ReadOnly]
            public NativeArray<NetworkRelayServerInjectSingleton.Inject> injects;

            [ReadOnly]
            public NativeArray<byte> injectBytes;

            public void Execute(int index)
            {
                var group = groups[index];
                int groupedInjectCount = groupedInjectIndices.Length;
                if (group.offset < 0 ||
                    group.count < 1 ||
                    group.offset > groupedInjectCount ||
                    group.count > groupedInjectCount - group.offset)
                    return;

                var identity = new NetworkServerSendBuffer.ParallelIdentity(group.id, ref sendBuffer);
                if (identity.connectionIndex < 0 || identity.channelIndex < 0)
                    return;

                int injectCount = injects.Length;
                int injectByteCount = injectBytes.Length;
                int end = group.offset + group.count;
                for (int i = group.offset; i < end; ++i)
                {
                    int injectIndex = groupedInjectIndices[i];
                    if ((uint)injectIndex >= (uint)injectCount)
                        continue;

                    var inject = injects[injectIndex];
                    if (inject.id != group.id ||
                        inject.byteOffset < 0 ||
                        inject.byteCount < 1 ||
                        inject.byteOffset > injectByteCount ||
                        inject.byteCount > injectByteCount - inject.byteOffset)
                        continue;

                    var reader = new DataStreamReader(
                        injectBytes.GetSubArray(inject.byteOffset, inject.byteCount));
                    handler.Read(ref reader, ref identity);
                }
            }
        }

        private struct Scheduler : INetworkServerScheduler
        {
            public int innerloopBatchCount;
            public double time;
            public NetworkServerSendBuffer sendBuffer;
            public NativeQueue<NetworkRelayServerModifier> modifiers;

            public NativeParallelMultiHashMap<int, uint> channelIDs;

            public NativeList<NetworkRelayServerChannel> channels;
            
            public NativeList<NetworkRelayServerIdentity> identities;

            public NativeList<NetworkRelayServerInjectSingleton.Inject> injects;

            public NativeList<byte> injectBytes;

            public NativeList<InjectGroup> injectGroups;

            public NativeList<int> groupedInjectIndices;

            public NativeHashMap<uint, int> injectGroupIndices;

            public NativeList<NetworkRelayServerMatch> matches;

            public NativeList<uint> matchIDs;

            public NativeList<int> matchDistances;

            public NativeArray<int> matchCount;

            public NativeParallelMultiHashMap<int, uint> matchDistanceIDs;

            public NetworkRelayServerHandler handler;

            public JobHandle Schedule(in JobHandle dependsOn)
            {
                // Drain bot/app-layer injects right after popEvents (real client reads) and
                // before Match/ModifyChannels, so injected messages are processed in the same
                // frame slot as genuine wire traffic.
                BuildInjectGroups buildInjectGroups;
                buildInjectGroups.injects = injects;
                buildInjectGroups.groups = injectGroups;
                buildInjectGroups.groupedInjectIndices = groupedInjectIndices;
                buildInjectGroups.groupIndices = injectGroupIndices;

                var jobHandle = buildInjectGroups.ScheduleByRef(dependsOn);

                Inject inject;
                inject.handler = handler;
                inject.sendBuffer = sendBuffer.AsParallelWriter();
                inject.groups = injectGroups.AsDeferredJobArray();
                inject.groupedInjectIndices = groupedInjectIndices.AsDeferredJobArray();
                inject.injects = injects.AsDeferredJobArray();
                inject.injectBytes = injectBytes.AsDeferredJobArray();

                jobHandle = inject.ScheduleByRef(injectGroups, innerloopBatchCount, jobHandle);

                Match match;
                match.time = time;
                match.sendBuffer = sendBuffer.AsReadOnly();
                match.identities = identities;
                match.matches = matches;
                match.matchIDs = matchIDs;
                match.matchDistances = matchDistances;
                match.matchDistanceIDs = matchDistanceIDs;
                match.channelIDs = channelIDs;
                match.modifiers = modifiers.AsParallelWriter();

                jobHandle = match.ScheduleByRef(matchIDs, innerloopBatchCount, jobHandle);

                ModifyChannels modifyChannels;
                modifyChannels.channelIDs = channelIDs;
                modifyChannels.modifiers = modifiers;
                modifyChannels.sendBuffer = sendBuffer.AsWriter(true);
                modifyChannels.identities = identities;
                modifyChannels.channels = channels;
                modifyChannels.matches = matches;
                modifyChannels.matchIDs = matchIDs;
                modifyChannels.matchDistances = matchDistances;
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

        private NativeList<int> __matchDistances;

        private NativeArray<int> __matchCount;

        private NativeParallelMultiHashMap<int, uint> __matchDistanceIDs;

        private NativeParallelMultiHashMap<int, uint> __channelIDs;

        private NativeQueue<NetworkRelayServerModifier> __modifiers;

        private NativeList<InjectGroup> __injectGroups;

        private NativeList<int> __groupedInjectIndices;

        private NativeHashMap<uint, int> __injectGroupIndices;

        public bool isCreated => __instance.isCreated;

        public int connectionCount => __sendBuffer.connections.Length;
        
        public int channelCount => __channelIDs.Count();

        public int matchCount => __matchIDs.Length;

        /// <summary>
        /// Burst-safe read view of the match pool after <see cref="Scheduler"/> jobs for this frame.
        /// Holds NativeList handles (like <see cref="NetworkServerSendBuffer.AsReadOnly"/>) so callers
        /// can schedule dependent Jobs without Completing ModifyChannels. Do not call
        /// <c>AsArray()</c> on these lists from the main thread while Server jobs are pending.
        /// </summary>
        public struct ReadOnly
        {
            [ReadOnly]
            public NativeList<uint> matchIDs;
            [ReadOnly]
            public NativeList<NetworkRelayServerMatch> matches;
            [ReadOnly]
            public NativeList<NetworkRelayServerIdentity> identities;
            public NetworkServerSendBuffer.ReadOnly sendBuffer;
        }

        public ReadOnly AsReadOnly()
        {
            ReadOnly value;
            value.matchIDs = __matchIDs;
            value.matches = __matches;
            value.identities = __identities;
            value.sendBuffer = __sendBuffer.AsReadOnly();
            return value;
        }
        
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

            __matchDistances = new NativeList<int>(allocator);

            __matchCount = CollectionHelper.CreateNativeArray<int>(1, allocator, NativeArrayOptions.ClearMemory);

            __matchDistanceIDs = new NativeParallelMultiHashMap<int, uint>(1, allocator);

            __channelIDs = new NativeParallelMultiHashMap<int, uint>(1, allocator);

            __modifiers = new NativeQueue<NetworkRelayServerModifier>(allocator);

            __injectGroups = new NativeList<InjectGroup>(allocator);
            __groupedInjectIndices = new NativeList<int>(allocator);
            __injectGroupIndices = new NativeHashMap<uint, int>(1, allocator);

            Pipeline = __instance.CreatePipeline(stages);
        }

        public NetworkRelayServer(
            in NativeArray<NetworkPipelineStage> stages,
            in NetworkSettings settings, 
            in AllocatorManager.AllocatorHandle allocator)
        {
            using (var stageIDs = stages.ToPipelineStageIDs(Allocator.Temp))
                this = new NetworkRelayServer(settings, stageIDs, allocator);
        }

        public void Dispose()
        {
            __instance.Dispose();
            __sendBuffer.Dispose();
            __channels.Dispose();
            __identities.Dispose();
            __matches.Dispose();
            __matchIDs.Dispose();
            __matchDistances.Dispose();
            __matchCount.Dispose();
            __matchDistanceIDs.Dispose();
            __channelIDs.Dispose();
            __modifiers.Dispose();
            __injectGroups.Dispose();
            __groupedInjectIndices.Dispose();
            __injectGroupIndices.Dispose();
        }

        public NetworkPipeline CreatePipeline(in NativeArray<NetworkPipelineStageId> stages)
        {
            return __instance.CreatePipeline(stages);
        }

        public int AddDriver(ref NetworkDriver driver)
        {
            return __instance.AddDriver(ref driver);
        }

        public void Listen(ushort udpPort, ushort wsPort, NetworkFamily family = NetworkFamily.Ipv4)
        {
            __instance.Listen(udpPort, wsPort, family);
        }

        public void Disconnect(in NetworkConnection connection)
        {
            __instance.Disconnect(connection);
        }

        public JobHandle Schedule(
            int innerloopBatchCount,
            double time, 
            in NetworkRelayServerInjectSingleton injectSingleton, 
            in JobHandle inputDeps)
        {
            NetworkRelayServerListener listener;
            listener.identities = __identities;
            listener.channels = __channels;
            listener.matches = __matches;
            listener.modifiers = __modifiers;

            NetworkRelayServerHandler handler;
            handler.time = time;
            handler.modifiers = __modifiers.AsParallelWriter();
            handler.channelIDs = __channelIDs;
            handler.channels = __channels.AsDeferredJobArray();
            handler.identities = __identities.AsDeferredJobArray();
            handler.matches = __matches.AsDeferredJobArray();
            handler.matchCount = __matchCount;

            Scheduler scheduler;
            scheduler.innerloopBatchCount = innerloopBatchCount;
            scheduler.time = time;
            scheduler.sendBuffer = __sendBuffer;
            scheduler.modifiers = __modifiers;
            scheduler.channelIDs = __channelIDs;
            scheduler.channels = __channels;
            scheduler.identities = __identities;
            scheduler.injects = injectSingleton.injects;
            scheduler.injectBytes = injectSingleton.bytes;
            scheduler.injectGroups = __injectGroups;
            scheduler.groupedInjectIndices = __groupedInjectIndices;
            scheduler.injectGroupIndices = __injectGroupIndices;
            scheduler.matches = __matches;
            scheduler.matchIDs = __matchIDs;
            scheduler.matchDistances = __matchDistances;
            scheduler.matchCount = __matchCount;
            scheduler.matchDistanceIDs = __matchDistanceIDs;
            scheduler.handler = handler;

            return __instance.Schedule(
                ref listener, ref handler, ref scheduler, ref __sendBuffer, 
                injectSingleton.pipelines, 
                Pipeline, 
                inputDeps, 
                innerloopBatchCount);
        }
    }
}
