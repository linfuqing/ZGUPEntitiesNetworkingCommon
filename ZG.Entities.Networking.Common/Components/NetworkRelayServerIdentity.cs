using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Mathematics;

namespace ZG
{
    
    public struct NetworkRelayServerIdentity
    {
        public const int CHANNEL_NULL = -1;

        public readonly uint ID;

        private UnsafeList<uint> __friendIDs;

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

        private static void SendChannelJoins<T>(
            int channel, 
            uint id, 
            in NativeArray<NetworkRelayServerIdentity> identities, 
            in NativeParallelMultiHashMap<int, uint> channelIDs, 
            ref T sendBuffer) where T : struct, INetworkServerSendBuffer
        {
            int identityIndex;
            NetworkRelayServerIdentity identity;
            foreach (var channelID in channelIDs.GetValuesForKey(channel))
            {
                if (channelID == id)
                    continue;

                identityIndex = sendBuffer.GetChannelIndex(channelID);
                identity = identities[identityIndex];
                identity.SendHeader((int)NetworkRelayMessageType.Join,
                    id, 
                    ref sendBuffer);
            }
        }
        
        public NetworkRelayServerIdentity(uint id, in AllocatorManager.AllocatorHandle allocator)
        {
            ID = id;

            match = 0;
            
            channel = CHANNEL_NULL;

            channelFlag = 0;
            
            __friendIDs = new UnsafeList<uint>(1, allocator);
        }

        public void Dispose()
        {
            __friendIDs.Dispose();
        }

        public void Clear()
        {
            match = 0;
            channel = CHANNEL_NULL;
            channelFlag = 0;
            
            __friendIDs.Clear();
        }

        public void SetTemp()
        {
            channelFlag |= NetworkRelayChannelFlag.Temp;
        }

        public void Connect<T>(
            uint id, 
            in NativeArray<NetworkRelayServerIdentity> identities, 
            in NativeParallelMultiHashMap<int, uint> channelIDs, 
            ref T sendBuffer) where T : struct, INetworkServerSendBuffer
        {
            isOnline = true;

            DataStreamWriter writer;
            int channel = this.channel;
            if (channel != CHANNEL_NULL)
            {
                UnityEngine.Assertions.Assert.AreEqual(channel, identities[sendBuffer.GetChannelIndex(id)].channel);
                
                var channelFlag = this.channelFlag;
                if (sendBuffer.BeginWrite(channel, out writer))
                {
                    __WriteStatus((int)NetworkRelayMessageType.Connect, ref writer);
                    
                    sendBuffer.EndWrite(writer);
                }
                
                SendHeader((channelFlag & NetworkRelayChannelFlag.Creator) ==
                                    NetworkRelayChannelFlag.Creator
                        ? (int)NetworkRelayMessageType.Create
                        : (int)NetworkRelayMessageType.Join,
                    id, 
                    ref sendBuffer);
                
                SendChannelJoins(channel, id, identities, channelIDs, ref sendBuffer);
            }

            NetworkRelayServerIdentity identity;
            foreach (var friendID in __friendIDs)
            {
                identity = identities[sendBuffer.GetChannelIndex(friendID)];
                if (CHANNEL_NULL == channel || identity.channel != channel)
                {
                    if (sendBuffer.BeginWrite(friendID, out writer))
                    {
                        __WriteStatus((int)NetworkRelayMessageType.Connect, ref writer);

                        sendBuffer.EndWrite(writer);
                    }
                }

                if (sendBuffer.BeginWrite(id, out writer))
                {
                    identity.__WriteStatus((int)NetworkRelayMessageType.Add, ref writer);
                    
                    sendBuffer.EndWrite(writer);
                }
            }
        }

        public void Disconnect<T>(
            uint id, 
            in NativeArray<NetworkRelayServerIdentity> identities, 
            ref T sendBuffer) where T : struct, INetworkServerSendBuffer
        {
            int channel = this.channel;
            if (channel != CHANNEL_NULL)
            {
                UnityEngine.Assertions.Assert.AreEqual(channel, identities[sendBuffer.GetChannelIndex(id)].channel);

                if (sendBuffer.BeginWrite(channel, out var writer))
                {
                    __WriteID((int)NetworkRelayMessageType.Disconnect, ref writer);
                    
                    sendBuffer.EndWrite(writer);
                }
            }
            
            foreach (var friendID in __friendIDs)
            {
                if(CHANNEL_NULL != channel && identities[sendBuffer.GetChannelIndex(friendID)].channel == channel)
                    continue;

                if (sendBuffer.BeginWrite(friendID, out var writer))
                {
                    __WriteID((int)NetworkRelayMessageType.Disconnect, ref writer);
                    
                    sendBuffer.EndWrite(writer);
                }
            }

            isOnline = false;
            
            /*if ((channelFlag & NetworkRelayChannelFlag.Temp) == NetworkRelayChannelFlag.Temp)
                Leave(id, ref sendBuffer);*/
        }

        public bool AddFriend<T>(uint id, ref T sendBuffer) where T : struct, INetworkServerSendBuffer
        {
            if (__friendIDs.Contains(id))
                return false;
            
            __friendIDs.Add(id);
            
            if (sendBuffer.BeginWrite(id, out var writer))
            {
                __WriteStatus((int)NetworkRelayMessageType.Add, ref writer);
                
                sendBuffer.EndWrite(writer);
            }
            
            return true;
        }

        public bool RemoveFriend<T>(uint id, ref T sendBuffer) where T : struct, INetworkServerSendBuffer
        {
            int index = __friendIDs.IndexOf(id);
            if (index == -1)
                return false;
            
            __friendIDs.RemoveAt(index);

            if (sendBuffer.BeginWrite(id, out var writer))
            {
                __WriteID((int)NetworkRelayMessageType.Remove, ref writer);

                sendBuffer.EndWrite(writer);
            }

            return true;
        }
        
        public bool SetStatus<T>(
            int value, 
            uint id, 
            in NativeArray<NetworkRelayServerIdentity> identities, 
            ref T sendBuffer) where T : struct, INetworkServerSendBuffer
        {
            int channelStatus = (int)this.channelFlag;
            channelStatus >>= (int)NetworkRelayChannelFlag.ShiftToStatus;
            if (channelStatus == value)
                return false;

            if (value == 0 && (this.channelFlag & NetworkRelayChannelFlag.Temp) == NetworkRelayChannelFlag.Temp)
                Leave(id, ref sendBuffer);

            var channelFlag = this.channelFlag;
            channelFlag &= NetworkRelayChannelFlag.All;
            channelFlag |= (NetworkRelayChannelFlag)(value << (int)NetworkRelayChannelFlag.ShiftToStatus);
            this.channelFlag = channelFlag;

            var channel = this.channel;
            if (channel != CHANNEL_NULL && sendBuffer.BeginWrite(channel, out var writer))
            {
                __WriteStatus((int)NetworkRelayMessageType.Status, ref writer);

                sendBuffer.EndWrite(writer);
            }
            
            NetworkRelayServerIdentity identity;
            foreach (var friendID in __friendIDs)
            {
                identity = identities[sendBuffer.GetChannelIndex(friendID)];
                if (identity.channel == channel)
                    continue;
                
                if (sendBuffer.BeginWrite(friendID, out writer))
                {
                    __WriteStatus((int)NetworkRelayMessageType.Status, ref writer);

                    sendBuffer.EndWrite(writer);
                }
            }

            return true;
        }

        public void SendHeader<T>(
            int type,
            uint id, 
            ref T sendBuffer) where T : struct, INetworkServerSendBuffer
        {
            //UnityEngine.Debug.Log($"[SendHeader]{(NetworkRelayMessageType)type} {ID} to {sendBuffer.ID} in {channel}");
            bool isSendOthers = id != ID;
            var payload = sendBuffer.GetPayload(ID);
            if (sendBuffer.BeginWrite(id, out var writer,
                    (ushort)((isSendOthers ? payload.Length : 0) + 3 * UnsafeUtility.SizeOf<int>())))
            {
                __WriteHeader(isSendOthers, type, payload, ref writer);

                sendBuffer.EndWrite(writer);
            }
        }

        public void SendHeader<T>(
            int channel, 
            int type,
            ref T sendBuffer) where T : struct, INetworkServerSendBuffer
        {
            if (sendBuffer.BeginWrite(channel, out var writer))
            {
                __WriteHeader(true, type, sendBuffer.GetPayload(ID), ref writer);

                sendBuffer.EndWrite(writer);
            }
        }
        
        public bool Create<T>(
            //int channel,
            bool isTemp, 
            uint id, 
            in NativeArray<NetworkRelayServerIdentity> identities, 
            in NativeParallelMultiHashMap<int, uint> channelIDs, 
            ref T sendBuffer) where T : struct, INetworkServerSendBuffer
        {
            int channel = sendBuffer.GetChannelIndex(ID);
            if (__CreateOrJoin(isTemp ? NetworkRelayChannelFlag.Temp | NetworkRelayChannelFlag.Creator : NetworkRelayChannelFlag.Creator,
                    (int)NetworkRelayMessageType.Create,
                    channel,
                    id, 
                    ref sendBuffer))
            {
                SendChannelJoins(channel, id, identities, channelIDs, ref sendBuffer);
                
                return true;
            }

            return false;
        }
        
        public bool Join<T>(
            bool isTemp, 
            int channel,
            uint id, 
            in NativeArray<NetworkRelayServerIdentity> identities, 
            in NativeParallelMultiHashMap<int, uint> channelIDs, 
            ref T sendBuffer) where T : struct, INetworkServerSendBuffer
        {
            if (__CreateOrJoin(
                    isTemp ? NetworkRelayChannelFlag.Temp : 0,
                    (int)NetworkRelayMessageType.Join,
                    channel,
                    id, 
                    ref sendBuffer))
            {
                SendChannelJoins(channel, id, identities, channelIDs, ref sendBuffer);
                
                return true;
            }

            return false;
        }

        public bool Leave<T>(uint id, ref T sendBuffer) where T : struct, INetworkServerSendBuffer
        {
            return __DropOrLeave((int)NetworkRelayMessageType.Leave, id, ref sendBuffer);
        }

        public bool Drop<T>(
            uint id, 
            ref T sendBuffer) where T : struct, INetworkServerSendBuffer
        {
            var type = (channelFlag & NetworkRelayChannelFlag.Creator) == NetworkRelayChannelFlag.Creator
                ? NetworkRelayMessageType.Leave : NetworkRelayMessageType.Drop;
            return __DropOrLeave((int)type, id, ref sendBuffer);
        }

        public bool Matching<T>(int value, uint id, ref T sendBuffer) where T : struct, INetworkServerSendBuffer
        {
            if (match != 0 || 
                channel != CHANNEL_NULL && (channelFlag & NetworkRelayChannelFlag.Creator) != NetworkRelayChannelFlag.Creator || 
                !canMatch)
                return false;
            
            match = value;
            
            __Match((int)NetworkRelayMessageType.Matching, id, ref sendBuffer);
            
            return true;
        }

        public void Match<T>(int match, int distance, uint id, ref T sendBuffer) where T : struct, INetworkServerSendBuffer
        {
            if (sendBuffer.BeginWrite(id, out var writer, (ushort)(3 * UnsafeUtility.SizeOf<int>())))
            {
                var streamCompressionModel = StreamCompressionModel.Default;

                writer.WritePackedInt((int)NetworkRelayMessageType.Match, streamCompressionModel);
                writer.WritePackedInt(match, streamCompressionModel);
                writer.WritePackedInt(distance, streamCompressionModel);
                sendBuffer.EndWrite(writer);
            }
            
            this.match = 0;
        }
        
        public bool Mismatch<T>(uint id, ref T sendBuffer) where T : struct, INetworkServerSendBuffer
        {
            if (match != 0)
            {
                __Match((int)NetworkRelayMessageType.Mismatch, id, ref sendBuffer);

                match = 0;

                return true;
            }

            return false;
        }

        public void Relay<T>(
            int type,
            NetworkRelayType relayType,
            uint id, 
            ref DataStreamReader reader,
            ref T sendBuffer) where T : struct, INetworkServerSendBuffer
        {
            ushort capacity = (ushort)(reader.Length - reader.GetBytesRead() + 3 * UnsafeUtility.SizeOf<int>());
            DataStreamWriter writer;
            switch(relayType)
            {
                case NetworkRelayType.All:
                    if (!sendBuffer.BeginWrite(out writer, capacity))
                        return;
                    break;
                case NetworkRelayType.Channel:
                    if (channel == CHANNEL_NULL || !sendBuffer.BeginWrite(channel, out writer, capacity))
                        return;
                    break;
                default:
                    if (!sendBuffer.BeginWrite(relayType.RelayID(), out writer, capacity))
                        return;
                    break;
            }

            SendRelay(type, (int)relayType, id, ref reader, ref writer);

            sendBuffer.EndWrite(writer);
        }

        private bool __CreateOrJoin<T>(
            NetworkRelayChannelFlag channelFlag, 
            int type, 
            int channel,
            uint id, 
            ref T sendBuffer) where T : struct, INetworkServerSendBuffer
        {
            UnityEngine.Assertions.Assert.AreEqual(ID, id);
            if (match != 0 || channel == CHANNEL_NULL || !sendBuffer.AddChannel(ID, channel))
                return false;

            Leave(id, ref sendBuffer);
            
            UnityEngine.Debug.Log($"[CreateOrJoin]{(NetworkRelayMessageType)type} {ID} to {id} in {channel}");
            
            this.channelFlag |= channelFlag;
            this.channel = channel;

            SendHeader(type, id, ref sendBuffer);
            SendHeader(channel, type, ref sendBuffer);

            Mismatch(id, ref sendBuffer);

            return true;
        }

        private bool __DropOrLeave<T>(
            int type, 
            uint id, 
            ref T sendBuffer) where T : struct, INetworkServerSendBuffer
        {
            if (channel == CHANNEL_NULL)
                return false;
            
            UnityEngine.Debug.Log($"[DropOrLeave]{(NetworkRelayMessageType)type} {ID} to {id} in {channel}");

            UnityEngine.Assertions.Assert.IsTrue(sendBuffer.ContainsChannel(id, channel));
            
            if (sendBuffer.RemoveChannel(ID, channel))
            {
                SendHeader(type, id, ref sendBuffer);
                SendHeader(channel, type, ref sendBuffer);
            }

            channelFlag &= ~(NetworkRelayChannelFlag.Creator | NetworkRelayChannelFlag.Temp);
            channel = CHANNEL_NULL;
            
            Mismatch(id, ref sendBuffer);

            return true;
        }
        
        private void __Match<T>(
            int type,
            uint id, 
            ref T sendBuffer) where T : struct, INetworkServerSendBuffer
        {
            var streamCompressionModel = StreamCompressionModel.Default;

            var channel = this.channel;
            if (channel != CHANNEL_NULL && sendBuffer.BeginWrite(channel, out var writer, (ushort)(3 * UnsafeUtility.SizeOf<int>())))
            {
                writer.WritePackedInt(type, streamCompressionModel);
                writer.WritePackedInt(match, streamCompressionModel);
                writer.WritePackedUInt(ID, streamCompressionModel);
                sendBuffer.EndWrite(writer);
            }

            if (sendBuffer.BeginWrite(id, out writer, (ushort)(2 * UnsafeUtility.SizeOf<int>())))
            {
                writer.WritePackedInt(type, streamCompressionModel);
                writer.WritePackedInt(match, streamCompressionModel);
                sendBuffer.EndWrite(writer);
            }
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

        private void __WriteStatus(int type, ref DataStreamWriter writer)
        {
            var streamCompressionModel = StreamCompressionModel.Default;
            
            writer.WritePackedInt(type, streamCompressionModel);
            writer.WritePackedInt((int)channelFlag, streamCompressionModel);
            writer.WritePackedUInt(ID, streamCompressionModel);
        }
        
        private void __WriteID(int type, ref DataStreamWriter writer)
        {
            var streamCompressionModel = StreamCompressionModel.Default;
            
            writer.WritePackedInt(type, streamCompressionModel);
            writer.WritePackedUInt(ID, streamCompressionModel);
        }
    }

}