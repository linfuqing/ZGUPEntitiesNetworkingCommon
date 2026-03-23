using System;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Networking.Transport;

namespace ZG
{
    public enum NetworkRelayMessageType
    {
        Connect, 
        Disconnect,
        Status,
        Create,
        Join,
        Leave,
        Drop, 
        Query
    }

    public enum NetworkRelayType
    {
        All,
        Channel,
        Identity
    }

    [Flags]
    public enum NetworkRelayChannelFlag
    {
        Online = 0x01, 
        Creator = 0x02, 
        
        ShiftToStatus = 2, 
        All = Online | Creator, 
    }

    public enum NetworkPipelineStage
    {
        Fragmentation,
        ReliableSequenced,
        UnreliableSequenced,
        Simulator
    }

    public static class NetworkingUtility
    {
        public static uint RelayID(this NetworkRelayType relayType)
        {
            UnityEngine.Assertions.Assert.IsTrue(relayType > NetworkRelayType.Identity);
            return (uint)(relayType - NetworkRelayType.Identity);
        }

        public static NetworkRelayType RelayType(this uint id)
        {
            return (NetworkRelayType)(id + (uint)NetworkRelayType.Identity);
        }
        
        public static NativeArray<NetworkPipelineStageId> ToPipelineStageIDs(
            this in NativeArray<NetworkPipelineStage> stages, 
            in AllocatorManager.AllocatorHandle allocator)
        {
            int numStages = stages.Length;
            using var stageIDs = new NativeList<NetworkPipelineStageId>(numStages, Allocator.Temp);
            for(int i = 0; i < numStages; ++i)
            {
                switch (stages[i])
                {
                    case NetworkPipelineStage.Fragmentation:
                        stageIDs.Add(NetworkPipelineStageId.Get<FragmentationPipelineStage>());
                        break;
                    case NetworkPipelineStage.ReliableSequenced:
                        stageIDs.Add(NetworkPipelineStageId.Get<ReliableSequencedPipelineStage>());
                        break;
                    case NetworkPipelineStage.UnreliableSequenced:
                        stageIDs.Add(NetworkPipelineStageId.Get<UnreliableSequencedPipelineStage>());
                        break;
                    case NetworkPipelineStage.Simulator:
                        stageIDs.Add(NetworkPipelineStageId.Get<SimulatorPipelineStage>());
                        break;
                }
            }

            return stageIDs.ToArray(allocator);
        }

        public static void WriteReplyHeader(this ref DataStreamWriter writer, int messageType, NetworkRelayType relayType)
        {
            var streamCompressionModel =  StreamCompressionModel.Default;
            writer.WritePackedInt(messageType, streamCompressionModel);
            writer.WritePackedInt((int)relayType, streamCompressionModel);
            writer.Flush();
        }
        
        public static void ReadReplyHeader(
            this ref DataStreamReader reader, 
            out int messageType, 
            out NetworkRelayType relayType, 
            out uint id)
        {
            var streamCompressionModel =  StreamCompressionModel.Default;
            messageType = reader.ReadPackedInt(streamCompressionModel);
            relayType = (NetworkRelayType)reader.ReadPackedInt(streamCompressionModel);
            id = reader.ReadPackedUInt(streamCompressionModel);
            reader.Flush();
        }
        
        public static void ReadReplyHeader(
            this ref DataStreamReader reader, 
            out NetworkRelayType relayType, 
            out uint id)
        {
            var streamCompressionModel =  StreamCompressionModel.Default;
            relayType = (NetworkRelayType)reader.ReadPackedInt(streamCompressionModel);
            id = reader.ReadPackedUInt(streamCompressionModel);
            reader.Flush();
        }

        public static void Write(this ref DataStreamWriter writer, ref DataStreamReader reader)
        {
            int length = reader.Length;
            NativeArray<byte> bytes;
            unsafe
            {
                int byteOffset = reader.GetBytesRead();
                bytes = CollectionHelper.ConvertExistingDataToNativeArray<byte>((byte*)reader.GetUnsafeReadOnlyPtr() + byteOffset, length - byteOffset,
                    Allocator.None, true);
            }

            writer.WriteBytes(bytes);
            
            reader.SeekSet(length);
        }
    }
}
