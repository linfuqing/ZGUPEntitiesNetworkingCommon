using System;
using Unity.Collections;
using Unity.Networking.Transport;

namespace ZG
{
    public enum NetworkRelayMessageType
    {
        Init,
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

    public enum NetworkPipelineStage
    {
        Fragmentation,
        ReliableSequenced,
        UnreliableSequenced,
        Simulator
    }

    public static class NetworkingUtility
    {
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
            out int identityIndex)
        {
            var streamCompressionModel =  StreamCompressionModel.Default;
            messageType = reader.ReadPackedInt(streamCompressionModel);
            relayType = (NetworkRelayType)reader.ReadPackedInt(streamCompressionModel);
            identityIndex = reader.ReadPackedInt(streamCompressionModel);
            reader.Flush();
        }
        
        public static void ReadReplyHeader(
            this ref DataStreamReader reader, 
            out NetworkRelayType relayType, 
            out int identityIndex)
        {
            var streamCompressionModel =  StreamCompressionModel.Default;
            relayType = (NetworkRelayType)reader.ReadPackedInt(streamCompressionModel);
            identityIndex = reader.ReadPackedInt(streamCompressionModel);
            reader.Flush();
        }
    }
}
