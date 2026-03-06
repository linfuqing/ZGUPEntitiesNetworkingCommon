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
    }
}
