using Unity.Burst;
using Unity.Entities;

namespace ZG
{
    [BurstCompile, WorldSystemFilter(WorldSystemFilterFlags.ServerSimulation)]
    public partial struct NetworkRelayServerSystem : ISystem
    {
        public static readonly int InnerloopBatchCount = 4;
        
        [BurstCompile]
        public void OnCreate(ref SystemState state)
        {
            state.RequireForUpdate<NetworkRelayServer>();
        }

        [BurstCompile]
        public void OnUpdate(ref SystemState state)
        {
            state.Dependency = SystemAPI.GetSingleton<NetworkRelayServer>().Schedule(InnerloopBatchCount, state.Dependency);
        }
    }
}