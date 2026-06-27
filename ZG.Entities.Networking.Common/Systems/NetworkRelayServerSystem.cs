using Unity.Burst;
using Unity.Entities;

namespace ZG
{
    [BurstCompile, WorldSystemFilter(WorldSystemFilterFlags.ServerSimulation |  WorldSystemFilterFlags.Default)]
    public partial struct NetworkRelayServerSystem : ISystem
    {
        public static readonly int InnerloopBatchCount = 4;
        
        [BurstCompile]
        public void OnCreate(ref SystemState state)
        {
            state.RequireForUpdate<NetworkRelayServer>();
        }

        [BurstCompile]
        public void OnDestroy(ref SystemState state)
        {
            if(SystemAPI.TryGetSingleton<NetworkRelayServer>(out var server))
                server.Dispose();
        }

        [BurstCompile]
        public void OnUpdate(ref SystemState state)
        {
            var server = SystemAPI.GetSingleton<NetworkRelayServer>();
            
            state.Dependency = server.Schedule(InnerloopBatchCount, SystemAPI.Time.ElapsedTime, state.Dependency);
            
            SystemAPI.SetSingleton(server);
        }
    }
}