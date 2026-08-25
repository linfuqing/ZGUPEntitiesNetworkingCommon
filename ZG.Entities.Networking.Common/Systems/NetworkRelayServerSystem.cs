using Unity.Burst;
using Unity.Collections;
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
            state.RequireForUpdate<NetworkRelayServerInjectSingleton>();

            state.EntityManager.CreateSingleton(new NetworkRelayServerInjectSingleton(Allocator.Persistent));
        }

        [BurstCompile]
        public void OnDestroy(ref SystemState state)
        {
            if(SystemAPI.TryGetSingletonRW<NetworkRelayServer>(out var server))
                server.ValueRW.Dispose();
            
            if(SystemAPI.TryGetSingleton<NetworkRelayServerInjectSingleton>(out var injectSingleton))
                injectSingleton.Dispose();
        }

        [BurstCompile]
        public void OnUpdate(ref SystemState state)
        {
            ref var server = ref SystemAPI.GetSingletonRW<NetworkRelayServer>().ValueRW;
            
            state.Dependency = server.Schedule(InnerloopBatchCount, SystemAPI.Time.ElapsedTime, SystemAPI.GetSingleton<NetworkRelayServerInjectSingleton>(), state.Dependency);
        }
    }
}
