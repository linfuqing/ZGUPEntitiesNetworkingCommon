using System.Collections;
using System.Collections.Generic;
using Unity.Burst;
using Unity.Entities;

namespace ZG
{

    [BurstCompile, UpdateInGroup(typeof(PresentationSystemGroup))]
    public partial struct NetworkClientSystem : ISystem
    {
        [BurstCompile]
        public void OnCreate(ref SystemState state)
        {
            state.RequireForUpdate<NetworkClientDriver>();
        }

        [BurstCompile]
        public void OnUpdate(ref SystemState state)
        {
            state.Dependency = SystemAPI.GetSingleton<NetworkClientDriver>().Schedule(state.Dependency);
        }
    }
}