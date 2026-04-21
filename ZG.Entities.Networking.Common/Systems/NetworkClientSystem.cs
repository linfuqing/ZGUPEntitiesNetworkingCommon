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
            var driver = SystemAPI.GetSingleton<NetworkClientDriver>();
            state.Dependency = driver.Schedule(SystemAPI.Time.ElapsedTime, state.Dependency);
            
            SystemAPI.SetSingleton(driver);
        }
    }
}