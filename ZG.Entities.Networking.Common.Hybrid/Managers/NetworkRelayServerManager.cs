using System.IO;
using Unity.Collections;
using Unity.Entities;
using Unity.Networking.Transport;
using Unity.Networking.Transport.TLS;
using UnityEngine;

namespace ZG
{

    public class NetworkRelayServerManager : MonoBehaviour
    {
        [SerializeField]
        internal int _connectTimeoutMS = 1000;
        [SerializeField]
        internal int _maxConnectAttempts = 60;
        [SerializeField]
        internal int _disconnectTimeoutMS = 30 * 1000;
        [SerializeField]
        internal int _heartbeatTimeoutMS = 500;
        [SerializeField]
        internal int _reconnectionTimeoutMS = 2000;
        //[SerializeField]
        //internal int _maxFrameTimeMS = 0;
        [SerializeField]
        internal int _fixedFrameTimeMS = 0;
        [SerializeField]
        internal int _receiveQueueCapacity = 4096;//ReceiveQueueCapacity;
        [SerializeField]
        internal int _sendQueueCapacity = 4096;// SendQueueCapacity;
        [SerializeField]
        internal ushort _udpPort = 1386;
        [SerializeField]
        internal ushort _wsPort = 1387;
        [SerializeField] 
        internal string _certificatePath;
        [SerializeField] 
        internal string _privateKeyPath;
        [SerializeField] 
        internal NetworkFamily _family = NetworkFamily.Ipv4;
        [SerializeField] 
        internal NetworkPipelineStage[] _stages = new NetworkPipelineStage[]
        {
            NetworkPipelineStage.Fragmentation,
            NetworkPipelineStage.ReliableSequenced,
        };

        public static ref NetworkRelayServer server
        {
            get
            {
                var world = World.DefaultGameObjectInjectionWorld;
                world.Unmanaged.GetExistingSystemState<NetworkRelayServerSystem>().Dependency.Complete();
                return ref world.EntityManager.GetComponentDataRW<NetworkRelayServer>(world.GetExistingSystem<NetworkRelayServerSystem>()).ValueRW;
            }
        }

        public static bool GetServerStatus(out int connectionCount, out int channelCount, out int matchCount)
        {
            var server = NetworkRelayServerManager.server;
            if (server.isCreated)
            {
                connectionCount = server.connectionCount;
                channelCount = server.channelCount;
                matchCount = server.matchCount;

                return true;
            }
            
            connectionCount = 0;
            channelCount = 0;
            matchCount = 0;

            return false;
        }

        void Awake()
        {
            var world = World.DefaultGameObjectInjectionWorld;
            if (world == null || !world.IsCreated)
                return;

            var entityManager = world.EntityManager;
            var systemHandle = world.GetExistingSystem<NetworkRelayServerSystem>();

            // Prefer reusing a live server (Enter Play Mode Options often keep the ECS world /
            // Listen socket across sessions). Only rebuild when missing or not created.
            if (systemHandle != default &&
                entityManager.HasComponent<NetworkRelayServer>(systemHandle))
            {
                var existing = entityManager.GetComponentData<NetworkRelayServer>(systemHandle);
                if (existing.isCreated)
                    return;
            }

            // Stale/disposed component: drop it before binding a new Listen socket.
            __DisposeServerComponent(world, systemHandle);

            NetworkRelayServer instance;
            using (var stages = new NativeArray<NetworkPipelineStage>(_stages, Allocator.Temp))
            {
                var settings = new NetworkSettings(Allocator.Temp);
                settings.WithNetworkConfigParameters(
                    _connectTimeoutMS,
                    _maxConnectAttempts,
                    _disconnectTimeoutMS,
                    _heartbeatTimeoutMS,
                    _reconnectionTimeoutMS,
                    Mathf.CeilToInt(Time.maximumDeltaTime * 1000),
                    _fixedFrameTimeMS,
                    _receiveQueueCapacity,
                    _sendQueueCapacity);

#if !UNITY_EDITOR
                    if (!string.IsNullOrEmpty(_certificatePath) && !string.IsNullOrEmpty(_privateKeyPath))
                    {
                        var cert = File.ReadAllText(_certificatePath);
                        var key = File.ReadAllText(_privateKeyPath);

                        settings.WithSecureServerParameters(cert, key);
                    }
#endif
                instance = new NetworkRelayServer(
                    stages,
                    settings,
                    Allocator.Persistent);
            }

            instance.Listen(_udpPort, _wsPort, _family);

            entityManager.AddComponentData(systemHandle, instance);
        }

        void OnDestroy()
        {
            try
            {
                var world = World.DefaultGameObjectInjectionWorld;
                if (world == null || !world.IsCreated)
                    return;

                __DisposeServerComponent(world, world.GetExistingSystem<NetworkRelayServerSystem>());
            }
            catch (System.Exception)
            {
                // World / system teardown order is not guaranteed on Exit Play Mode.
            }
        }

        static void __DisposeServerComponent(World world, SystemHandle systemHandle)
        {
            if (systemHandle == default || world == null || !world.IsCreated)
                return;

            var entityManager = world.EntityManager;
            if (!entityManager.HasComponent<NetworkRelayServer>(systemHandle))
                return;

            try
            {
                ref var systemState = ref world.Unmanaged.GetExistingSystemState<NetworkRelayServerSystem>();
                systemState.Dependency.Complete();
            }
            catch (System.Exception)
            {
                // Best-effort; Dispose still needed to release Listen sockets.
            }

            var server = entityManager.GetComponentData<NetworkRelayServer>(systemHandle);
            // Remove before Dispose so NetworkRelayServerSystem.OnDestroy cannot double-free.
            entityManager.RemoveComponent<NetworkRelayServer>(systemHandle);
            if (server.isCreated)
                server.Dispose();
        }
    }
}