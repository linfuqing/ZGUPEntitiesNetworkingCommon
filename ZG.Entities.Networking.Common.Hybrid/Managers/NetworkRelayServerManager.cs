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

        private NetworkRelayServer __instance;
        
        private static Entity __entity;

        public static NetworkRelayServer server
        {
            get
            {
                if (__entity == Entity.Null)
                    return default;
                
                return World.DefaultGameObjectInjectionWorld.EntityManager.GetComponentData<NetworkRelayServer>(__entity);
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
        
        void Start()
        {
            if (__entity == Entity.Null)
            {
                var entityManager = World.DefaultGameObjectInjectionWorld.EntityManager;
                __entity = entityManager.CreateSingleton<NetworkRelayServer>();

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

                    if (!string.IsNullOrEmpty(_certificatePath) && !string.IsNullOrEmpty(_privateKeyPath))
                    {
                        var cert = File.ReadAllText(_certificatePath);
                        var key = File.ReadAllText(_privateKeyPath);

                        settings.WithSecureServerParameters(cert, key);
                    }

                    __instance = new NetworkRelayServer(
                        stages,
                        settings,
                        Allocator.Persistent);
                }

                __instance.Listen(_udpPort, _wsPort, _family);

                entityManager.SetComponentData(__entity, __instance);
            }
        }

        void OnDestroy()
        {
            __instance.Dispose();
        }
    }
}