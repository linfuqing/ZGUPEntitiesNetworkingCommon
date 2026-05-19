using System;
using System.Runtime.InteropServices;

using Unity.Burst;
using Unity.Collections;
using Unity.Jobs;
using Unity.Networking.Transport;
using Unity.Networking.Transport.Relay;
using Unity.Networking.Transport.TLS;
using Unity.Networking.Transport.Error;

namespace ZG
{
    
    public struct NetworkDelaySocket
    {
        public struct 
        public int Create(in FixedString512Bytes address)
        {
            
        }

        public void Destroy(int instanceID)
        {
            
        }

        public int Send(int instanceID, IntPtr data, int size)
        {
            
        }

        public int Recv(int instanceID, IntPtr data, int size)
        {
            
        }

        public bool IsConnectionReady(int instanceID)
        {
            
        }
    }

    public struct NetworkInterface : INetworkInterface
    {
        private const string DLL = "__Internal";

        static class WebSocket
        {
            public static int s_NextSocketId = 0;

            [DllImport(DLL, EntryPoint = "js_html_utpWebSocketCreate")]
            public static extern void Create(int sockId, IntPtr addrData, int addrSize);

            [DllImport(DLL, EntryPoint = "js_html_utpWebSocketDestroy")]
            public static extern void Destroy(int sockId);

            [DllImport(DLL, EntryPoint = "js_html_utpWebSocketSend")]
            public static extern int Send(int sockId, IntPtr data, int size);

            [DllImport(DLL, EntryPoint = "js_html_utpWebSocketRecv")]
            public static extern int Recv(int sockId, IntPtr data, int size);

            [DllImport(DLL, EntryPoint = "js_html_utpWebSocketIsConnected")]
            public static extern int IsConnectionReady(int sockId);
        }

        private struct InternalData
        {
            public int connectTimeoutMS; // maximum time to wait for a connection to complete

            // If non-empty, will connect to this hostname with the wss:// protocol. Otherwise the
            // IP address of the endpoint is used to connect with the ws:// protocol.
            public FixedString512Bytes secureHostname;

            public FixedString128Bytes path;
        }

        private struct ConnectionData
        {
            public int socket;
            public long connectStartTime;
        }

        private NativeReference<InternalData> __internalData;

        // Maps a connection id from the connection list to its connection data.
        private ConnectionDataMap<ConnectionData> __connectionMap;

        // List of connection information carried over to the layer above
        private ConnectionList __connectionList;

        internal ConnectionList CreateConnectionList()
        {
            __connectionList = ConnectionList.Create();
            return __connectionList;
        }

        /// <inheritdoc/>
        public NetworkEndpoint LocalEndpoint
        {
            get
            {
                // For WebGL there's really no concept of a local endpoint since the browser manages
                // the underlying sockets. Best we can do is differentiate between loopback and
                // external connections and return different addresses depending on that.

                var hostname = __internalData.Value.secureHostname.ToString();
                if (string.IsNullOrEmpty(hostname))
                {
                    // Try to get the hostname from the connection list.
                    int count = __connectionList.Count;
                    for (int i = 0; i < count; i++)
                    {
                        var connectionId = __connectionList.ConnectionAt(i);
                        var connectionState = __connectionList.GetConnectionState(connectionId);
                        if (connectionState == NetworkConnection.State.Connected)
                        {
                            var endpoint = __connectionList.GetConnectionEndpoint(connectionId);
                            hostname = endpoint.ToString();
                            break;
                        }
                    }
                }

                return hostname.StartsWith("127.") || hostname.StartsWith("localhost") || hostname.StartsWith("[::1]")
                    ? NetworkEndpoint.LoopbackIpv4
                    : NetworkEndpoint.AnyIpv4;
            }
        }

        /// <inheritdoc/>
        public int Initialize(ref NetworkSettings settings, ref int packetPadding)
        {
            var networkConfiguration = settings.GetNetworkConfigParameters();

            // This needs to match the value of Unity.Networking.Transport.WebSocket.MaxPayloadSize
            packetPadding += 14;

            var secureHostname = new FixedString512Bytes();
            if (settings.TryGet<RelayNetworkParameter>(out var relayParams) && relayParams.ServerData.IsSecure != 0)
                secureHostname.CopyFrom(relayParams.ServerData.HostString);

            // Shouldn't be required for normal use cases but is provided as an out in case the user
            // wants to override the hostname (useful if say the user ended up resolving the Relay's
            // hostname on their own instead of providing it directly in the Relay parameters).
            if (settings.TryGet<SecureNetworkProtocolParameter>(out var secureParams))
                secureHostname.CopyFrom(secureParams.Hostname);

            InternalData state;
            state.connectTimeoutMS = networkConfiguration.connectTimeoutMS * networkConfiguration.maxConnectAttempts;
            state.secureHostname = secureHostname;
            state.path = settings.GetWebSocketParameters().Path;
            
            __internalData = new NativeReference<InternalData>(state, Allocator.Persistent);
            __connectionMap = new ConnectionDataMap<ConnectionData>(1, default, Allocator.Persistent);
            
            return 0;
        }

        /// <inheritdoc/>
        public int Bind(NetworkEndpoint endpoint)
        {
            return 0;
        }

        /// <inheritdoc/>
        public int Listen()
        {
            return 0;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            __internalData.Dispose();

            for (int i = 0; i < __connectionMap.Length; ++i)
            {
                WebSocket.Destroy(__connectionMap.DataAt(i).socket);
            }

            __connectionMap.Dispose();
            __connectionList.Dispose();
        }

        /// <inheritdoc/>
        public JobHandle ScheduleReceive(ref ReceiveJobArguments arguments, JobHandle dep)
        {
            ReceiveJob job;

            job.receiveQueue = arguments.ReceiveQueue;
            job.internalData = __internalData;
            job.connectionList = __connectionList;
            job.connectionMap = __connectionMap;
            job.time = arguments.Time;
            return job.ScheduleByRef(dep);
        }

        private struct ReceiveJob : IJob
        {
            public PacketsQueue receiveQueue;
            public NativeReference<InternalData> internalData;
            public ConnectionList connectionList;
            public ConnectionDataMap<ConnectionData> connectionMap;
            public long time;

            public unsafe void Execute()
            {
                // Update each connection from the connection list
                var count = connectionList.Count;
                for (int i = 0; i < count; i++)
                {
                    var connectionId = connectionList.ConnectionAt(i);
                    var connectionState = connectionList.GetConnectionState(connectionId);

                    if (connectionState == NetworkConnection.State.Disconnected)
                        continue;

                    var connectionData = connectionMap[connectionId];

                    // Detect if the upper layer is requesting to connect.
                    if (connectionState == NetworkConnection.State.Connecting)
                    {
                        // The time here is a signed 64bit and we're never going to run at time 0 so if the connection
                        // has ConnectStartTime == 0 it's the creation of this connection data.
                        if (connectionData.connectStartTime == 0)
                        {
                            var socket = ++WebSocket.s_NextSocketId;
                            var url = __GetServerURL(connectionId);

#if UNITY_TRANSPORT_TESTS_INSTALLED
                            Debug.Log(FixedString.Format("WebSocket: Connecting to {0}.", url));
#endif

                            WebSocket.Create(socket, (IntPtr)url.GetUnsafePtr(), url.Length);

                            connectionData.connectStartTime = time;
                            connectionData.socket = socket;
                        }

                        // Check if the WebSocket connection is established.
                        var status = WebSocket.IsConnectionReady(connectionData.socket);
                        if (status > 0)
                            connectionList.FinishConnectingFromLocal(ref connectionId);
                        else if (status < 0)
                        {
                            connectionList.StartDisconnecting(ref connectionId,
                                DisconnectReason.MaxConnectionAttempts);
                            __Abort(ref connectionId, ref connectionData);
                            continue;
                        }

                        // Disconnect if we've reached the maximum connection timeout.
                        if (time - connectionData.connectStartTime >= internalData.Value.connectTimeoutMS)
                        {
                            connectionList.StartDisconnecting(ref connectionId,
                                DisconnectReason.MaxConnectionAttempts);
                            __Abort(ref connectionId, ref connectionData);
                            continue;
                        }

                        connectionMap[connectionId] = connectionData;
                        continue;
                    }

                    // Detect if the upper layer is requesting to disconnect.
                    if (connectionState == NetworkConnection.State.Disconnecting)
                    {
                        __Abort(ref connectionId, ref connectionData);
                        continue;
                    }

                    // Read data from the connection if we can. Receive should return chunks of up to MTU.
                    // Close the connection in case of a receive error.
                    var endpoint = connectionList.GetConnectionEndpoint(connectionId);
                    var nbytes = 0;
                    while (true)
                    {
                        // No need to disconnect in case the receive queue becomes full just let the TCP socket buffer
                        // the incoming data.
                        if (!receiveQueue.EnqueuePacket(out var packetProcessor))
                            break;

                        nbytes = WebSocket.Recv(connectionData.socket,
                            (IntPtr)(byte*)packetProcessor.GetUnsafePayloadPtr() + packetProcessor.Offset,
                            packetProcessor.BytesAvailableAtEnd);
                        if (nbytes > 0)
                        {
                            packetProcessor.ConnectionRef = connectionId;
                            packetProcessor.EndpointRef = endpoint;
                            packetProcessor.SetUnsafeMetadata(nbytes, packetProcessor.Offset);
                        }
                        else
                        {
                            packetProcessor.Drop();
                            break;
                        }
                    }

                    if (nbytes < 0)
                    {
                        // Disconnect
                        connectionList.StartDisconnecting(ref connectionId, DisconnectReason.ClosedByRemote);
                        __Abort(ref connectionId, ref connectionData);
                        continue;
                    }

                    // Update the connection data
                    connectionMap[connectionId] = connectionData;
                }
            }

            private void __Abort(ref ConnectionId connectionId, ref ConnectionData connectionData)
            {
                connectionList.FinishDisconnecting(ref connectionId);
                connectionMap.ClearData(ref connectionId);
                WebSocket.Destroy(connectionData.socket);
            }

            // Get the address to connect to for the given connection.
            private FixedString512Bytes __GetServerURL(ConnectionId connection)
            {
                var endpoint = connectionList.GetConnectionEndpoint(connection);
                var secureHostname = internalData.Value.secureHostname;

                // If provided a secure hostname, then we're connecting over WSS.
                FixedString512Bytes url = secureHostname.IsEmpty ? "ws://" : "wss://";

                // If the address family is custom, we can assume that the user called the Connect
                // method that performs hostname resolution and thus the endpoint contains the
                // entire hostname plus the port. Also if a secure hostname was not provided, then
                // we can just grab the endpoint as is.
                if (endpoint.Family == NetworkFamily.Custom || secureHostname.IsEmpty)
                {
                    url.Append(endpoint.ToFixedString512Bytes());
                }
                else
                {
                    url.Append(secureHostname);
                    url.Append(':');
                    url.Append(endpoint.Port);
                }

                url.Append(internalData.Value.path);
                return url;
            }
        }

        /// <inheritdoc/>
        public JobHandle ScheduleSend(ref SendJobArguments arguments, JobHandle dep)
        {
            SendJob sendJob;
            sendJob.sendQueue = arguments.SendQueue;
            sendJob.connectionList = __connectionList;
            sendJob.connectionMap = __connectionMap;
            return sendJob.ScheduleByRef(dep);
        }

        [BurstCompile]
        private struct SendJob : IJob
        {
            public PacketsQueue sendQueue;
            public ConnectionList connectionList;
            public ConnectionDataMap<ConnectionData> connectionMap;

            private void Abort(ref ConnectionId connectionId, ref ConnectionData connectionData)
            {
                connectionList.FinishDisconnecting(ref connectionId);
                connectionMap.ClearData(ref connectionId);
                WebSocket.Destroy(connectionData.socket);
            }

            public unsafe void Execute()
            {
                // Each packet is sent individually. The connection is aborted if a packet cannot be transmiited
                // entirely.
                var count = sendQueue.Count;
                for (int i = 0; i < count; i++)
                {
                    var packetProcessor = sendQueue[i];
                    if (packetProcessor.Length == 0)
                        continue;

                    var connectionId = packetProcessor.ConnectionRef;
                    var connectionState = connectionList.GetConnectionState(connectionId);

                    if (connectionState != NetworkConnection.State.Connected)
                    {
                        packetProcessor.Drop();
                        continue;
                    }

                    var connectionData = connectionMap[connectionId];

                    var nbytes = WebSocket.Send(connectionData.socket,
                        (IntPtr)(byte*)packetProcessor.GetUnsafePayloadPtr() + packetProcessor.Offset,
                        packetProcessor.Length);
                    if (nbytes != packetProcessor.Length)
                    {
                        // Disconnect
                        connectionList.StartDisconnecting(ref connectionId, DisconnectReason.ClosedByRemote);
                        Abort(ref connectionId, ref connectionData);
                        continue;
                    }

                    connectionMap[connectionId] = connectionData;
                }
            }
        }
    }
}