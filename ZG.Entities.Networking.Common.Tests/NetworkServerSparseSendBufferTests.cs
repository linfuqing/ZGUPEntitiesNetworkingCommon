using System.Collections.Generic;
using NUnit.Framework;
using Unity.Collections;
using Unity.Jobs;
using Unity.Networking.Transport;

public class NetworkServerSparseSendBufferTests
{
    private struct SendJob : IJobParallelFor
    {
        [ReadOnly] public NativeArray<NetworkConnection> connections;
        [ReadOnly] public NativeHashMap<uint, NetworkPipeline> pipelines;
        public NetworkServerSendBuffer.Sender sender;
        public MultiNetworkDriver.Concurrent driver;
        public NativeArray<int> results;

        public void Execute(int index)
        {
            results[index] = sender.Send(
                connections[index],
                NetworkPipeline.Null,
                pipelines,
                ref driver)
                ? 1
                : 0;
        }
    }

    [Test]
    public void SourceOutbox_StoresOnlyActualMessages_InSourceOrder()
    {
        var outbox = new NetworkServerSendBuffer.SourceOutbox(Allocator.Temp);
        try
        {
            Assert.IsTrue(outbox.BeginWrite(0, out var allWriter, 8));
            allWriter.WriteByte(11);
            outbox.EndWrite(allWriter);

            int channelTarget = NetworkServerSendBuffer.GetTargetFromChannel(37);
            Assert.IsTrue(outbox.BeginWrite(channelTarget, out var channelWriter, 8));
            channelWriter.WriteByte(21);
            channelWriter.WriteByte(22);
            outbox.EndWrite(channelWriter);

            Assert.AreEqual(2, outbox.messageCount);
            Assert.AreEqual(3, outbox.payloadByteCount);
            Assert.AreEqual(0, outbox.GetMessage(0).target);
            Assert.AreEqual(channelTarget, outbox.GetMessage(1).target);

            var firstPayload = outbox.GetPayload(0);
            var secondPayload = outbox.GetPayload(1);
            Assert.AreEqual(1, firstPayload.Length);
            Assert.AreEqual(11, firstPayload[0]);
            Assert.AreEqual(2, secondPayload.Length);
            Assert.AreEqual(21, secondPayload[0]);
            Assert.AreEqual(22, secondPayload[1]);
        }
        finally
        {
            outbox.Dispose();
        }
    }

    [Test]
    public void SourceOutbox_ConsumePrefix_CompactsRemainingMessagesAndPayloads()
    {
        var outbox = new NetworkServerSendBuffer.SourceOutbox(Allocator.Temp);
        try
        {
            Assert.IsTrue(outbox.BeginWrite(0, out var firstWriter, 8));
            firstWriter.WriteByte(10);
            firstWriter.WriteByte(11);
            outbox.EndWrite(firstWriter);

            Assert.IsTrue(outbox.BeginWrite(1, 4321, out var secondWriter, 8));
            secondWriter.WriteByte(20);
            outbox.EndWrite(secondWriter);

            Assert.IsTrue(outbox.BeginWrite(-8, out var thirdWriter, 8));
            thirdWriter.WriteByte(30);
            thirdWriter.WriteByte(31);
            thirdWriter.WriteByte(32);
            outbox.EndWrite(thirdWriter);

            outbox.ConsumePrefix(1, out int consumedMessages, out int consumedBytes);

            Assert.AreEqual(1, consumedMessages);
            Assert.AreEqual(2, consumedBytes);
            Assert.AreEqual(2, outbox.messageCount);
            Assert.AreEqual(4, outbox.payloadByteCount);
            Assert.AreEqual(1, outbox.GetMessage(0).target);
            Assert.AreEqual(4321u, outbox.GetMessage(0).targetID);
            CollectionAssert.AreEqual(new byte[] { 20 }, outbox.GetPayload(0).ToArray());
            CollectionAssert.AreEqual(new byte[] { 30, 31, 32 }, outbox.GetPayload(1).ToArray());

            outbox.ConsumePrefix(2, out consumedMessages, out consumedBytes);
            Assert.AreEqual(2, consumedMessages);
            Assert.AreEqual(4, consumedBytes);
            Assert.AreEqual(0, outbox.messageCount);
            Assert.AreEqual(0, outbox.payloadByteCount);
        }
        finally
        {
            outbox.Dispose();
        }
    }

    [Test]
    public void SourceOutbox_RejectsOverlappingWrites_AndRecoversAfterEnd()
    {
        var outbox = new NetworkServerSendBuffer.SourceOutbox(Allocator.Temp);
        try
        {
            Assert.IsTrue(outbox.BeginWrite(0, out var firstWriter, 8));
            Assert.IsFalse(outbox.BeginWrite(0, out _, 8));

            firstWriter.WriteByte(1);
            outbox.EndWrite(firstWriter);

            Assert.IsTrue(outbox.BeginWrite(0, out var secondWriter, 8));
            secondWriter.WriteByte(2);
            outbox.EndWrite(secondWriter);
            Assert.AreEqual(2, outbox.messageCount);
        }
        finally
        {
            outbox.Dispose();
        }
    }

    [Test]
    public void DeliverySort_IsDeterministicBySourceThenLocalSequence()
    {
        var deliveries = new NativeArray<NetworkServerSendBuffer.Delivery>(
            4,
            Allocator.Temp);
        try
        {
            deliveries[0] = __Delivery(2, 1);
            deliveries[1] = __Delivery(0, 3);
            deliveries[2] = __Delivery(2, 0);
            deliveries[3] = __Delivery(0, 1);
            deliveries.Sort();

            Assert.AreEqual(0, deliveries[0].sourceIndex);
            Assert.AreEqual(1, deliveries[0].messageIndex);
            Assert.AreEqual(0, deliveries[1].sourceIndex);
            Assert.AreEqual(3, deliveries[1].messageIndex);
            Assert.AreEqual(2, deliveries[2].sourceIndex);
            Assert.AreEqual(0, deliveries[2].messageIndex);
            Assert.AreEqual(2, deliveries[3].sourceIndex);
            Assert.AreEqual(1, deliveries[3].messageIndex);
        }
        finally
        {
            deliveries.Dispose();
        }
    }

    [Test]
    public void RetryBuffer_EnforcesLimits_AndCompactsSentPrefix()
    {
        var buffer = new ZG.NetworkSendBuffer(Allocator.Temp);
        try
        {
            int initialByteCapacity = buffer.byteCapacity;
            using var first = new NativeArray<byte>(new byte[] { 1, 2 }, Allocator.Temp);
            using var second = new NativeArray<byte>(new byte[] { 3, 4, 5 }, Allocator.Temp);
            using var third = new NativeArray<byte>(new byte[] { 6 }, Allocator.Temp);

            Assert.IsTrue(buffer.TryAppendMessage(first, 2, 9));
            Assert.IsTrue(buffer.TryAppendMessage(second, 2, 9));
            Assert.AreEqual(2, buffer.messageCount);
            Assert.AreEqual(9, buffer.byteCount);
            Assert.IsFalse(buffer.TryAppendMessage(third, 2, 9));

            int sendIndex = 1;
            buffer.Compact(ref sendIndex);
            Assert.AreEqual(0, sendIndex);
            Assert.AreEqual(1, buffer.messageCount);
            Assert.AreEqual(5, buffer.byteCount);

            int readIndex = 0;
            Assert.IsTrue(buffer.ReadNext(ref readIndex, out var remainingPayload));
            CollectionAssert.AreEqual(new byte[] { 3, 4, 5 }, remainingPayload.ToArray());
            Assert.IsFalse(buffer.ReadNext(ref readIndex, out _));

            Assert.IsTrue(buffer.TryAppendMessage(third, 2, 9));
            Assert.AreEqual(2, buffer.messageCount);
            Assert.AreEqual(8, buffer.byteCount);

            buffer.Clear();
            using var large = new NativeArray<byte>(200, Allocator.Temp);
            Assert.IsTrue(buffer.TryAppendMessage(large, 1, 256));
            Assert.Greater(buffer.byteCapacity, initialByteCapacity);

            buffer.Reset();
            Assert.AreEqual(0, buffer.messageCount);
            Assert.AreEqual(0, buffer.byteCount);
            Assert.AreEqual(initialByteCapacity, buffer.byteCapacity);
        }
        finally
        {
            buffer.Dispose();
        }
    }

    [Test]
    public void DeliveryPlan_ContinuesAcrossTicks_WithoutDroppingOrDuplicatingRealTransportMessages()
    {
        const ushort port = 31973;
        const int channel = 7;
        uint[] ids = { 101, 102, 103 };
        var endpoint = NetworkEndpoint.LoopbackIpv4.WithPort(port);
        var serverDriver = NetworkDriver.Create(new IPCNetworkInterface());
        var multiDriver = default(MultiNetworkDriver);
        var clientDriver = default(NetworkDriver);
        var sendBuffer = default(NetworkServerSendBuffer);
        var clientConnections = default(NativeArray<NetworkConnection>);
        var serverConnections = default(NativeArray<NetworkConnection>);
        var acceptedByID = default(NativeHashMap<uint, NetworkConnection>);
        var pipelines = default(NativeHashMap<uint, NetworkPipeline>);
        var sendResults = default(NativeArray<int>);

        try
        {
            Assert.AreEqual(0, serverDriver.Bind(endpoint));
            Assert.AreEqual(0, serverDriver.Listen());
            multiDriver = MultiNetworkDriver.Create();
            multiDriver.AddDriver(serverDriver);
            serverDriver = default;

            clientDriver = NetworkDriver.Create(new IPCNetworkInterface());
            clientConnections = new NativeArray<NetworkConnection>(ids.Length, Allocator.TempJob);
            serverConnections = new NativeArray<NetworkConnection>(ids.Length, Allocator.TempJob);
            acceptedByID = new NativeHashMap<uint, NetworkConnection>(ids.Length, Allocator.TempJob);
            sendBuffer = new NetworkServerSendBuffer(
                Allocator.TempJob,
                maxPlannedDeliveryCount: 3);

            for (int i = 0; i < ids.Length; ++i)
            {
                using var payload = __CreateConnectionPayload(ids[i]);
                clientConnections[i] = clientDriver.Connect(endpoint, payload);
            }

            for (int iteration = 0; iteration < 32 && acceptedByID.Count < ids.Length; ++iteration)
            {
                clientDriver.ScheduleUpdate().Complete();
                multiDriver.ScheduleUpdate().Complete();

                NetworkConnection connection;
                while ((connection = multiDriver.Accept(out var payload)) != default)
                {
                    var acceptedConnection = connection;
                    uint id = sendBuffer.Connect(ref acceptedConnection, payload);
                    Assert.AreNotEqual(0u, id);
                    Assert.IsTrue(acceptedByID.TryAdd(id, acceptedConnection));
                }
            }

            Assert.AreEqual(ids.Length, acceptedByID.Count, "IPC clients did not connect to the test server.");
            for (int i = 0; i < ids.Length; ++i)
            {
                Assert.IsTrue(acceptedByID.TryGetValue(ids[i], out var connection));
                serverConnections[i] = connection;
            }

            int retryCapacityBaseline = sendBuffer.GetDiagnostics().retainedRetryByteCapacity;

            var writer = sendBuffer.AsWriter(false);
            Assert.IsTrue(writer.AddChannel(ids[0], channel));
            Assert.IsTrue(writer.AddChannel(ids[1], channel));

            __WriteAll(ref writer, ids[0], 10, 128);
            __WriteChannel(ref writer, ids[0], channel, 11);
            __WriteIdentity(ref writer, ids[1], ids[2], 20);
            __WriteAll(ref writer, ids[1], 21);
            __WriteIdentity(ref writer, ids[2], ids[0], 30);

            pipelines = new NativeHashMap<uint, NetworkPipeline>(1, Allocator.TempJob);
            sendResults = new NativeArray<int>(ids.Length, Allocator.TempJob);
            var received = new List<byte>[ids.Length];
            for (int i = 0; i < received.Length; ++i)
                received[i] = new List<byte>();

            int[] expectedPlanStatuses = { 1, 1, 0 };
            int[] expectedPlannedDeliveries = { 3, 2, 2 };
            int[] expectedRemainingMessages = { 3, 1, 0 };
            for (int tick = 0; tick < expectedPlanStatuses.Length; ++tick)
            {
                // Clear resets only transient routing state. Source messages deferred by the Tick
                // budget must remain available to the next plan.
                sendBuffer.Clear();
                sendBuffer.ScheduleDeliveryPlan(1, default).Complete();
                var beforeSend = sendBuffer.GetDiagnostics();
                Assert.AreEqual(3, beforeSend.activeConnectionCount);
                Assert.AreEqual(3, beforeSend.maxPlannedDeliveryCount);
                Assert.AreEqual(expectedPlanStatuses[tick], beforeSend.planStatus);
                Assert.AreEqual(expectedPlannedDeliveries[tick], beforeSend.plannedDeliveryCount);
                Assert.LessOrEqual(beforeSend.plannedDeliveryCount, beforeSend.maxPlannedDeliveryCount);

                var sendJob = new SendJob
                {
                    connections = serverConnections,
                    pipelines = pipelines,
                    sender = sendBuffer.AsSender(),
                    driver = multiDriver.ToConcurrent(),
                    results = sendResults
                };
                var sendHandle = sendJob.Schedule(ids.Length, 1);
                sendBuffer.ScheduleCompleteDeliveryPlan(1, sendHandle).Complete();
                multiDriver.ScheduleFlushSend().Complete();
                for (int i = 0; i < sendResults.Length; ++i)
                    Assert.AreEqual(1, sendResults[i], $"send failed for destination {i} on Tick {tick}");

                var afterSend = sendBuffer.GetDiagnostics();
                Assert.AreEqual(
                    expectedRemainingMessages[tick],
                    afterSend.deferredMessageCount,
                    $"wrong retained source-message count after Tick {tick}");
                Assert.AreEqual(0, afterSend.pendingRetryMessageCount);
                Assert.AreEqual(0, afterSend.pendingRetryByteCount);
                Assert.AreEqual(
                    retryCapacityBaseline,
                    afterSend.retainedRetryByteCapacity,
                    "The UTP fast path must not retain current-Tick broadcast payload capacity per destination.");

                for (int iteration = 0; iteration < 8; ++iteration)
                {
                    clientDriver.ScheduleUpdate().Complete();
                    for (int i = 0; i < clientConnections.Length; ++i)
                        __CollectPayloads(ref clientDriver, clientConnections[i], received[i]);
                }
            }

            CollectionAssert.AreEqual(new byte[] { 30, 21 }, received[0]);
            CollectionAssert.AreEqual(new byte[] { 10, 11 }, received[1]);
            CollectionAssert.AreEqual(new byte[] { 10, 20, 21 }, received[2]);

            // Messages whose direct target disconnects before planning have zero deliveries, but
            // they must still obey the Tick work budget rather than all collapsing into one frame.
            writer = sendBuffer.AsWriter(false);
            for (int i = 0; i < 10; ++i)
                __WriteIdentity(ref writer, ids[0], ids[2], (byte)(40 + i));

            Assert.AreEqual(ids[2], sendBuffer.Disconnect(serverConnections[2]));
            int[] expectedOfflineTargetBacklog = { 7, 4, 1, 0 };
            foreach (int expectedRemaining in expectedOfflineTargetBacklog)
            {
                sendBuffer.Clear();
                sendBuffer.ScheduleDeliveryPlan(1, default).Complete();
                var zeroFanoutPlan = sendBuffer.GetDiagnostics();
                Assert.AreEqual(0, zeroFanoutPlan.plannedDeliveryCount);
                Assert.LessOrEqual(zeroFanoutPlan.plannedMessageCount, 3);
                Assert.AreEqual(expectedRemaining == 0 ? 0 : 1, zeroFanoutPlan.planStatus);

                sendBuffer.ScheduleCompleteDeliveryPlan(1, default).Complete();
                Assert.AreEqual(expectedRemaining, sendBuffer.GetDiagnostics().deferredMessageCount);
            }
        }
        finally
        {
            if (sendResults.IsCreated)
                sendResults.Dispose();
            if (pipelines.IsCreated)
                pipelines.Dispose();
            if (acceptedByID.IsCreated)
                acceptedByID.Dispose();
            if (serverConnections.IsCreated)
                serverConnections.Dispose();
            if (clientConnections.IsCreated)
                clientConnections.Dispose();
            if (sendBuffer.connections.IsCreated)
                sendBuffer.Dispose();
            if (clientDriver.IsCreated)
                clientDriver.Dispose();
            if (multiDriver.IsCreated)
                multiDriver.Dispose();
            if (serverDriver.IsCreated)
                serverDriver.Dispose();
        }
    }

    private static NativeArray<byte> __CreateConnectionPayload(uint id)
    {
        var payload = new NativeArray<byte>(16, Allocator.Temp);
        var writer = new DataStreamWriter(payload);
        writer.WritePackedUInt(id, StreamCompressionModel.Default);
        return payload;
    }

    private static void __WriteAll(
        ref NetworkServerSendBuffer.Writer writer,
        uint sourceID,
        byte value,
        int payloadLength = 1)
    {
        Assert.IsTrue(writer.BeginWrite(sourceID, out var stream, (ushort)payloadLength));
        for (int i = 0; i < payloadLength; ++i)
            stream.WriteByte(value);
        writer.EndWrite(stream);
    }

    private static void __WriteChannel(
        ref NetworkServerSendBuffer.Writer writer,
        uint sourceID,
        int channel,
        byte value)
    {
        Assert.IsTrue(writer.BeginWrite(sourceID, channel, out var stream, 1));
        stream.WriteByte(value);
        writer.EndWrite(stream);
    }

    private static void __WriteIdentity(
        ref NetworkServerSendBuffer.Writer writer,
        uint sourceID,
        uint destinationID,
        byte value)
    {
        Assert.IsTrue(writer.BeginWrite(sourceID, destinationID, out var stream, 1));
        stream.WriteByte(value);
        writer.EndWrite(stream);
    }

    private static void __CollectPayloads(
        ref NetworkDriver driver,
        in NetworkConnection connection,
        List<byte> payloads)
    {
        NetworkEvent.Type eventType;
        while ((eventType = driver.PopEventForConnection(connection, out var reader)) != NetworkEvent.Type.Empty)
        {
            if (eventType != NetworkEvent.Type.Data)
                continue;

            while (reader.GetBytesRead() < reader.Length)
            {
                int payloadLength = reader.ReadUShort();
                Assert.Greater(payloadLength, 0);
                byte value = reader.ReadByte();
                for (int i = 1; i < payloadLength; ++i)
                    Assert.AreEqual(value, reader.ReadByte());

                payloads.Add(value);
            }
        }
    }

    private static NetworkServerSendBuffer.Delivery __Delivery(int sourceIndex, int messageIndex)
    {
        NetworkServerSendBuffer.Delivery result;
        result.sourceIndex = sourceIndex;
        result.messageIndex = messageIndex;
        result.destinationIndex = 0;
        result.order = ((ulong)(uint)sourceIndex << 32) | (uint)messageIndex;
        return result;
    }
}
