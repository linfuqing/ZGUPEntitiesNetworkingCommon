using System;
using System.Threading;
using Unity.Burst;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Jobs;
using Unity.Mathematics;
using Unity.Networking.Transport;
using ZG;

public interface INetworkServerSendBuffer
{
    int GetChannelIndex(uint id);

    NativeArray<byte> GetPayload(uint id);

    bool ContainsChannel(uint id, int value);

    bool AddChannel(uint id, int value);

    bool RemoveChannel(uint id, int value);

    bool BeginWrite(
        out DataStreamWriter writer, ushort capacity = 1024);

    bool BeginWrite(
        int channel,
        out DataStreamWriter writer, ushort capacity = 1024);

    bool BeginWrite(
        uint id,
        out DataStreamWriter writer, ushort capacity = 1024);

    void EndWrite(in DataStreamWriter writer);
}

/// <summary>
/// Server-side sparse message storage.
///
/// Producers own one <see cref="SourceOutbox"/> per active source connection. After all producer
/// jobs complete, <see cref="ScheduleDeliveryPlan"/> expands only the actual All/Channel/Identity
/// routes into destination-contiguous delivery spans. The send phase remains parallel per
/// destination while storage no longer scales as sourceConnections * historicalChannels.
/// </summary>
public struct NetworkServerSendBuffer
{
    public const int DefaultMaxPendingMessageCountPerConnection = 4096;
    public const int DefaultMaxPendingBytesPerConnection = 256 * 1024;
    public const int DefaultMaxPlannedDeliveryWorkPerTick = 256 * 1024;

    private struct ConnectionIndex
    {
        public int value;
        public int channelIndex;

        public int payloadOffset;
        public int payloadSize;
    }

    private struct SendBuffer
    {
        public int index;
        public NetworkSendBuffer value;

        public void Clear()
        {
            index = 0;
            value.Clear();
        }
    }

    private struct Channel
    {
        private UnsafeList<int> __values;

        public int count => __values.Length;

        public Channel(in AllocatorManager.AllocatorHandle allocator)
        {
            __values = new UnsafeList<int>(1, allocator);
        }

        public void Dispose()
        {
            __values.Dispose();
        }

        public bool Contains(int value)
        {
            return __values.Contains(value);
        }

        public bool Add(int value)
        {
            if (__values.Contains(value))
                return false;

            __values.Add(value);
            return true;
        }

        public bool Remove(int value)
        {
            int index = __values.IndexOf(value);
            if (index == -1)
                return false;

            __values.RemoveAtSwapBack(index);
            return true;
        }

        public UnsafeList<int>.Enumerator GetEnumerator() => __values.GetEnumerator();
    }

    internal struct OutboundMessage
    {
        // 0 = all, negative = channel, positive = stable identity target.
        public int target;
        public uint targetID;
        public int payloadOffset;
        public int payloadLength;
    }

    internal struct SourceOutbox
    {
        private UnsafeList<OutboundMessage> __messages;
        private UnsafeList<byte> __payloads;
        private int __pendingOffset;
        private int __pendingTarget;
        private uint __pendingTargetID;

        public int messageCount => __messages.Length;
        public int payloadByteCount => __payloads.Length;

        public SourceOutbox(in AllocatorManager.AllocatorHandle allocator)
        {
            __messages = new UnsafeList<OutboundMessage>(1, allocator);
            __payloads = new UnsafeList<byte>(1, allocator);
            __pendingOffset = -1;
            __pendingTarget = 0;
            __pendingTargetID = 0;
        }

        public void Dispose()
        {
            __messages.Dispose();
            __payloads.Dispose();
        }

        public void Clear()
        {
            __messages.Clear();
            __payloads.Clear();
            __pendingOffset = -1;
            __pendingTarget = 0;
            __pendingTargetID = 0;
        }

        public bool BeginWrite(int target, out DataStreamWriter writer, ushort capacity)
        {
            return BeginWrite(target, 0, out writer, capacity);
        }

        public bool BeginWrite(
            int target,
            uint targetID,
            out DataStreamWriter writer,
            ushort capacity)
        {
            if (capacity < 1 || __pendingOffset != -1)
            {
                writer = default;
                return false;
            }

            __pendingOffset = __payloads.Length;
            __pendingTarget = target;
            __pendingTargetID = targetID;
            __payloads.Resize(__pendingOffset + capacity, NativeArrayOptions.UninitializedMemory);
            unsafe
            {
                writer = new DataStreamWriter(__payloads.Ptr + __pendingOffset, capacity);
            }

            return true;
        }

        public void EndWrite(in DataStreamWriter writer)
        {
            if (__pendingOffset < 0)
                return;

            int payloadLength = writer.Length;
            if (payloadLength > 0)
            {
                OutboundMessage message;
                message.target = __pendingTarget;
                message.targetID = __pendingTargetID;
                message.payloadOffset = __pendingOffset;
                message.payloadLength = payloadLength;
                __messages.Add(message);
                __payloads.Resize(__pendingOffset + payloadLength, NativeArrayOptions.UninitializedMemory);
            }
            else
                __payloads.Resize(__pendingOffset, NativeArrayOptions.UninitializedMemory);

            __pendingOffset = -1;
            __pendingTarget = 0;
            __pendingTargetID = 0;
        }

        public void ConsumePrefix(
            int messageCount,
            out int consumedMessageCount,
            out int consumedPayloadByteCount)
        {
            int totalMessageCount = __messages.Length;
            consumedMessageCount = math.clamp(messageCount, 0, totalMessageCount);
            if (consumedMessageCount < 1)
            {
                consumedPayloadByteCount = 0;
                return;
            }

            var lastConsumed = __messages[consumedMessageCount - 1];
            consumedPayloadByteCount = lastConsumed.payloadOffset + lastConsumed.payloadLength;
            if (consumedMessageCount >= totalMessageCount)
            {
                Clear();
                return;
            }

            int remainingMessageCount = totalMessageCount - consumedMessageCount;
            int remainingPayloadByteCount = __payloads.Length - consumedPayloadByteCount;
            unsafe
            {
                UnsafeUtility.MemMove(
                    __payloads.Ptr,
                    __payloads.Ptr + consumedPayloadByteCount,
                    remainingPayloadByteCount);
                UnsafeUtility.MemMove(
                    __messages.Ptr,
                    __messages.Ptr + consumedMessageCount,
                    remainingMessageCount * UnsafeUtility.SizeOf<OutboundMessage>());
            }

            __payloads.Resize(remainingPayloadByteCount, NativeArrayOptions.UninitializedMemory);
            __messages.Resize(remainingMessageCount, NativeArrayOptions.UninitializedMemory);
            for (int i = 0; i < remainingMessageCount; ++i)
            {
                var message = __messages[i];
                message.payloadOffset -= consumedPayloadByteCount;
                __messages[i] = message;
            }
        }

        public OutboundMessage GetMessage(int index) => __messages[index];

        public NativeArray<byte> GetPayload(int messageIndex)
        {
            var message = __messages[messageIndex];
            unsafe
            {
                return CollectionHelper.ConvertExistingDataToNativeArray<byte>(
                    __payloads.Ptr + message.payloadOffset,
                    message.payloadLength,
                    Allocator.None,
                    true);
            }
        }
    }

    internal struct Delivery : IComparable<Delivery>
    {
        public ulong order;
        public int destinationIndex;
        public int sourceIndex;
        public int messageIndex;

        public int CompareTo(Delivery other)
        {
            int result = order.CompareTo(other.order);
            if (result != 0)
                return result;

            result = sourceIndex.CompareTo(other.sourceIndex);
            return result == 0 ? messageIndex.CompareTo(other.messageIndex) : result;
        }
    }

    private struct ChannelMember : IComparable<ChannelMember>
    {
        public int channel;
        public int destinationIndex;

        public int CompareTo(ChannelMember other)
        {
            int result = channel.CompareTo(other.channel);
            return result == 0 ? destinationIndex.CompareTo(other.destinationIndex) : result;
        }
    }

    private struct Core
    {
        [ReadOnly]
        private NativeHashMap<uint, ConnectionIndex> __connectionIndices;
        [ReadOnly]
        private NativeList<NetworkConnection> __connections;
        [ReadOnly]
        private NativeArray<byte> __payloads;

        [NativeDisableParallelForRestriction]
        private NativeArray<Channel> __channels;
        [NativeDisableContainerSafetyRestriction]
        [NativeDisableParallelForRestriction]
        private NativeArray<SourceOutbox> __outboxes;

        public Core(ref NetworkServerSendBuffer sendBuffer, bool isDeferredJob)
        {
            __connections = sendBuffer.__connections;
            __connectionIndices = sendBuffer.__connectionIndices;
            if (isDeferredJob)
            {
                __payloads = sendBuffer.__payloads.AsDeferredJobArray();
                __channels = sendBuffer.__channels.AsDeferredJobArray();
                __outboxes = sendBuffer.__outboxes.AsDeferredJobArray();
            }
            else
            {
                __payloads = sendBuffer.__payloads.AsArray();
                __channels = sendBuffer.__channels.AsArray();
                __outboxes = sendBuffer.__outboxes.AsArray();
            }

        }

        public NativeArray<byte> GetPayload(uint id)
        {
            var connectionIndex = __connectionIndices[id];
            return __payloads.GetSubArray(connectionIndex.payloadOffset, connectionIndex.payloadSize);
        }

        public int GetConnectionIndex(uint id) => __connectionIndices.TryGetValue(id, out var connectionIndex)
            ? connectionIndex.value
            : -1;

        public int GetChannelIndex(uint id) => __connectionIndices.TryGetValue(id, out var connectionIndex)
            ? connectionIndex.channelIndex
            : -1;

        public bool ContainsChannel(uint id, int value)
        {
            int channelIndex = GetChannelIndex(id);
            return channelIndex >= 0 && __channels[channelIndex].Contains(value);
        }

        public bool AddChannel(uint id, int value)
        {
            int channelIndex = GetChannelIndex(id);
            if (channelIndex < 0)
                return false;

            var channel = __channels[channelIndex];
            if (!channel.Add(value))
                return false;

            __channels[channelIndex] = channel;
            return true;
        }

        public bool RemoveChannel(uint id, int value)
        {
            int channelIndex = GetChannelIndex(id);
            if (channelIndex < 0)
                return false;

            var channel = __channels[channelIndex];
            if (!channel.Remove(value))
                return false;

            __channels[channelIndex] = channel;
            return true;
        }

        public bool BeginWrite(uint id, out DataStreamWriter writer, ushort capacity = 1024)
        {
            return __BeginWrite(capacity, 0, 0, id, out writer);
        }

        public bool BeginWrite(uint id, int channel, out DataStreamWriter writer, ushort capacity = 1024)
        {
            UnityEngine.Assertions.Assert.IsFalse(channel < 0);
            return __BeginWrite(capacity, GetTargetFromChannel(channel), 0, id, out writer);
        }

        public bool BeginWrite(uint id, uint targetID, out DataStreamWriter writer, ushort capacity = 1024)
        {
            if (!__connectionIndices.TryGetValue(targetID, out var connectionIndex) || connectionIndex.value == -1)
            {
                writer = default;
                return false;
            }

            return __BeginWrite(capacity, 1, targetID, id, out writer);
        }

        public void EndWrite(in DataStreamWriter writer)
        {
            int sourceIndex = (int)writer.m_SendHandleData;
            if ((uint)sourceIndex >= (uint)__connections.Length)
                return;

            var outbox = __outboxes[sourceIndex];
            outbox.EndWrite(writer);
            __outboxes[sourceIndex] = outbox;
        }

        private bool __BeginWrite(
            ushort capacity,
            int target,
            uint targetID,
            uint id,
            out DataStreamWriter writer)
        {
            if (!__connectionIndices.TryGetValue(id, out var connectionIndex) || connectionIndex.value == -1)
            {
                writer = default;
                return false;
            }

            int sourceIndex = connectionIndex.value;
            var outbox = __outboxes[sourceIndex];
            if (!outbox.BeginWrite(target, targetID, out writer, capacity))
                return false;

            __outboxes[sourceIndex] = outbox;
            writer.m_SendHandleData = (IntPtr)sourceIndex;
            return true;
        }
    }

    public struct Writer
    {
        private Core __core;

        public Writer(ref NetworkServerSendBuffer sendBuffer, bool isDeferredJob)
        {
            __core = new Core(ref sendBuffer, isDeferredJob);
        }

        public NativeArray<byte> GetPayload(uint id) => __core.GetPayload(id);
        public int GetConnectionIndex(uint id) => __core.GetConnectionIndex(id);
        public int GetChannelIndex(uint id) => __core.GetChannelIndex(id);
        public bool ContainsChannel(uint id, int value) => __core.ContainsChannel(id, value);
        public bool AddChannel(uint id, int value) => __core.AddChannel(id, value);
        public bool RemoveChannel(uint id, int value) => __core.RemoveChannel(id, value);
        public bool BeginWrite(uint id, out DataStreamWriter writer, ushort capacity = 1024) =>
            __core.BeginWrite(id, out writer, capacity);
        public bool BeginWrite(uint id, int channel, out DataStreamWriter writer, ushort capacity = 1024) =>
            __core.BeginWrite(id, channel, out writer, capacity);
        public bool BeginWrite(uint id, uint targetID, out DataStreamWriter writer, ushort capacity = 1024) =>
            __core.BeginWrite(id, targetID, out writer, capacity);
        public void EndWrite(in DataStreamWriter writer) => __core.EndWrite(writer);
    }

    public struct ParallelWriter
    {
        private Core __core;

        public ParallelWriter(ref NetworkServerSendBuffer sendBuffer)
        {
            __core = new Core(ref sendBuffer, true);
        }

        public NativeArray<byte> GetPayload(uint id) => __core.GetPayload(id);
        public int GetConnectionIndex(uint id) => __core.GetConnectionIndex(id);
        public int GetChannelIndex(uint id) => __core.GetChannelIndex(id);
        public bool ContainsChannel(uint id, int value) => __core.ContainsChannel(id, value);
        public bool AddChannel(uint id, int value) => __core.AddChannel(id, value);
        public bool RemoveChannel(uint id, int value) => __core.RemoveChannel(id, value);
        public bool BeginWrite(uint id, out DataStreamWriter writer, ushort capacity = 1024) =>
            __core.BeginWrite(id, out writer, capacity);
        public bool BeginWrite(uint id, int channel, out DataStreamWriter writer, ushort capacity = 1024) =>
            __core.BeginWrite(id, channel, out writer, capacity);
        public bool BeginWrite(uint id, uint targetID, out DataStreamWriter writer, ushort capacity = 1024) =>
            __core.BeginWrite(id, targetID, out writer, capacity);
        public void EndWrite(in DataStreamWriter writer) => __core.EndWrite(writer);
    }

    public struct ParallelIdentity : INetworkServerSendBuffer
    {
        public uint ID;
        private ParallelWriter __writer;

        public int connectionIndex => __writer.GetConnectionIndex(ID);
        public int channelIndex => __writer.GetChannelIndex(ID);

        public ParallelIdentity(uint id, ref ParallelWriter sendBuffer)
        {
            ID = id;
            __writer = sendBuffer;
        }

        public int GetChannelIndex(uint id) => __writer.GetChannelIndex(id);
        public NativeArray<byte> GetPayload(uint id) => __writer.GetPayload(id);
        public bool ContainsChannel(uint id, int value) => __writer.ContainsChannel(id, value);
        public bool AddChannel(uint id, int value) => __writer.AddChannel(id, value);
        public bool RemoveChannel(uint id, int value) => __writer.RemoveChannel(id, value);
        public bool BeginWrite(out DataStreamWriter writer, ushort capacity = 1024) =>
            __writer.BeginWrite(ID, out writer, capacity);
        public bool BeginWrite(int channel, out DataStreamWriter writer, ushort capacity = 1024) =>
            __writer.BeginWrite(ID, channel, out writer, capacity);
        public bool BeginWrite(uint id, out DataStreamWriter writer, ushort capacity = 1024) =>
            __writer.BeginWrite(ID, id, out writer, capacity);
        public void EndWrite(in DataStreamWriter writer) => __writer.EndWrite(writer);
    }

    public struct Identity : INetworkServerSendBuffer
    {
        public uint ID;
        private Writer __writer;

        public int connectionIndex => __writer.GetConnectionIndex(ID);
        public int channelIndex => __writer.GetChannelIndex(ID);

        public Identity(uint id, ref Writer writer)
        {
            ID = id;
            __writer = writer;
        }

        public Identity(uint id, ref NetworkServerSendBuffer sendBuffer)
        {
            ID = id;
            __writer = sendBuffer.AsWriter(false);
        }

        public int GetChannelIndex(uint id) => __writer.GetChannelIndex(id);
        public NativeArray<byte> GetPayload(uint id) => __writer.GetPayload(id);
        public bool ContainsChannel(uint id, int value) => __writer.ContainsChannel(id, value);
        public bool AddChannel(uint id, int value) => __writer.AddChannel(id, value);
        public bool RemoveChannel(uint id, int value) => __writer.RemoveChannel(id, value);
        public bool BeginWrite(out DataStreamWriter writer, ushort capacity = 1024) =>
            __writer.BeginWrite(ID, out writer, capacity);
        public bool BeginWrite(int channel, out DataStreamWriter writer, ushort capacity = 1024) =>
            __writer.BeginWrite(ID, channel, out writer, capacity);
        public bool BeginWrite(uint id, out DataStreamWriter writer, ushort capacity = 1024) =>
            __writer.BeginWrite(ID, id, out writer, capacity);
        public void EndWrite(in DataStreamWriter writer) => __writer.EndWrite(writer);
    }

    [BurstCompile]
    private struct CountMemberships : IJobParallelForDefer
    {
        [ReadOnly] public NativeArray<NetworkConnection> connections;
        [ReadOnly] public NativeHashMap<NetworkConnection, uint> connectionIDs;
        [ReadOnly] public NativeHashMap<uint, ConnectionIndex> connectionIndices;
        [ReadOnly] public NativeList<Channel> channels;
        [NativeDisableParallelForRestriction] public NativeArray<int> counts;

        public void Execute(int index)
        {
            uint id = connectionIDs[connections[index]];
            counts[index] = channels[connectionIndices[id].channelIndex].count;
        }
    }

    [BurstCompile]
    private struct PrepareMemberships : IJob
    {
        [ReadOnly] public NativeList<NetworkConnection> connections;
        [ReadOnly] public NativeList<int> counts;
        public NativeList<int> offsets;
        public NativeList<ChannelMember> members;

        public void Execute()
        {
            int total = 0;
            int connectionCount = connections.Length;
            offsets[0] = 0;
            for (int i = 0; i < connectionCount; ++i)
            {
                total += counts[i];
                offsets[i + 1] = total;
            }

            members.ResizeUninitialized(total);
        }
    }

    [BurstCompile]
    private struct FillMemberships : IJobParallelForDefer
    {
        [ReadOnly] public NativeArray<NetworkConnection> connections;
        [ReadOnly] public NativeHashMap<NetworkConnection, uint> connectionIDs;
        [ReadOnly] public NativeHashMap<uint, ConnectionIndex> connectionIndices;
        [ReadOnly] public NativeList<Channel> channels;
        [ReadOnly] public NativeArray<int> offsets;
        [NativeDisableParallelForRestriction] public NativeArray<ChannelMember> members;

        public void Execute(int index)
        {
            uint id = connectionIDs[connections[index]];
            var channel = channels[connectionIndices[id].channelIndex];
            int memberIndex = offsets[index];
            foreach (int value in channel)
            {
                ChannelMember member;
                member.channel = value;
                member.destinationIndex = index;
                members[memberIndex++] = member;
            }
        }
    }

    [BurstCompile]
    private struct SortMemberships : IJob
    {
        public NativeList<ChannelMember> members;

        public void Execute()
        {
            if (members.Length > 1)
                members.AsArray().Sort();
        }
    }

    [BurstCompile]
    private struct SelectMessages : IJob
    {
        [ReadOnly] public NativeList<NetworkConnection> connections;
        [ReadOnly] public NativeHashMap<uint, ConnectionIndex> connectionIndices;
        [ReadOnly] public NativeArray<ChannelMember> members;
        [NativeDisableContainerSafetyRestriction]
        [ReadOnly] public NativeArray<SourceOutbox> outboxes;
        public int configuredDeliveryBudget;
        public NativeList<int> plannedMessageCounts;
        public NativeArray<int> plannedDeliveryCount;
        public NativeArray<int> planSourceCursor;
        public NativeArray<int> planStatus;

        public void Execute()
        {
            int sourceCount = connections.Length;
            if (sourceCount < 1)
            {
                plannedDeliveryCount[0] = 0;
                planSourceCursor[0] = 0;
                planStatus[0] = 0;
                return;
            }

            int budget = math.max(configuredDeliveryBudget, sourceCount);
            int remaining = budget;
            int deliveryCount = 0;
            int sourceIndex = planSourceCursor[0] % sourceCount;
            if (sourceIndex < 0)
                sourceIndex += sourceCount;

            for (int i = 0; i < sourceCount; ++i)
                plannedMessageCounts[i] = 0;

            int skippedInARow = 0;
            int nextCursor = sourceIndex;
            while (skippedInARow < sourceCount)
            {
                int plannedMessageCount = plannedMessageCounts[sourceIndex];
                var outbox = outboxes[sourceIndex];
                if (plannedMessageCount < outbox.messageCount)
                {
                    int messageDeliveryCount = __CountDeliveries(
                        sourceIndex,
                        outbox.GetMessage(plannedMessageCount));
                    // Even a message whose target is now offline costs one unit to retire. This
                    // prevents zero-fanout backlog from bypassing the per-Tick work budget.
                    int workCount = math.max(1, messageDeliveryCount);
                    if (workCount <= remaining)
                    {
                        plannedMessageCounts[sourceIndex] = plannedMessageCount + 1;
                        remaining -= workCount;
                        deliveryCount += messageDeliveryCount;
                        skippedInARow = 0;
                        nextCursor = (sourceIndex + 1) % sourceCount;
                    }
                    else
                        ++skippedInARow;
                }
                else
                    ++skippedInARow;

                sourceIndex = (sourceIndex + 1) % sourceCount;
            }

            bool hasDeferred = false;
            for (int i = 0; i < sourceCount; ++i)
            {
                if (plannedMessageCounts[i] < outboxes[i].messageCount)
                {
                    hasDeferred = true;
                    break;
                }
            }

            plannedDeliveryCount[0] = deliveryCount;
            planSourceCursor[0] = nextCursor;
            planStatus[0] = hasDeferred ? 1 : 0;
        }

        private int __CountDeliveries(int sourceIndex, in OutboundMessage message)
        {
            int connectionCount = connections.Length;
            if (message.target == 0)
                return math.max(0, connectionCount - 1);

            if (message.target > 0)
                return connectionIndices.TryGetValue(message.targetID, out var destination) &&
                       destination.value >= 0
                    ? 1
                    : 0;

            int channel = -message.target - 1;
            __GetChannelMemberRange(members, channel, out int start, out int end);
            int result = end - start;
            if (__ContainsDestination(members, start, end, sourceIndex))
                --result;

            return result;
        }
    }

    [BurstCompile]
    private struct CountDestinationDeliveries : IJobParallelForDefer
    {
        [ReadOnly] public NativeList<NetworkConnection> connections;
        [ReadOnly] public NativeHashMap<uint, ConnectionIndex> connectionIndices;
        [ReadOnly] public NativeArray<ChannelMember> members;
        [ReadOnly] public NativeArray<int> plannedMessageCounts;
        [NativeDisableContainerSafetyRestriction]
        [ReadOnly] public NativeArray<SourceOutbox> outboxes;
        [NativeDisableParallelForRestriction] public NativeArray<int> destinationCounts;

        public void Execute(int sourceIndex)
        {
            int connectionCount = connections.Length;
            var outbox = outboxes[sourceIndex];
            int messageCount = plannedMessageCounts[sourceIndex];
            for (int messageIndex = 0; messageIndex < messageCount; ++messageIndex)
            {
                var message = outbox.GetMessage(messageIndex);
                if (message.target == 0)
                {
                    for (int destinationIndex = 0; destinationIndex < connectionCount; ++destinationIndex)
                    {
                        if (destinationIndex != sourceIndex)
                            __Count(destinationIndex);
                    }
                }
                else if (message.target > 0)
                {
                    if (connectionIndices.TryGetValue(message.targetID, out var destination) &&
                        destination.value >= 0)
                        __Count(destination.value);
                }
                else
                {
                    int channel = -message.target - 1;
                    __GetChannelMemberRange(members, channel, out int start, out int end);
                    for (int i = start; i < end; ++i)
                    {
                        int destinationIndex = members[i].destinationIndex;
                        if (destinationIndex != sourceIndex)
                            __Count(destinationIndex);
                    }
                }
            }
        }

        private void __Count(int destinationIndex)
        {
            Interlocked.Increment(ref destinationCounts.AsSpan()[destinationIndex]);
        }
    }

    [BurstCompile]
    private struct PrepareDeliveryCounts : IJob
    {
        [ReadOnly] public NativeList<NetworkConnection> connections;
        public NativeList<int> destinationCounts;

        public void Execute()
        {
            int connectionCount = connections.Length;
            for (int i = 0; i < connectionCount; ++i)
                destinationCounts[i] = 0;
        }
    }

    [BurstCompile]
    private struct FillDeliveries : IJobParallelForDefer
    {
        [ReadOnly] public NativeArray<NetworkConnection> connections;
        [ReadOnly] public NativeHashMap<uint, ConnectionIndex> connectionIndices;
        [ReadOnly] public NativeArray<ChannelMember> members;
        [ReadOnly] public NativeArray<int> destinationOffsets;
        [ReadOnly] public NativeArray<int> plannedMessageCounts;
        [ReadOnly] public NativeArray<int> planStatus;
        [NativeDisableContainerSafetyRestriction]
        [ReadOnly] public NativeArray<SourceOutbox> outboxes;
        [NativeDisableParallelForRestriction] public NativeArray<int> destinationCursors;
        [NativeDisableParallelForRestriction] public NativeArray<Delivery> orderedDeliveries;

        public void Execute(int sourceIndex)
        {
            if (planStatus[0] >= 2)
                return;

            int connectionCount = connections.Length;
            var outbox = outboxes[sourceIndex];
            int messageCount = plannedMessageCounts[sourceIndex];
            for (int messageIndex = 0; messageIndex < messageCount; ++messageIndex)
            {
                var message = outbox.GetMessage(messageIndex);
                int target = message.target;
                ulong order = ((ulong)(uint)sourceIndex << 32) | (uint)messageIndex;
                if (target == 0)
                {
                    for (int destinationIndex = 0; destinationIndex < connectionCount; ++destinationIndex)
                    {
                        if (destinationIndex != sourceIndex)
                            __Write(order, destinationIndex, sourceIndex, messageIndex);
                    }
                }
                else if (target > 0)
                {
                    if (connectionIndices.TryGetValue(message.targetID, out var destination) &&
                        destination.value >= 0)
                        __Write(order, destination.value, sourceIndex, messageIndex);
                }
                else
                {
                    int channel = -target - 1;
                    __GetChannelMemberRange(members, channel, out int start, out int end);
                    for (int i = start; i < end; ++i)
                    {
                        int destinationIndex = members[i].destinationIndex;
                        if (destinationIndex != sourceIndex)
                            __Write(order, destinationIndex, sourceIndex, messageIndex);
                    }
                }
            }
        }

        private void __Write(
            ulong order,
            int destinationIndex,
            int sourceIndex,
            int messageIndex)
        {
            Delivery delivery;
            delivery.order = order;
            delivery.destinationIndex = destinationIndex;
            delivery.sourceIndex = sourceIndex;
            delivery.messageIndex = messageIndex;
            int localIndex = Interlocked.Increment(
                ref destinationCursors.AsSpan()[destinationIndex]) - 1;
            orderedDeliveries[destinationOffsets[destinationIndex] + localIndex] = delivery;
        }
    }

    [BurstCompile]
    private struct PrepareDestinationSpans : IJob
    {
        [ReadOnly] public NativeList<NetworkConnection> connections;
        [ReadOnly] public NativeArray<int> plannedDeliveryCount;
        [ReadOnly] public NativeList<int> destinationCounts;
        public NativeArray<int> planStatus;
        public NativeList<int> offsets;
        public NativeList<int> cursors;
        public NativeList<Delivery> orderedDeliveries;

        public void Execute()
        {
            int connectionCount = connections.Length;
            int expectedDeliveryCount = plannedDeliveryCount[0];

            int total = 0;
            offsets[0] = 0;
            for (int i = 0; i < connectionCount; ++i)
            {
                total += destinationCounts[i];
                offsets[i + 1] = total;
                cursors[i] = 0;
            }

            if (total != expectedDeliveryCount)
            {
                planStatus[0] = 2;
                orderedDeliveries.Clear();
                for (int i = 0; i <= connectionCount; ++i)
                    offsets[i] = 0;
                return;
            }

            orderedDeliveries.ResizeUninitialized(total);
        }
    }

    [BurstCompile]
    private struct SortDestinationSpans : IJobParallelForDefer
    {
        [ReadOnly] public NativeList<NetworkConnection> connections;
        [ReadOnly] public NativeArray<int> offsets;
        [ReadOnly] public NativeArray<int> planStatus;
        [NativeDisableParallelForRestriction] public NativeArray<Delivery> orderedDeliveries;

        public void Execute(int destinationIndex)
        {
            if (planStatus[0] >= 2)
                return;

            int start = offsets[destinationIndex];
            int count = offsets[destinationIndex + 1] - start;
            if (count > 1)
                orderedDeliveries.GetSubArray(start, count).Sort();
        }
    }

    /// <summary>
    /// Commits the planned source prefixes only after every destination send job has either handed
    /// those payloads to UTP or copied its unsent suffix into a persistent retry buffer. A status of
    /// 1 means more source messages remain for a later Tick; only status 2 is a planner consistency
    /// fault and must retain everything.
    /// </summary>
    [BurstCompile]
    private struct CompleteDeliveryPlan : IJobParallelForDefer
    {
        [ReadOnly] public NativeArray<NetworkConnection> connections;
        [ReadOnly] public NativeArray<int> plannedMessageCounts;
        [ReadOnly] public NativeArray<int> planStatus;
        [NativeDisableContainerSafetyRestriction]
        [NativeDisableParallelForRestriction]
        public NativeArray<SourceOutbox> outboxes;

        public void Execute(int sourceIndex)
        {
            if (planStatus[0] >= 2)
                return;

            int messageCount = plannedMessageCounts[sourceIndex];
            if (messageCount < 1)
                return;

            var outbox = outboxes[sourceIndex];
            outbox.ConsumePrefix(messageCount, out _, out _);
            outboxes[sourceIndex] = outbox;
        }
    }

    public struct Sender
    {
        [ReadOnly] private NativeHashMap<NetworkConnection, uint> __connectionIDs;
        [ReadOnly] private NativeHashMap<uint, ConnectionIndex> __connectionIndices;
        [NativeDisableContainerSafetyRestriction]
        [ReadOnly] private NativeList<SourceOutbox> __outboxes;
        [ReadOnly] private NativeList<Delivery> __orderedDeliveries;
        [ReadOnly] private NativeList<int> __destinationOffsets;
        [ReadOnly] private NativeArray<int> __planStatus;
        private NativeArray<SendBuffer> __sendBuffers;
        private int __maxPendingMessageCount;
        private int __maxPendingByteCount;

        public Sender(ref NetworkServerSendBuffer sendBuffer)
        {
            __connectionIDs = sendBuffer.__connectionIDs;
            __connectionIndices = sendBuffer.__connectionIndices;
            __outboxes = sendBuffer.__outboxes;
            __orderedDeliveries = sendBuffer.__orderedDeliveries;
            __destinationOffsets = sendBuffer.__destinationOffsets;
            __planStatus = sendBuffer.__planStatus;
            __sendBuffers = sendBuffer.__sendBuffers.AsDeferredJobArray();
            __maxPendingMessageCount = sendBuffer.MaxPendingMessageCountPerConnection;
            __maxPendingByteCount = sendBuffer.MaxPendingBytesPerConnection;
        }

        /// <returns>False when the destination exceeded its bounded retry queue.</returns>
        public bool Send(
            in NetworkConnection connection,
            in NetworkPipeline pipeline,
            in NativeHashMap<uint, NetworkPipeline> pipelines,
            ref MultiNetworkDriver.Concurrent driver)
        {
            uint id = __connectionIDs[connection];
            var targetPipeline = pipelines.TryGetValue(id, out var temp) ? temp : pipeline;
            var connectionIndex = __connectionIndices[id];
            int destinationIndex = connectionIndex.value;
            var sendBuffer = __sendBuffers[destinationIndex];
            sendBuffer.value.Compact(ref sendBuffer.index);

            int start = 0;
            int end = 0;
            if (__planStatus[0] < 2)
            {
                start = __destinationOffsets[destinationIndex];
                end = __destinationOffsets[destinationIndex + 1];
            }

            if (sendBuffer.value.messageCount > 0)
            {
                if (sendBuffer.value.Apply(
                        connection,
                        targetPipeline,
                        ref driver,
                        ref sendBuffer.index))
                {
                    // Persistent storage exists only for real queue-full retries. Release its peak
                    // capacity after recovery so one large burst is not retained per connection.
                    sendBuffer.value.Reset();
                    sendBuffer.index = 0;
                }
                else
                {
                    sendBuffer.value.Compact(ref sendBuffer.index);
                    bool isWithinLimit = __AppendDeliveries(ref sendBuffer, start, end);
                    __sendBuffers[destinationIndex] = sendBuffer;
                    return isWithinLimit;
                }
            }

            bool result = __SendDirect(
                connection,
                targetPipeline,
                ref driver,
                ref sendBuffer,
                start,
                end);

            __sendBuffers[destinationIndex] = sendBuffer;
            return result;
        }

        private bool __SendDirect(
            in NetworkConnection connection,
            in NetworkPipeline pipeline,
            ref MultiNetworkDriver.Concurrent driver,
            ref SendBuffer sendBuffer,
            int start,
            int end)
        {
            DataStreamWriter writer = default;
            int packetStart = start;
            int index = start;
            while (index < end)
            {
                var delivery = __orderedDeliveries[index];
                var payload = __outboxes[delivery.sourceIndex].GetPayload(delivery.messageIndex);
                int framedByteCount = UnsafeUtility.SizeOf<ushort>() + payload.Length;
                if (!writer.IsCreated)
                {
                    int beginResult = driver.BeginSend(
                        pipeline,
                        connection,
                        out writer,
                        framedByteCount);
                    if (beginResult != (int)Unity.Networking.Transport.Error.StatusCode.Success)
                    {
                        __LogSendError(beginResult);
                        return __AppendDeliveries(ref sendBuffer, index, end);
                    }

                    packetStart = index;
                }

                if (writer.Capacity - writer.Length < framedByteCount)
                {
                    if (writer.Length < 1)
                    {
                        driver.AbortSend(writer);
                        return __AppendDeliveries(ref sendBuffer, index, end);
                    }

                    int endResult = driver.EndSend(writer);
                    writer = default;
                    if (endResult < 0)
                    {
                        __LogSendError(endResult);
                        return __AppendDeliveries(ref sendBuffer, packetStart, end);
                    }

                    continue;
                }

                writer.WriteUShort((ushort)payload.Length);
                if (!writer.WriteBytes(payload))
                {
                    // BeginSend(requiredPayloadSize) and the capacity check above make this an
                    // internal consistency fault. Preserve the entire uncommitted packet suffix.
                    driver.AbortSend(writer);
                    writer = default;
                    return __AppendDeliveries(ref sendBuffer, packetStart, end);
                }

                ++index;
            }

            if (writer.IsCreated)
            {
                int endResult = driver.EndSend(writer);
                if (endResult < 0)
                {
                    __LogSendError(endResult);
                    return __AppendDeliveries(ref sendBuffer, packetStart, end);
                }
            }

            return true;
        }

        private bool __AppendDeliveries(ref SendBuffer sendBuffer, int start, int end)
        {
            for (int i = start; i < end; ++i)
            {
                var delivery = __orderedDeliveries[i];
                var payload = __outboxes[delivery.sourceIndex].GetPayload(delivery.messageIndex);
                if (!sendBuffer.value.TryAppendMessage(
                        payload,
                        __maxPendingMessageCount,
                        __maxPendingByteCount))
                    return false;
            }

            return true;
        }

        private static void __LogSendError(int result)
        {
            var statusCode = (Unity.Networking.Transport.Error.StatusCode)result;
            if (statusCode != Unity.Networking.Transport.Error.StatusCode.NetworkSendQueueFull)
                NetworkSendBuffer.LogError(statusCode);
        }
    }

    public struct ReadOnly
    {
        [ReadOnly] private NativeHashMap<uint, ConnectionIndex> __connectionIndices;
        [ReadOnly] private NativeList<byte> __payloads;

        public ReadOnly(ref NetworkServerSendBuffer sendBuffer)
        {
            __connectionIndices = sendBuffer.__connectionIndices;
            __payloads = sendBuffer.__payloads;
        }

        public bool GetConnection(uint id, out int connectionIndex, out int channelIndex, out NativeArray<byte> payload)
        {
            if (!__connectionIndices.TryGetValue(id, out var temp))
            {
                connectionIndex = -1;
                channelIndex = -1;
                payload = default;
                return false;
            }

            connectionIndex = temp.value;
            channelIndex = temp.channelIndex;
            payload = __payloads.AsArray().GetSubArray(temp.payloadOffset, temp.payloadSize);
            return true;
        }
    }

    public struct Diagnostics
    {
        public int activeConnectionCount;
        public int knownIdentityCount;
        public int retainedOutboxSlotCount;
        public int deferredMessageCount;
        public int deferredPayloadByteCount;
        public int pendingRetryMessageCount;
        public int pendingRetryByteCount;
        public int retainedRetryByteCapacity;
        public int plannedMessageCount;
        public int plannedDeliveryCount;
        public int plannedDeliveryCapacity;
        public int maxPlannedDeliveryCount;
        public int planStatus;
    }

    private NativeHashMap<NetworkConnection, uint> __connectionIDs;
    private NativeHashMap<uint, ConnectionIndex> __connectionIndices;
    private NativeList<NetworkConnection> __connections;
    private NativeList<byte> __payloads;
    private NativeList<Channel> __channels;
    private NativeList<SendBuffer> __sendBuffers;
    private NativeList<SourceOutbox> __outboxes;

    private NativeList<int> __membershipCounts;
    private NativeList<int> __membershipOffsets;
    private NativeList<ChannelMember> __activeChannelMembers;
    private NativeList<int> __plannedMessageCounts;
    private NativeList<int> __destinationCounts;
    private NativeList<int> __destinationOffsets;
    private NativeList<int> __destinationCursors;
    private NativeList<Delivery> __orderedDeliveries;
    private NativeArray<int> __plannedDeliveryCount;
    private NativeArray<int> __planSourceCursor;
    private NativeArray<int> __planStatus;

    public readonly int ChannelCount;
    public readonly int MaxPendingMessageCountPerConnection;
    public readonly int MaxPendingBytesPerConnection;
    public readonly int MaxPlannedDeliveryCount;

    public int channelCount => ChannelCount == 0 ? __channels.Length : ChannelCount;
    public unsafe AllocatorManager.AllocatorHandle allocator => __connections.GetUnsafeList()->Allocator;
    public NativeList<NetworkConnection> connections => __connections;
    public NativeHashMap<NetworkConnection, uint> connectionIDs => __connectionIDs;

    public NetworkServerSendBuffer(
        in AllocatorManager.AllocatorHandle allocator,
        int channelCount = 0,
        int maxPendingMessageCountPerConnection = DefaultMaxPendingMessageCountPerConnection,
        int maxPendingBytesPerConnection = DefaultMaxPendingBytesPerConnection,
        int maxPlannedDeliveryCount = DefaultMaxPlannedDeliveryWorkPerTick)
    {
        ChannelCount = channelCount;
        MaxPendingMessageCountPerConnection = math.max(1, maxPendingMessageCountPerConnection);
        MaxPendingBytesPerConnection = math.max(
            UnsafeUtility.SizeOf<ushort>() + 1,
            maxPendingBytesPerConnection);
        MaxPlannedDeliveryCount = math.max(1, maxPlannedDeliveryCount);

        __connectionIDs = new NativeHashMap<NetworkConnection, uint>(1, allocator);
        __connectionIndices = new NativeHashMap<uint, ConnectionIndex>(1, allocator);
        __connections = new NativeList<NetworkConnection>(allocator);
        __payloads = new NativeList<byte>(allocator);
        __channels = new NativeList<Channel>(allocator);
        __sendBuffers = new NativeList<SendBuffer>(allocator);
        __outboxes = new NativeList<SourceOutbox>(allocator);

        __membershipCounts = new NativeList<int>(allocator);
        __membershipOffsets = new NativeList<int>(1, allocator);
        __membershipOffsets.Add(0);
        __activeChannelMembers = new NativeList<ChannelMember>(allocator);
        __plannedMessageCounts = new NativeList<int>(allocator);
        __destinationCounts = new NativeList<int>(allocator);
        __destinationOffsets = new NativeList<int>(1, allocator);
        __destinationOffsets.Add(0);
        __destinationCursors = new NativeList<int>(allocator);
        __orderedDeliveries = new NativeList<Delivery>(allocator);
        __plannedDeliveryCount = CollectionHelper.CreateNativeArray<int>(
            1,
            allocator,
            NativeArrayOptions.ClearMemory);
        __planSourceCursor = CollectionHelper.CreateNativeArray<int>(
            1,
            allocator,
            NativeArrayOptions.ClearMemory);
        __planStatus = CollectionHelper.CreateNativeArray<int>(
            1,
            allocator,
            NativeArrayOptions.ClearMemory);
    }

    public void Dispose()
    {
        __connectionIDs.Dispose();
        __connectionIndices.Dispose();
        __connections.Dispose();
        __payloads.Dispose();

        foreach (var channel in __channels)
            channel.Dispose();
        __channels.Dispose();

        foreach (var sendBuffer in __sendBuffers)
            sendBuffer.value.Dispose();
        __sendBuffers.Dispose();

        foreach (var outbox in __outboxes)
            outbox.Dispose();
        __outboxes.Dispose();

        __membershipCounts.Dispose();
        __membershipOffsets.Dispose();
        __activeChannelMembers.Dispose();
        __plannedMessageCounts.Dispose();
        __destinationCounts.Dispose();
        __destinationOffsets.Dispose();
        __destinationCursors.Dispose();
        __orderedDeliveries.Dispose();
        __plannedDeliveryCount.Dispose();
        __planSourceCursor.Dispose();
        __planStatus.Dispose();
    }

    public void Clear()
    {
        // Source outboxes intentionally persist across Ticks. The delivery budget is a throughput
        // quota, not a drop policy; CompleteDeliveryPlan consumes only prefixes copied by Sender.
        __activeChannelMembers.Clear();
        __orderedDeliveries.Clear();
        __plannedDeliveryCount[0] = 0;
        __planStatus[0] = 0;
    }

    public Writer AsWriter(bool isDeferredJob) => new Writer(ref this, isDeferredJob);
    public ParallelWriter AsParallelWriter() => new ParallelWriter(ref this);
    public Sender AsSender() => new Sender(ref this);
    public ReadOnly AsReadOnly() => new ReadOnly(ref this);

    public Diagnostics GetDiagnostics()
    {
        Diagnostics result;
        result.activeConnectionCount = __connections.Length;
        result.knownIdentityCount = __connectionIndices.Count;
        result.retainedOutboxSlotCount = __outboxes.Length;
        result.deferredMessageCount = 0;
        result.deferredPayloadByteCount = 0;
        result.pendingRetryMessageCount = 0;
        result.pendingRetryByteCount = 0;
        result.retainedRetryByteCapacity = 0;
        result.plannedMessageCount = 0;
        for (int i = 0; i < __connections.Length; ++i)
        {
            result.deferredMessageCount += __outboxes[i].messageCount;
            result.deferredPayloadByteCount += __outboxes[i].payloadByteCount;
            result.pendingRetryMessageCount += __sendBuffers[i].value.messageCount;
            result.pendingRetryByteCount += __sendBuffers[i].value.byteCount;
            result.retainedRetryByteCapacity += __sendBuffers[i].value.byteCapacity;
            result.plannedMessageCount += __plannedMessageCounts[i];
        }
        result.plannedDeliveryCount = __orderedDeliveries.Length;
        result.plannedDeliveryCapacity = __orderedDeliveries.Capacity;
        result.maxPlannedDeliveryCount = MaxPlannedDeliveryCount;
        result.planStatus = __planStatus[0];
        return result;
    }

    public JobHandle ScheduleDeliveryPlan(int innerloopBatchCount, in JobHandle inputDeps)
    {
        var connectionList = __connections;
        JobHandle jobHandle = inputDeps;

        CountMemberships countMemberships;
        countMemberships.connections = connectionList.AsDeferredJobArray();
        countMemberships.connectionIDs = __connectionIDs;
        countMemberships.connectionIndices = __connectionIndices;
        countMemberships.channels = __channels;
        countMemberships.counts = __membershipCounts.AsDeferredJobArray();
        jobHandle = countMemberships.ScheduleByRef(connectionList, innerloopBatchCount, jobHandle);

        PrepareMemberships prepareMemberships;
        prepareMemberships.connections = connectionList;
        prepareMemberships.counts = __membershipCounts;
        prepareMemberships.offsets = __membershipOffsets;
        prepareMemberships.members = __activeChannelMembers;
        jobHandle = prepareMemberships.ScheduleByRef(jobHandle);

        FillMemberships fillMemberships;
        fillMemberships.connections = connectionList.AsDeferredJobArray();
        fillMemberships.connectionIDs = __connectionIDs;
        fillMemberships.connectionIndices = __connectionIndices;
        fillMemberships.channels = __channels;
        fillMemberships.offsets = __membershipOffsets.AsDeferredJobArray();
        fillMemberships.members = __activeChannelMembers.AsDeferredJobArray();
        jobHandle = fillMemberships.ScheduleByRef(connectionList, innerloopBatchCount, jobHandle);

        SortMemberships sortMemberships;
        sortMemberships.members = __activeChannelMembers;
        jobHandle = sortMemberships.ScheduleByRef(jobHandle);

        SelectMessages selectMessages;
        selectMessages.connections = connectionList;
        selectMessages.connectionIndices = __connectionIndices;
        selectMessages.members = __activeChannelMembers.AsDeferredJobArray();
        selectMessages.outboxes = __outboxes.AsDeferredJobArray();
        selectMessages.configuredDeliveryBudget = MaxPlannedDeliveryCount;
        selectMessages.plannedMessageCounts = __plannedMessageCounts;
        selectMessages.plannedDeliveryCount = __plannedDeliveryCount;
        selectMessages.planSourceCursor = __planSourceCursor;
        selectMessages.planStatus = __planStatus;
        jobHandle = selectMessages.ScheduleByRef(jobHandle);

        PrepareDeliveryCounts prepareDeliveryCounts;
        prepareDeliveryCounts.connections = connectionList;
        prepareDeliveryCounts.destinationCounts = __destinationCounts;
        jobHandle = prepareDeliveryCounts.ScheduleByRef(jobHandle);

        CountDestinationDeliveries countDestinationDeliveries;
        countDestinationDeliveries.connections = connectionList;
        countDestinationDeliveries.connectionIndices = __connectionIndices;
        countDestinationDeliveries.members = __activeChannelMembers.AsDeferredJobArray();
        countDestinationDeliveries.plannedMessageCounts = __plannedMessageCounts.AsDeferredJobArray();
        countDestinationDeliveries.outboxes = __outboxes.AsDeferredJobArray();
        countDestinationDeliveries.destinationCounts = __destinationCounts.AsDeferredJobArray();
        jobHandle = countDestinationDeliveries.ScheduleByRef(connectionList, innerloopBatchCount, jobHandle);

        PrepareDestinationSpans prepareDestinationSpans;
        prepareDestinationSpans.connections = connectionList;
        prepareDestinationSpans.plannedDeliveryCount = __plannedDeliveryCount;
        prepareDestinationSpans.destinationCounts = __destinationCounts;
        prepareDestinationSpans.planStatus = __planStatus;
        prepareDestinationSpans.offsets = __destinationOffsets;
        prepareDestinationSpans.cursors = __destinationCursors;
        prepareDestinationSpans.orderedDeliveries = __orderedDeliveries;
        jobHandle = prepareDestinationSpans.ScheduleByRef(jobHandle);

        FillDeliveries fillDeliveries;
        fillDeliveries.connections = connectionList.AsDeferredJobArray();
        fillDeliveries.connectionIndices = __connectionIndices;
        fillDeliveries.members = __activeChannelMembers.AsDeferredJobArray();
        fillDeliveries.destinationOffsets = __destinationOffsets.AsDeferredJobArray();
        fillDeliveries.plannedMessageCounts = __plannedMessageCounts.AsDeferredJobArray();
        fillDeliveries.planStatus = __planStatus;
        fillDeliveries.outboxes = __outboxes.AsDeferredJobArray();
        fillDeliveries.destinationCursors = __destinationCursors.AsDeferredJobArray();
        fillDeliveries.orderedDeliveries = __orderedDeliveries.AsDeferredJobArray();
        jobHandle = fillDeliveries.ScheduleByRef(connectionList, innerloopBatchCount, jobHandle);

        SortDestinationSpans sortDestinationSpans;
        sortDestinationSpans.connections = connectionList;
        sortDestinationSpans.offsets = __destinationOffsets.AsDeferredJobArray();
        sortDestinationSpans.planStatus = __planStatus;
        sortDestinationSpans.orderedDeliveries = __orderedDeliveries.AsDeferredJobArray();
        jobHandle = sortDestinationSpans.ScheduleByRef(connectionList, innerloopBatchCount, jobHandle);
        return jobHandle;
    }

    /// <summary>
    /// Consumes only the source-message prefixes selected for the current Tick. Call this after all
    /// Sender jobs: every selected payload has then either entered UTP, entered the destination's
    /// persistent retry queue, or caused only that over-cap destination to be disconnected.
    /// </summary>
    public JobHandle ScheduleCompleteDeliveryPlan(int innerloopBatchCount, in JobHandle inputDeps)
    {
        CompleteDeliveryPlan completeDeliveryPlan;
        completeDeliveryPlan.connections = __connections.AsDeferredJobArray();
        completeDeliveryPlan.plannedMessageCounts = __plannedMessageCounts.AsDeferredJobArray();
        completeDeliveryPlan.planStatus = __planStatus;
        completeDeliveryPlan.outboxes = __outboxes.AsDeferredJobArray();
        return completeDeliveryPlan.ScheduleByRef(__connections, innerloopBatchCount, inputDeps);
    }

    public uint Connect(ref NetworkConnection connection, in NativeArray<byte> payload)
    {
        if (!payload.IsCreated || payload.Length < 4)
            return 0;

        uint id = new DataStreamReader(payload).ReadPackedUInt(StreamCompressionModel.Default);
        var allocator = this.allocator;
        if (__connectionIndices.TryGetValue(id, out var connectionIndex))
        {
            if (connectionIndex.value != -1)
            {
                connection = __connections[connectionIndex.value];
                return 0;
            }

            if (connectionIndex.payloadSize != payload.Length)
            {
                UnityEngine.Debug.LogError($"Payload size mismatch: {connectionIndex.payloadSize} != {payload.Length}");
                return 0;
            }

            NativeArray<byte>.Copy(
                payload,
                0,
                __payloads.AsArray(),
                connectionIndex.payloadOffset,
                connectionIndex.payloadSize);
        }
        else
        {
            UnityEngine.Assertions.Assert.AreEqual(__connectionIndices.Count, __channels.Length);
            connectionIndex.channelIndex = __channels.Length;
            __channels.Add(new Channel(allocator));
            connectionIndex.payloadOffset = __payloads.Length;
            connectionIndex.payloadSize = payload.Length;
            __payloads.AddRange(payload);
        }

        connectionIndex.value = __connections.Length;
        __connections.Add(connection);
        __connectionIndices[id] = connectionIndex;
        __connectionIDs.Add(connection, id);

        int connectionCount = __connections.Length;
        UnityEngine.Assertions.Assert.AreEqual(connectionCount, __connectionIDs.Count);
        if (__sendBuffers.Length < connectionCount)
        {
            SendBuffer sendBuffer;
            sendBuffer.index = 0;
            sendBuffer.value = new NetworkSendBuffer(allocator);
            __sendBuffers.Add(sendBuffer);
            __outboxes.Add(new SourceOutbox(allocator));
        }
        else
        {
            var sendBuffer = __sendBuffers[connectionIndex.value];
            sendBuffer.Clear();
            __sendBuffers[connectionIndex.value] = sendBuffer;

            var outbox = __outboxes[connectionIndex.value];
            outbox.Clear();
            __outboxes[connectionIndex.value] = outbox;
        }

        __EnsureLength(ref __membershipCounts, connectionCount);
        __EnsureLength(ref __membershipOffsets, connectionCount + 1);
        __EnsureLength(ref __plannedMessageCounts, connectionCount);
        __EnsureLength(ref __destinationCounts, connectionCount);
        __EnsureLength(ref __destinationOffsets, connectionCount + 1);
        __EnsureLength(ref __destinationCursors, connectionCount);

        __Log($"Connect {id}(index {connectionIndex.value}, channel {connectionIndex.channelIndex}, payload size {payload.Length})");
        return id;
    }

    public uint Disconnect(in NetworkConnection connection)
    {
        if (!__connectionIDs.TryGetValue(connection, out var id))
            return 0;

        __connectionIDs.Remove(connection);
        var connectionIndex = __connectionIndices[id];
        int connectionCount = __connections.Length;
        __connections.RemoveAt(connectionIndex.value);

        UnityEngine.Assertions.Assert.IsTrue(__sendBuffers.Length >= connectionCount);
        UnityEngine.Assertions.Assert.IsTrue(__outboxes.Length >= connectionCount);

        var sendBuffer = __sendBuffers[connectionIndex.value];
        sendBuffer.Clear();
        var outbox = __outboxes[connectionIndex.value];
        outbox.Clear();

        ShiftSlotsForDisconnect(
            ref __sendBuffers,
            connectionIndex.value,
            connectionCount,
            in sendBuffer);
        ShiftSlotsForDisconnect(
            ref __outboxes,
            connectionIndex.value,
            connectionCount,
            in outbox);

        foreach (var pair in __connectionIDs)
        {
            uint tempID = pair.Value;
            var tempConnectionIndex = __connectionIndices[tempID];
            if (tempConnectionIndex.value > connectionIndex.value)
            {
                --tempConnectionIndex.value;
                UnityEngine.Assertions.Assert.AreEqual(
                    tempID,
                    __connectionIDs[__connections[tempConnectionIndex.value]]);
                __connectionIndices[tempID] = tempConnectionIndex;
            }
        }

        connectionIndex.value = -1;
        __connectionIndices[id] = connectionIndex;
        __Log($"Disconnect {id}");
        return id;
    }

    internal static void ShiftSlotsForDisconnect<T>(
        ref NativeList<T> slots,
        int removedIndex,
        int activeCount,
        in T recycled) where T : unmanaged
    {
        UnityEngine.Assertions.Assert.IsTrue(activeCount > 0);
        UnityEngine.Assertions.Assert.IsTrue((uint)removedIndex < (uint)activeCount);
        UnityEngine.Assertions.Assert.IsTrue(slots.Length >= activeCount);

        int lastActiveIndex = activeCount - 1;
        for (int i = removedIndex; i < lastActiveIndex; ++i)
            slots[i] = slots[i + 1];
        slots[lastActiveIndex] = recycled;
    }

    public static int GetTargetFromChannel(int channel) => -channel - 1;

    private static void __GetChannelMemberRange(
        in NativeArray<ChannelMember> members,
        int channel,
        out int start,
        out int end)
    {
        int low = 0;
        int high = members.Length;
        while (low < high)
        {
            int middle = low + ((high - low) >> 1);
            if (members[middle].channel < channel)
                low = middle + 1;
            else
                high = middle;
        }

        start = low;
        high = members.Length;
        while (low < high)
        {
            int middle = low + ((high - low) >> 1);
            if (members[middle].channel <= channel)
                low = middle + 1;
            else
                high = middle;
        }

        end = low;
    }

    private static bool __ContainsDestination(
        in NativeArray<ChannelMember> members,
        int start,
        int end,
        int destinationIndex)
    {
        while (start < end)
        {
            int middle = start + ((end - start) >> 1);
            int value = members[middle].destinationIndex;
            if (value < destinationIndex)
                start = middle + 1;
            else if (value > destinationIndex)
                end = middle;
            else
                return true;
        }

        return false;
    }

    private static void __EnsureLength(ref NativeList<int> values, int length)
    {
        if (values.Length < length)
            values.ResizeUninitialized(length);
    }

    private static void __Log(string message)
    {
        UnityEngine.Debug.Log(message);
    }
}
