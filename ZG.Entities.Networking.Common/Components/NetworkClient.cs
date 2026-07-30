using System;
using Unity.Jobs;
using Unity.Burst;
using Unity.Entities;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Jobs.LowLevel.Unsafe;
using Unity.Mathematics;
using Unity.Networking.Transport;
using Unity.Networking.Transport.Error;

namespace ZG
{

    public enum NetworkClientMessageType
    {
        Connect, 
        Data, 
        Disconnect
    }

    public struct NetworkClientSendBuffer : IComponentData
    {
        public readonly struct EndWriteCaptureStamp
        {
            public readonly uint epoch;
            public readonly double timestamp;

            public EndWriteCaptureStamp(uint epoch, double timestamp)
            {
                this.epoch = epoch;
                this.timestamp = timestamp;
            }
        }

        public readonly struct EndWriteCaptureToken
        {
            internal readonly uint generation;
            internal readonly int slotIndex;
            internal readonly int localIndex;

            public readonly uint sequence;

            internal EndWriteCaptureToken(uint generation, int slotIndex, int localIndex, uint sequence)
            {
                this.generation = generation;
                this.slotIndex = slotIndex;
                this.localIndex = localIndex;
                this.sequence = sequence;
            }
        }

        public enum EndWriteCaptureReadStatus : byte
        {
            Success,
            Empty,
            NotActive,
            Faulted
        }

        public enum EndWriteCaptureFault : byte
        {
            None,
            JournalNotCreated,
            JournalAppendFailed,
            JournalInvariantFailed,
            SequenceWindowExceeded
        }

        internal struct EndWriteCaptureMetadata
        {
            public uint sequence;
            public EndWriteCaptureStamp stamp;
        }

        private struct EndWriteCaptureControl
        {
            public int isActive;
            public int generation;
            public int sequence;
            public int outstandingCount;
            public int fault;
            public EndWriteCaptureStamp stamp;
        }

        private struct DriverWrapper : INetworkDriver
        {
            private NetworkDriver.Concurrent __instance;

            public DriverWrapper(ref NetworkDriver.Concurrent instance)
            {
                __instance = instance;
            }

            public int BeginSend(
                NetworkPipeline pipe,
                NetworkConnection connection,
                out DataStreamWriter writer,
                int requiredPayloadSize = 0)
                => __instance.BeginSend(pipe, connection, out writer, requiredPayloadSize);

            public int EndSend(DataStreamWriter writer) => __instance.EndSend(writer);
        }

        public struct Buffer
        {
            public int index;
            public NetworkSendBuffer value;

            internal int captureReadIndex;
            internal NetworkSendBuffer capturePayloads;
            internal UnsafeList<EndWriteCaptureMetadata> captureMetadata;

            public void Clear()
            {
                index = 0;
                value.Clear();
            }

            internal void InitializeCapture(in AllocatorManager.AllocatorHandle allocator)
            {
                captureReadIndex = 0;
                if (capturePayloads.isCreated)
                    capturePayloads.Clear();
                else
                    capturePayloads = new NetworkSendBuffer(allocator);

                if (captureMetadata.IsCreated)
                    captureMetadata.Clear();
                else
                    captureMetadata = new UnsafeList<EndWriteCaptureMetadata>(1, allocator);
            }

            internal void ClearCapture()
            {
                captureReadIndex = 0;
                if (capturePayloads.isCreated)
                    capturePayloads.Clear();

                if (captureMetadata.IsCreated)
                    captureMetadata.Clear();
            }

            internal void Dispose()
            {
                value.Dispose();
                if (capturePayloads.isCreated)
                    capturePayloads.Dispose();

                if (captureMetadata.IsCreated)
                    captureMetadata.Dispose();
            }
        }

        public struct BufferIndex : IEquatable<BufferIndex>, IComparable<BufferIndex>
        {
            public int value;

            public int index;

            public bool Equals(BufferIndex other)
            {
                return value == other.value;
            }

            public int CompareTo(BufferIndex other)
            {
                return index.CompareTo(other.index);
            }

            public override int GetHashCode()
            {
                return value;
            }
        }

        public struct ParallelWriter
        {
            private NativeParallelHashSet<BufferIndex>.ParallelWriter __bufferIndices;

            [NativeDisableParallelForRestriction]
            private NativeArray<Buffer> __buffers;

            [NativeDisableParallelForRestriction]
            private NativeArray<int> __index;

            [NativeDisableParallelForRestriction]
            private NativeArray<EndWriteCaptureControl> __captureControl;

            [NativeSetThreadIndex]
            internal int _threadIndex;

            internal ParallelWriter(ref NetworkClientSendBuffer buffer)
            {
                __bufferIndices = buffer.__bufferIndices.AsParallelWriter();
                __buffers = buffer.__buffers.AsDeferredJobArray();
                __index = buffer.__index;
                __captureControl = buffer.__captureControl;
                _threadIndex = 0;
            }

            public bool BeginWrite(int pipelineIndex, out DataStreamWriter writer, ushort capacity = 1024)
            {
                int bufferIndex = pipelineIndex * JobsUtility.MaxJobThreadCount + _threadIndex;
                var buffer = __buffers[bufferIndex];
                
                bool result = buffer.value.BeginWrite(out writer, capacity);
                if (result)
                {
                    writer.m_SendHandleData = (IntPtr)bufferIndex;

                    __buffers[bufferIndex] = buffer;
                }

                return result;
            }

            public void EndWrite(in DataStreamWriter writer)
            {
                BufferIndex bufferIndex;
                bufferIndex.value = (int)writer.m_SendHandleData;
                var buffer = __buffers[bufferIndex.value];
                int messageIndex = buffer.value.messageCount;
                buffer.value.EndWrite(writer);

                if (buffer.value.messageCount != messageIndex + 1)
                {
                    __buffers[bufferIndex.value] = buffer;
                    return;
                }

                __CaptureCommittedEndWrite(
                    ref buffer,
                    messageIndex,
                    ref __captureControl);
                __buffers[bufferIndex.value] = buffer;

                bufferIndex.index = System.Threading.Interlocked.Increment(ref __index.AsSpan()[0]);
                __bufferIndices.Add(bufferIndex);
            }
        }

        private NativeArray<int> __index;
        [ReadOnly]
        private NativeList<NetworkPipeline> __pipelines;
        private NativeList<Buffer> __buffers;
        private NativeParallelHashSet<BufferIndex> __bufferIndices;
        private NativeArray<EndWriteCaptureControl> __captureControl;
        
        public bool isCreated => __buffers.IsCreated;
        
        public unsafe AllocatorManager.AllocatorHandle allocator => __pipelines.GetUnsafeList()->Allocator;

        public NetworkClientSendBuffer(in AllocatorManager.AllocatorHandle allocator)
        {
            __index = CollectionHelper.CreateNativeArray<int>(1, allocator);
            
            __pipelines = new NativeList<NetworkPipeline>(allocator);

            __buffers = new NativeList<Buffer>(allocator);

            __bufferIndices = new NativeParallelHashSet<BufferIndex>(1, allocator);

            __captureControl = CollectionHelper.CreateNativeArray<EndWriteCaptureControl>(1, allocator);
        }

        public void Dispose()
        {
            __index.Dispose();
            
            __pipelines.Dispose();

            foreach (var buffer in __buffers)
                buffer.Dispose();
            
            __buffers.Dispose();
            __bufferIndices.Dispose();
            __captureControl.Dispose();
        }

        public void Clear()
        {
            Buffer buffer;
            int length = __buffers.Length;
            for (int i = 0; i < length; ++i)
            {
                buffer = __buffers[i];
                buffer.Clear();

                __buffers[i] = buffer;
            }
            __bufferIndices.Clear();
            __index[0] = 0;
        }

        public ParallelWriter AsParallelWriter()
        {
            return new ParallelWriter(ref this);
        }

        public NetworkPipeline GetPipeline(int pipelineIndex)
        {
            return  __pipelines[pipelineIndex];
        }

        public int CreatePipeline(in NetworkPipeline pipeline)
        {
            int result = __pipelines.Length;
            
            __pipelines.Add(pipeline);

            for (int i = 0; i < JobsUtility.MaxJobThreadCount; ++i)
            {
                var buffer = default(Buffer);
                buffer.value = new NetworkSendBuffer(allocator);
                if (__captureControl[0].isActive != 0)
                    buffer.InitializeCapture(allocator);

                __buffers.Add(buffer);
            }
            
            __bufferIndices.Capacity = math.max(__bufferIndices.Capacity, __pipelines.Length * JobsUtility.MaxJobThreadCount);
            return result;
        }
        
        public int GetPendingSendSlotCount() => __buffers.IsCreated ? __buffers.Length : 0;

        public int GetPendingSendReadIndex(int slotIndex) => __buffers[slotIndex].index;

        public bool TryReadPendingSend(int slotIndex, ref int readIndex, out NativeArray<byte> bytes)
            => __buffers[slotIndex].value.ReadNext(ref readIndex, out bytes);

        /// <summary>
        /// Starts the optional completed-EndWrite journal. Call only after producer jobs are complete.
        /// The journal is independent from the transport buffers and therefore survives <see cref="Apply"/> and
        /// <see cref="Clear"/>. Only one consumer may drain it.
        /// </summary>
        public uint BeginEndWriteCapture(in EndWriteCaptureStamp initialStamp)
        {
            if (!isCreated || !__captureControl.IsCreated)
                throw new InvalidOperationException("NetworkClientSendBuffer is not created.");

            ref var control = ref __captureControl.AsSpan()[0];
            if (control.isActive != 0)
                throw new InvalidOperationException("EndWrite capture is already active.");

            var captureAllocator = allocator;
            for (int i = 0; i < __buffers.Length; ++i)
            {
                var buffer = __buffers[i];
                buffer.InitializeCapture(captureAllocator);
                __buffers[i] = buffer;
            }

            int generation = unchecked(control.generation + 1);
            if (generation == 0)
                generation = 1;

            control = new EndWriteCaptureControl
            {
                isActive = 1,
                generation = generation,
                stamp = initialStamp
            };

            return unchecked((uint)generation);
        }

        /// <summary>
        /// Updates the opaque producer epoch and high-precision timestamp copied by subsequent EndWrites.
        /// Do not call concurrently with an EndWrite producer job.
        /// </summary>
        public bool SetEndWriteCaptureStamp(uint generation, in EndWriteCaptureStamp stamp)
        {
            if (!__captureControl.IsCreated)
                return false;

            ref var control = ref __captureControl.AsSpan()[0];
            if (control.isActive == 0 || unchecked((uint)control.generation) != generation)
                return false;

            control.stamp = stamp;
            return true;
        }

        public int capturedEndWriteCount
        {
            get
            {
                if (!__captureControl.IsCreated)
                    return 0;

                int count = __captureControl[0].outstandingCount;
                return count > 0 ? count : 0;
            }
        }

        public EndWriteCaptureFault endWriteCaptureFault
            => !__captureControl.IsCreated
                ? EndWriteCaptureFault.JournalNotCreated
                : (EndWriteCaptureFault)__captureControl[0].fault;

        /// <summary>
        /// Peeks the globally earliest unread committed EndWrite. The payload view remains valid until the matching
        /// <see cref="ConsumeCapturedEndWrite"/> or another producer write touches that slot; copy it before consuming.
        /// Call only after producer jobs are complete.
        /// </summary>
        public EndWriteCaptureReadStatus TryPeekCapturedEndWrite(
            out EndWriteCaptureToken token,
            out NativeArray<byte> bytes,
            out EndWriteCaptureStamp stamp)
        {
            token = default;
            bytes = default;
            stamp = default;
            if (!__captureControl.IsCreated)
                return EndWriteCaptureReadStatus.NotActive;

            ref var control = ref __captureControl.AsSpan()[0];
            if (control.isActive == 0)
                return EndWriteCaptureReadStatus.NotActive;

            if (control.fault != 0)
                return EndWriteCaptureReadStatus.Faulted;

            if (control.outstandingCount < 1)
                return EndWriteCaptureReadStatus.Empty;

            int bestSlotIndex = -1;
            EndWriteCaptureMetadata bestMetadata = default;
            for (int slotIndex = 0; slotIndex < __buffers.Length; ++slotIndex)
            {
                var buffer = __buffers[slotIndex];
                if (!buffer.captureMetadata.IsCreated ||
                    buffer.captureReadIndex < 0 ||
                    buffer.captureReadIndex >= buffer.captureMetadata.Length)
                {
                    continue;
                }

                var metadata = buffer.captureMetadata[buffer.captureReadIndex];
                if (bestSlotIndex < 0)
                {
                    bestSlotIndex = slotIndex;
                    bestMetadata = metadata;
                    continue;
                }

                if (metadata.sequence == bestMetadata.sequence)
                {
                    __SetCaptureFault(ref control, EndWriteCaptureFault.JournalInvariantFailed);
                    return EndWriteCaptureReadStatus.Faulted;
                }

                if (__IsSequenceBefore(metadata.sequence, bestMetadata.sequence))
                {
                    bestSlotIndex = slotIndex;
                    bestMetadata = metadata;
                }
            }

            if (bestSlotIndex < 0)
            {
                __SetCaptureFault(ref control, EndWriteCaptureFault.JournalInvariantFailed);
                return EndWriteCaptureReadStatus.Faulted;
            }

            var bestBuffer = __buffers[bestSlotIndex];
            int payloadIndex = bestBuffer.captureReadIndex;
            int probeIndex = payloadIndex;
            if (!bestBuffer.capturePayloads.isCreated ||
                bestBuffer.capturePayloads.messageCount != bestBuffer.captureMetadata.Length ||
                !bestBuffer.capturePayloads.ReadNext(ref probeIndex, out bytes) ||
                probeIndex != payloadIndex + 1)
            {
                bytes = default;
                __SetCaptureFault(ref control, EndWriteCaptureFault.JournalInvariantFailed);
                return EndWriteCaptureReadStatus.Faulted;
            }

            token = new EndWriteCaptureToken(
                unchecked((uint)control.generation),
                bestSlotIndex,
                payloadIndex,
                bestMetadata.sequence);
            stamp = bestMetadata.stamp;
            return EndWriteCaptureReadStatus.Success;
        }

        /// <summary>Consumes exactly the frame returned by the latest matching peek.</summary>
        public bool ConsumeCapturedEndWrite(in EndWriteCaptureToken token)
        {
            if (!__captureControl.IsCreated)
                return false;

            ref var control = ref __captureControl.AsSpan()[0];
            if (control.isActive == 0 ||
                unchecked((uint)control.generation) != token.generation ||
                token.slotIndex < 0 ||
                token.slotIndex >= __buffers.Length)
            {
                return false;
            }

            ref var buffer = ref __buffers.ElementAt(token.slotIndex);
            if (!buffer.captureMetadata.IsCreated ||
                buffer.captureReadIndex != token.localIndex ||
                token.localIndex < 0 ||
                token.localIndex >= buffer.captureMetadata.Length ||
                buffer.captureMetadata[token.localIndex].sequence != token.sequence)
            {
                return false;
            }

            ++buffer.captureReadIndex;
            int outstandingCount = --control.outstandingCount;
            if (outstandingCount < 0)
            {
                __SetCaptureFault(ref control, EndWriteCaptureFault.JournalInvariantFailed);
                return false;
            }

            if (buffer.captureReadIndex == buffer.captureMetadata.Length)
                buffer.ClearCapture();

            return true;
        }

        /// <summary>
        /// Ends the capture session. Without <paramref name="discardUnread"/>, every frame must have been consumed and
        /// no capture fault may be present; otherwise this returns false and leaves the session active for inspection.
        /// </summary>
        public bool EndWriteCapture(uint generation, bool discardUnread = false)
        {
            if (!__captureControl.IsCreated)
                return false;

            ref var control = ref __captureControl.AsSpan()[0];
            if (control.isActive == 0 || unchecked((uint)control.generation) != generation)
                return false;

            if (!discardUnread && (control.outstandingCount != 0 || control.fault != 0))
                return false;

            control.isActive = 0;
            for (int i = 0; i < __buffers.Length; ++i)
            {
                var buffer = __buffers[i];
                buffer.ClearCapture();
                __buffers[i] = buffer;
            }

            control.outstandingCount = 0;
            return true;
        }

        internal bool SetEndWriteCaptureSequenceForTests(uint sequence)
        {
            if (!__captureControl.IsCreated || __captureControl[0].isActive == 0)
                return false;

            __captureControl.AsSpan()[0].sequence = unchecked((int)sequence);
            return true;
        }

        public bool BeginWrite(int pipelineIndex, out DataStreamWriter writer, ushort capacity = 1024)
        {
            int bufferIndex = pipelineIndex * JobsUtility.MaxJobThreadCount;
            
            bool result = __buffers.ElementAt(bufferIndex).value.BeginWrite(out writer, capacity);

            if(result)
                writer.m_SendHandleData = (IntPtr)bufferIndex;

            return result;
        }

        public void EndWrite(in DataStreamWriter writer)
        {
            BufferIndex bufferIndex;
            bufferIndex.value = (int)writer.m_SendHandleData;
            ref var buffer = ref __buffers.ElementAt(bufferIndex.value);
            int messageIndex = buffer.value.messageCount;
            buffer.value.EndWrite(writer);
            if (buffer.value.messageCount != messageIndex + 1)
                return;

            __CaptureCommittedEndWrite(
                ref buffer,
                messageIndex,
                ref __captureControl);

            bufferIndex.index = System.Threading.Interlocked.Increment(ref __index.AsSpan()[0]);
            __bufferIndices.Add(bufferIndex);
        }

        public void Apply(in NetworkConnection connection, ref NetworkDriver.Concurrent driver)
        {
            var wrapper = new DriverWrapper(ref driver);
            Apply(connection, ref wrapper);
        }

        internal void Apply<T>(in NetworkConnection connection, ref T driver)
            where T : struct, INetworkDriver
        {
            using var bufferIndices = __bufferIndices.ToNativeArray(Allocator.Temp);
            bufferIndices.Sort();

            __bufferIndices.Clear();
            bool hasFailure = false;
            foreach (var bufferIndex in bufferIndices)
            {
                ref var buffer = ref __buffers.ElementAt(bufferIndex.value);
                if (buffer.value.Apply(
                        connection,
                        __pipelines[bufferIndex.value / JobsUtility.MaxJobThreadCount],
                        ref driver,
                        ref buffer.index))
                {
                    continue;
                }

                hasFailure = true;
                __bufferIndices.Add(bufferIndex);
            }

            if (!hasFailure)
                __index[0] = 0;
        }

        private static void __CaptureCommittedEndWrite(
            ref Buffer buffer,
            int messageIndex,
            ref NativeArray<EndWriteCaptureControl> captureControl)
        {
            if (!captureControl.IsCreated)
                return;

            ref var control = ref captureControl.AsSpan()[0];
            if (control.isActive == 0 || control.fault != 0)
                return;

            if (!buffer.capturePayloads.isCreated || !buffer.captureMetadata.IsCreated)
            {
                __SetCaptureFault(ref control, EndWriteCaptureFault.JournalNotCreated);
                return;
            }

            int captureMessageCount = buffer.capturePayloads.messageCount;
            buffer.capturePayloads.Append(buffer.value, messageIndex);
            if (buffer.capturePayloads.messageCount != captureMessageCount + 1)
            {
                __SetCaptureFault(ref control, EndWriteCaptureFault.JournalAppendFailed);
                return;
            }

            uint sequence = unchecked((uint)System.Threading.Interlocked.Increment(ref control.sequence));
            buffer.captureMetadata.Add(new EndWriteCaptureMetadata
            {
                sequence = sequence,
                stamp = control.stamp
            });

            int outstandingCount = System.Threading.Interlocked.Increment(ref control.outstandingCount);
            if (outstandingCount < 1)
                __SetCaptureFault(ref control, EndWriteCaptureFault.SequenceWindowExceeded);
        }

        private static bool __IsSequenceBefore(uint lhs, uint rhs)
            => unchecked((int)(lhs - rhs)) < 0;

        private static void __SetCaptureFault(
            ref EndWriteCaptureControl control,
            EndWriteCaptureFault fault)
        {
            System.Threading.Interlocked.CompareExchange(ref control.fault, (int)fault, 0);
        }
    }
    
    public struct NetworkClient
    {
        private struct Header
        {
            public NetworkConnection connection;
            public NetworkEndpoint endpoint;
            public double disconnectionTime;
        }

        public struct Message : IComparable<Message>
        {
            public NetworkClientMessageType type;
            public int offset;
            public int size;

            public NativeArray<byte> AsArray(in NativeArray<byte> buffer)
            {
                return buffer.GetSubArray(offset, size);
            }

            public DataStreamReader Read(in NativeArray<byte> buffer)
            {
                return new DataStreamReader(AsArray(buffer));
            }

            public int CompareTo(Message other)
            {
                int result = offset.CompareTo(other.offset);
                if(0 == result)
                    return ((int)type).CompareTo((int)other.type);

                return result;
            }
        }

        public struct MessageElement : IComparable<MessageElement>
        {
            public readonly Message Message;
                
            private NativeArray<byte> __buffer;
            
            public DataStreamReader reader => Message.Read(__buffer);

            public MessageElement(in Message message, in NativeArray<byte> buffer)
            {
                Message = message;
                __buffer = buffer;
            }

            public MessageElement(in Message message, in Messages messages)
            {
                Message = message;
                __buffer = messages._buffer.AsArray();
            }

            public NativeArray<byte> AsArray()
            {
                return Message.AsArray(__buffer);
            }
            
            public int CompareTo(MessageElement other)
            {
                return Message.offset.CompareTo(other.Message.offset);
            }
        }

        public struct MessageEnumerator
        {
            private NativeList<byte> __buffer;
            private NativeParallelMultiHashMap<NetworkPipeline, Message>.KeyValueEnumerator __enumerator;

            public MessageElement Current => new MessageElement(__enumerator.Current.Value, __buffer.AsArray());

            public MessageEnumerator(in Messages messages)
            {
                __buffer = messages._buffer;
                __enumerator = messages._values.GetEnumerator();
            }

            public bool MoveNext() => __enumerator.MoveNext();
        }

        public struct Messages
        {
            internal NativeList<byte> _buffer;
            internal NativeParallelMultiHashMap<NetworkPipeline, Message> _values;
            
            public Messages(in NetworkClient client)
            {
                _buffer = client.__buffer;
                _values = client.__messages;
            }

            public MessageEnumerator GetEnumerator()
            {
                return new MessageEnumerator(in this);
            }
        }

        [BurstCompile]
        private struct Send : IJob
        {
            [ReadOnly]
            public NativeArray<byte> headers;
            public NetworkDriver.Concurrent driver;
            public NetworkClientSendBuffer sendBuffer;

            public void Execute()
            {
                var connection = headers.GetSubArray(0, UnsafeUtility.SizeOf<NetworkConnection>()).Reinterpret<NetworkConnection>(1)[0];
                if (NetworkConnection.State.Connected != driver.GetConnectionState(connection))
                    return;
                
                sendBuffer.Apply(connection, ref driver);
            }
        }

#if !DEBUG
        [BurstCompile]
#endif
        private struct PopEvents : IJob
        {
            public float reconnectionTime;
            public double time;
            public NetworkDriver driver;
            public NetworkClientSendBuffer sendBuffer;
            public NativeList<byte> buffer;
            public NativeArray<byte> headers;
            public NativeParallelMultiHashMap<NetworkPipeline, Message> messages;

            public void Execute()
            {
                int headerSize = UnsafeUtility.SizeOf<Header>();
                var headers = this.headers.Length < headerSize
                    ? default
                    : this.headers.GetSubArray(0, headerSize).Reinterpret<Header>(1);
                var header = headers.IsCreated ? headers[0] : default;
                if (header.disconnectionTime > math.DBL_MIN_NORMAL)
                {
                    if (time - header.disconnectionTime > reconnectionTime)
                    {
                        switch (driver.GetConnectionState(header.connection))
                        {
                            case NetworkConnection.State.Disconnecting:
                                return;
                            case NetworkConnection.State.Disconnected:
                                header.connection = driver.Connect(header.endpoint,
                                    this.headers.GetSubArray(headerSize, this.headers.Length - headerSize));

                                break;
                        }

                        header.disconnectionTime = 0.0;

                        headers[0] = header;
                    }

                    return;
                }

                buffer.Clear();

                messages.Clear();

                bool isEmpty = false;
                NetworkEvent.Type cmd;
                Message message;
                DataStreamReader stream;
                NetworkPipeline pipeline;
                do
                {
                    cmd = driver.PopEventForConnection(header.connection, out stream, out pipeline);
                    switch (cmd)
                    {
                        case NetworkEvent.Type.Empty:
                            isEmpty = true;
                            break;
                        case NetworkEvent.Type.Data:
                            if (stream.IsCreated)
                            {
                                message.type = NetworkClientMessageType.Data;

                                while (stream.GetBytesRead() + 2 < stream.Length)
                                {
                                    message.offset = buffer.Length;
                                    message.size = stream.ReadUShort();
                                    if (stream.GetBytesRead() + message.size > stream.Length)
                                    {
                                        UnityEngine.Debug.LogError("Bad Message!");

                                        break;
                                    }

                                    buffer.ResizeUninitialized(message.offset + message.size);
                                    stream.ReadBytes(buffer.AsArray().GetSubArray(message.offset, message.size));

                                    messages.Add(pipeline, message);
                                }
                            }

                            break;
                        case NetworkEvent.Type.Connect:
                            /*int headersLength = headers.Length - headerSize;
                            if (headersLength > 0 && driver.BeginSend(header.connection, out var writer) >= 0)
                            {
                                writer.WriteUShort((ushort)headersLength);
                                writer.WriteBytes(headers.GetSubArray(headerSize, headersLength));

                                driver.EndSend(writer);
                            }*/

                            message.type = NetworkClientMessageType.Connect;
                            message.offset = buffer.Length;
                            message.size = 0;

                            messages.Add(pipeline, message);
                            break;
                        case NetworkEvent.Type.Disconnect:
                            var disconnectReason = stream.IsCreated ? (DisconnectReason)stream.ReadByte() : DisconnectReason.Default;
                            __LogDisconnectReason(disconnectReason);

                            header.disconnectionTime = time;
                            headers[0] = header;

                            /*driver.Disconnect(header.connection);
                            
                            header.connection = driver.Connect(header.endpoint, headers.GetSubArray(headerSize, headers.Length - headerSize));

                            var connections = headers.GetSubArray(0, UnsafeUtility.SizeOf<NetworkConnection>())
                                .Reinterpret<NetworkConnection>(1);
                            connections[0] = header.connection;*/
                    
                            message.type = NetworkClientMessageType.Disconnect;
                            message.offset = buffer.Length;
                            message.size = 0;

                            messages.Add(pipeline, message);
                            break;
                    }
                } while (!isEmpty);

                if (NetworkConnection.State.Connected == driver.GetConnectionState(header.connection))
                {
                    var driver = this.driver.ToConcurrent();
                    
                    sendBuffer.Apply(header.connection, ref driver);
                }
            }

            private void __LogDisconnectReason(DisconnectReason disconnectReason)
            {
                UnityEngine.Debug.LogError($"DisconnectReason: {(int)disconnectReason}");
            }
        }

        public readonly float ReconnectionTime;

        private NetworkDriver __driver;
        private NativeList<byte> __headers;
        private NativeList<byte> __buffer;
        private NativeParallelMultiHashMap<NetworkPipeline, Message> __messages;

        public bool isCreated => __driver.IsCreated;

        public NetworkConnection.State connectionState => __driver.GetConnectionState(connection);

        public NetworkConnection connection
        {
            get
            {
                int size = UnsafeUtility.SizeOf<NetworkConnection>();
                return size > __headers.Length ? default : __headers.AsArray()
                    .GetSubArray(0, size).Reinterpret<NetworkConnection>(1)[0];
            }
        }
        
        public NativeList<byte> buffer => __buffer;

        public NetworkClient(NetworkSettings settings, in AllocatorManager.AllocatorHandle allocator)
        {
            ReconnectionTime = settings.GetNetworkConfigParameters().reconnectionTimeoutMS * 0.001f;
            
#if UNITY_WEBGL && !UNITY_EDITOR
            __driver = NetworkDriver.Create(new WebSocketNetworkInterface(), settings);
#else
            __driver = NetworkDriver.Create(settings);
#endif
            //__driver = NetworkDriver.Create(settings);
            
            __headers = new NativeList<byte>(allocator);
            __buffer = new NativeList<byte>(allocator);
            __messages = new NativeParallelMultiHashMap<NetworkPipeline, Message>(1, allocator);
        }

        public void Dispose()
        {
            __driver.Dispose();
            __headers.Dispose();
            __buffer.Dispose();
            __messages.Dispose();
        }

        public Messages AsMessages()
        {
            return new Messages(this);
        }

        public void Shutdown()
        {
            __driver.Disconnect(connection);

            //__identities.Clear();
        }

        public void Connect(in NetworkEndpoint endPoint, in NativeArray<byte> payload)
        {
            if (NetworkConnection.State.Disconnected != connectionState)
                __driver.Disconnect(connection);

            int headerSize = UnsafeUtility.SizeOf<Header>(), headersSize = payload.IsCreated ? payload.Length : 0;
            __headers.ResizeUninitialized(headerSize + headersSize);
            var headersArray = __headers.AsArray();
            var temp = headersArray.GetSubArray(0, headerSize).Reinterpret<Header>(1);
            
            Header header;
            header.connection = __driver.Connect(endPoint, payload);
            header.endpoint = endPoint;
            header.disconnectionTime = 0.0;
            temp[0] = header;
            
            if(headersSize > 0)
                NativeArray<byte>.Copy(payload, 0, headersArray, headerSize, headersSize);
        }
        
        public bool Connect(in FixedString128Bytes address, ushort port, in NativeArray<byte> headers)
        {
            if (NetworkEndpoint.TryParse(address, port, out var endpoint))
            {
                Connect(endpoint, headers);

                return true;
            }

            return false;
        }
        
        public NetworkPipeline CreatePipeline(in NativeArray<NetworkPipelineStageId> stages)
        {
            return __driver.CreatePipeline(stages);
        }

        public NetworkPipeline CreatePipeline(params Type[] stages)
        {
            return __driver.CreatePipeline(stages);
        }
        
        public void GetPipelines(ref NativeList<NetworkPipeline> pipelines)
        {
            NetworkPipeline pipeline;
            foreach (var message in __messages)
            {
                pipeline = message.Key;
                if (pipelines.IndexOf(pipeline) != -1)
                    continue;

                pipelines.Add(pipeline);
            }
        }

        public void GetMessages(in NetworkPipeline pipeline, ref NativeList<Message> messages)
        {
            foreach (var message in __messages.GetValuesForKey(pipeline))
                messages.Add(message);

            messages.Sort();
        }

        public JobHandle Schedule(
            double time, 
            ref NetworkClientSendBuffer sendBuffer, 
            in JobHandle inputDeps)
        {
            var jobHandle = inputDeps;

            var headers = __headers.AsArray();
            
            bool bound = __driver.Bound;
            if (bound)
            {
                Send send;
                send.headers = headers;
                send.driver = __driver.ToConcurrent();
                send.sendBuffer = sendBuffer;

                jobHandle = send.ScheduleByRef(jobHandle);
            }
            
            jobHandle = __driver.ScheduleUpdate(jobHandle);

            PopEvents popEvents;
            popEvents.reconnectionTime = ReconnectionTime;
            popEvents.time = time;
            popEvents.driver = __driver;
            popEvents.sendBuffer = sendBuffer;
            popEvents.buffer = __buffer;
            popEvents.headers = headers;
            popEvents.messages = __messages;

            jobHandle = popEvents.ScheduleByRef(jobHandle);
            
            if(!bound)
                jobHandle = __driver.ScheduleFlushSend(jobHandle);
            
            return jobHandle;
        }

        public void TryEnqueueSyntheticData(in NetworkPipeline pipeline, ref DataStreamReader reader)
        {
            Message message;
            message.type = NetworkClientMessageType.Data;
                            
            do
            {
                message.offset = __buffer.Length;
                message.size = reader.ReadUShort();
                __buffer.ResizeUninitialized(message.offset + message.size);
                reader.ReadBytes(buffer.AsArray().GetSubArray(message.offset, message.size));

                __messages.Add(pipeline, message);
            } while (reader.GetBytesRead() < reader.Length);

        }
    }

    public struct NetworkClientDriver : IComponentData
    {
        private NetworkClient __instance;
        private NetworkClientSendBuffer __sendBuffer;
        
        public NetworkClient instance => __instance;
        
        public NetworkClientSendBuffer sendBuffer => __sendBuffer;

        public bool isCreated => __instance.isCreated;

        public NetworkClientDriver(in NetworkSettings settings, in AllocatorManager.AllocatorHandle allocator)
        {
            __instance = new NetworkClient(settings, allocator);
            __sendBuffer = new NetworkClientSendBuffer(allocator);
        }

        public NetworkClientDriver(
            in NetworkSettings settings,
            AllocatorManager.AllocatorHandle allocator)
        {
            __instance = new NetworkClient(settings, allocator);
            __sendBuffer = new NetworkClientSendBuffer(allocator);
        }

        public void Dispose()
        {
            __instance.Dispose();
            __sendBuffer.Dispose();
        }
        
        public int CreatePipeline(in NativeArray<NetworkPipelineStageId> stages)
        {
            var pipeline = __instance.CreatePipeline(stages);
            
            return __sendBuffer.CreatePipeline(pipeline);
        }
        
        public int CreatePipeline(in NativeArray<NetworkPipelineStage> stages)
        {
            using var stageIDs = stages.ToPipelineStageIDs(Allocator.Temp);
            var pipeline = __instance.CreatePipeline(stageIDs);
            
            return __sendBuffer.CreatePipeline(pipeline);
        }

        public int CreatePipeline(int pipelineIndex)
        {
            var pipeline = __sendBuffer.GetPipeline(pipelineIndex);
            
            return __sendBuffer.CreatePipeline(pipeline);
        }

        public JobHandle Schedule(double time, in JobHandle inputDeps)
        {
            return __instance.Schedule(time, ref __sendBuffer, inputDeps);
        }
    }
}
