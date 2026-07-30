using System;
using System.Collections.Generic;
using NUnit.Framework;
using Unity.Collections;
using Unity.Jobs;
using Unity.Networking.Transport;
using Unity.Networking.Transport.Error;
using ZG;

public class NetworkClientSendBufferCaptureTests
{
    private struct ParallelWriteJob : IJobParallelFor
    {
        public NetworkClientSendBuffer.ParallelWriter writer;

        public void Execute(int index)
        {
            if (!writer.BeginWrite(0, out var stream, sizeof(int)))
                return;

            stream.WriteInt(index);
            writer.EndWrite(stream);
        }
    }

    private struct TestDriver : INetworkDriver, IDisposable
    {
        public NativeArray<byte> bytes;
        public int failuresRemaining;
        public int beginSendCount;
        public int endSendCount;
        public int lastLength;

        public TestDriver(int capacity, int failuresRemaining = 0)
        {
            bytes = new NativeArray<byte>(capacity, Allocator.Persistent);
            this.failuresRemaining = failuresRemaining;
            beginSendCount = 0;
            endSendCount = 0;
            lastLength = 0;
        }

        public int BeginSend(
            NetworkPipeline pipe,
            NetworkConnection connection,
            out DataStreamWriter writer,
            int requiredPayloadSize = 0)
        {
            ++beginSendCount;
            writer = new DataStreamWriter(bytes);
            return (int)StatusCode.Success;
        }

        public int EndSend(DataStreamWriter writer)
        {
            ++endSendCount;
            lastLength = writer.Length;
            if (failuresRemaining > 0)
            {
                --failuresRemaining;
                return (int)StatusCode.NetworkSendQueueFull;
            }

            return writer.Length;
        }

        public void Dispose()
        {
            if (bytes.IsCreated)
                bytes.Dispose();
        }
    }

    [Test]
    public void Capture_SameSlotMultipleWrites_AreReturnedExactlyOnce()
    {
        var buffer = CreateBuffer(1);
        try
        {
            uint generation = buffer.BeginEndWriteCapture(
                new NetworkClientSendBuffer.EndWriteCaptureStamp(7, 1.25));

            WriteByte(ref buffer, 0, 0xA0);
            WriteByte(ref buffer, 0, 0xA1);
            WriteByte(ref buffer, 0, 0xA2);

            Assert.That(buffer.capturedEndWriteCount, Is.EqualTo(3));
            AssertCapturedByte(ref buffer, 1, 7, 1.25, 0xA0);
            AssertCapturedByte(ref buffer, 2, 7, 1.25, 0xA1);
            AssertCapturedByte(ref buffer, 3, 7, 1.25, 0xA2);
            Assert.That(
                buffer.TryPeekCapturedEndWrite(out _, out _, out _),
                Is.EqualTo(NetworkClientSendBuffer.EndWriteCaptureReadStatus.Empty));
            Assert.That(buffer.EndWriteCapture(generation), Is.True);
        }
        finally
        {
            buffer.Dispose();
        }
    }

    [Test]
    public void Capture_InterleavedSlots_UsesGlobalCommittedEndWriteOrderAndStamp()
    {
        var buffer = CreateBuffer(2);
        try
        {
            uint generation = buffer.BeginEndWriteCapture(
                new NetworkClientSendBuffer.EndWriteCaptureStamp(1, 0.5));

            WriteByte(ref buffer, 0, 0xA1);
            Assert.That(
                buffer.SetEndWriteCaptureStamp(
                    generation,
                    new NetworkClientSendBuffer.EndWriteCaptureStamp(2, 0.75)),
                Is.True);
            WriteByte(ref buffer, 1, 0xB1);
            WriteByte(ref buffer, 0, 0xA2);

            AssertCapturedByte(ref buffer, 1, 1, 0.5, 0xA1);
            AssertCapturedByte(ref buffer, 2, 2, 0.75, 0xB1);
            AssertCapturedByte(ref buffer, 3, 2, 0.75, 0xA2);
            Assert.That(buffer.EndWriteCapture(generation), Is.True);
        }
        finally
        {
            buffer.Dispose();
        }
    }

    [Test]
    public void Capture_EmptyEndWrite_IsNeitherJournaledNorSent()
    {
        var buffer = CreateBuffer(1);
        var driver = new TestDriver(64);
        try
        {
            uint generation = buffer.BeginEndWriteCapture(default);
            Assert.That(buffer.BeginWrite(0, out var writer, 1), Is.True);
            buffer.EndWrite(writer);

            Assert.That(buffer.capturedEndWriteCount, Is.Zero);
            Assert.That(
                buffer.TryPeekCapturedEndWrite(out _, out _, out _),
                Is.EqualTo(NetworkClientSendBuffer.EndWriteCaptureReadStatus.Empty));

            buffer.Apply(default, ref driver);
            Assert.That(driver.endSendCount, Is.Zero);
            Assert.That(buffer.EndWriteCapture(generation), Is.True);
        }
        finally
        {
            driver.Dispose();
            buffer.Dispose();
        }
    }

    [Test]
    public void Capture_SurvivesSuccessfulApplyAndExplicitClear()
    {
        var buffer = CreateBuffer(1);
        var driver = new TestDriver(64);
        try
        {
            uint generation = buffer.BeginEndWriteCapture(default);
            WriteByte(ref buffer, 0, 0xA0);
            buffer.Apply(default, ref driver);

            WriteByte(ref buffer, 0, 0xA1);
            buffer.Clear();

            AssertCapturedByte(ref buffer, 1, 0, 0.0, 0xA0);
            AssertCapturedByte(ref buffer, 2, 0, 0.0, 0xA1);
            Assert.That(buffer.EndWriteCapture(generation), Is.True);
        }
        finally
        {
            driver.Dispose();
            buffer.Dispose();
        }
    }

    [Test]
    public void Capture_PeekIsStableAndTokenCanOnlyBeConsumedOnce()
    {
        var buffer = CreateBuffer(1);
        try
        {
            uint generation = buffer.BeginEndWriteCapture(default);
            WriteByte(ref buffer, 0, 0x5A);

            Assert.That(
                buffer.TryPeekCapturedEndWrite(out var firstToken, out var firstBytes, out _),
                Is.EqualTo(NetworkClientSendBuffer.EndWriteCaptureReadStatus.Success));
            Assert.That(
                buffer.TryPeekCapturedEndWrite(out var secondToken, out var secondBytes, out _),
                Is.EqualTo(NetworkClientSendBuffer.EndWriteCaptureReadStatus.Success));
            Assert.That(secondToken.sequence, Is.EqualTo(firstToken.sequence));
            Assert.That(secondBytes[0], Is.EqualTo(firstBytes[0]));

            Assert.That(buffer.ConsumeCapturedEndWrite(firstToken), Is.True);
            Assert.That(buffer.ConsumeCapturedEndWrite(secondToken), Is.False);
            Assert.That(buffer.EndWriteCapture(generation), Is.True);
        }
        finally
        {
            buffer.Dispose();
        }
    }

    [Test]
    public void Capture_SequenceWrap_PreservesSerialOrder()
    {
        var buffer = CreateBuffer(2);
        try
        {
            uint generation = buffer.BeginEndWriteCapture(default);
            Assert.That(buffer.SetEndWriteCaptureSequenceForTests(uint.MaxValue - 1), Is.True);

            WriteByte(ref buffer, 0, 0xFE);
            WriteByte(ref buffer, 1, 0x00);

            AssertCapturedByte(ref buffer, uint.MaxValue, 0, 0.0, 0xFE);
            AssertCapturedByte(ref buffer, 0, 0, 0.0, 0x00);
            Assert.That(buffer.EndWriteCapture(generation), Is.True);
        }
        finally
        {
            buffer.Dispose();
        }
    }

    [Test]
    public void ApplyFailure_RetainsSlotForRetryWithoutRecapturing()
    {
        var buffer = CreateBuffer(1);
        var driver = new TestDriver(64, failuresRemaining: 1);
        try
        {
            uint generation = buffer.BeginEndWriteCapture(default);
            WriteByte(ref buffer, 0, 0xA0);

            buffer.Apply(default, ref driver);
            Assert.That(driver.endSendCount, Is.EqualTo(1));
            Assert.That(buffer.capturedEndWriteCount, Is.EqualTo(1));

            buffer.Apply(default, ref driver);
            Assert.That(driver.endSendCount, Is.EqualTo(2), "Failed sends must remain active for retry.");
            Assert.That(buffer.capturedEndWriteCount, Is.EqualTo(1), "A retry is not another EndWrite.");

            AssertCapturedByte(ref buffer, 1, 0, 0.0, 0xA0);
            Assert.That(buffer.EndWriteCapture(generation), Is.True);
        }
        finally
        {
            driver.Dispose();
            buffer.Dispose();
        }
    }

    [Test]
    public void Clear_ClearsEveryPipelineSlot()
    {
        var buffer = CreateBuffer(2);
        var driver = new TestDriver(64);
        try
        {
            WriteByte(ref buffer, 1, 0xA0);
            buffer.Clear();
            WriteByte(ref buffer, 1, 0xB0);

            buffer.Apply(default, ref driver);

            Assert.That(driver.endSendCount, Is.EqualTo(1));
            Assert.That(driver.lastLength, Is.EqualTo(sizeof(ushort) + 1));
            Assert.That(driver.bytes[sizeof(ushort)], Is.EqualTo(0xB0));
        }
        finally
        {
            driver.Dispose();
            buffer.Dispose();
        }
    }

    [Test]
    public void Capture_ParallelWriters_ProduceUniqueGloballyOrderedFrames()
    {
        const int frameCount = 64;
        var buffer = CreateBuffer(1);
        try
        {
            uint generation = buffer.BeginEndWriteCapture(
                new NetworkClientSendBuffer.EndWriteCaptureStamp(4, 2.0));

            new ParallelWriteJob
            {
                writer = buffer.AsParallelWriter()
            }.Schedule(frameCount, 1).Complete();

            var values = new HashSet<int>();
            for (uint expectedSequence = 1; expectedSequence <= frameCount; ++expectedSequence)
            {
                Assert.That(
                    buffer.TryPeekCapturedEndWrite(out var token, out var bytes, out var stamp),
                    Is.EqualTo(NetworkClientSendBuffer.EndWriteCaptureReadStatus.Success));
                Assert.That(token.sequence, Is.EqualTo(expectedSequence));
                Assert.That(stamp.epoch, Is.EqualTo(4));
                Assert.That(stamp.timestamp, Is.EqualTo(2.0));

                var reader = new DataStreamReader(bytes);
                Assert.That(values.Add(reader.ReadInt()), Is.True, "A parallel EndWrite was captured twice.");
                Assert.That(buffer.ConsumeCapturedEndWrite(token), Is.True);
            }

            Assert.That(values.Count, Is.EqualTo(frameCount));
            for (int i = 0; i < frameCount; ++i)
                Assert.That(values.Contains(i), Is.True, $"Missing parallel payload {i}.");

            Assert.That(buffer.EndWriteCapture(generation), Is.True);
        }
        finally
        {
            buffer.Dispose();
        }
    }

    private static NetworkClientSendBuffer CreateBuffer(int pipelineCount)
    {
        var buffer = new NetworkClientSendBuffer(Allocator.Persistent);
        for (int i = 0; i < pipelineCount; ++i)
            buffer.CreatePipeline(default);

        return buffer;
    }

    private static void WriteByte(ref NetworkClientSendBuffer buffer, int pipelineIndex, byte value)
    {
        Assert.That(buffer.BeginWrite(pipelineIndex, out var writer, 1), Is.True);
        writer.WriteByte(value);
        buffer.EndWrite(writer);
    }

    private static void AssertCapturedByte(
        ref NetworkClientSendBuffer buffer,
        uint sequence,
        uint epoch,
        double timestamp,
        byte expected)
    {
        Assert.That(
            buffer.TryPeekCapturedEndWrite(out var token, out var bytes, out var stamp),
            Is.EqualTo(NetworkClientSendBuffer.EndWriteCaptureReadStatus.Success));
        Assert.That(token.sequence, Is.EqualTo(sequence));
        Assert.That(stamp.epoch, Is.EqualTo(epoch));
        Assert.That(stamp.timestamp, Is.EqualTo(timestamp));
        Assert.That(bytes.Length, Is.EqualTo(1));
        Assert.That(bytes[0], Is.EqualTo(expected));
        Assert.That(buffer.ConsumeCapturedEndWrite(token), Is.True);
    }
}
