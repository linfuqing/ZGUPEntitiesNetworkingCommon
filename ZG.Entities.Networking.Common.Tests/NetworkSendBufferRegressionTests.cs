using NUnit.Framework;
using Unity.Collections;
using ZG;

public class NetworkSendBufferRegressionTests
{
    [Test]
    public void Append_FromZero_PreservesEveryMessageAndFraming()
    {
        var destination = new NetworkSendBuffer(Allocator.Temp);
        var source = new NetworkSendBuffer(Allocator.Temp);

        try
        {
            Write(ref destination, 0xD0);
            Write(ref source, 0xA0);
            Write(ref source, 0xA1, 0xB1);
            Write(ref source, 0xA2, 0xB2, 0xC2);

            destination.Append(source, 0);

            AssertFrames(
                ref destination,
                new byte[] { 0xD0 },
                new byte[] { 0xA0 },
                new byte[] { 0xA1, 0xB1 },
                new byte[] { 0xA2, 0xB2, 0xC2 });
        }
        finally
        {
            source.Dispose();
            destination.Dispose();
        }
    }

    [Test]
    public void Append_FromRetryIndex_IncludesFirstUnsentMessage()
    {
        var destination = new NetworkSendBuffer(Allocator.Temp);
        var source = new NetworkSendBuffer(Allocator.Temp);

        try
        {
            Write(ref destination, 0xD0);
            Write(ref source, 0xA0);
            Write(ref source, 0xA1, 0xB1);
            Write(ref source, 0xA2, 0xB2, 0xC2);

            destination.Append(source, 1);

            AssertFrames(
                ref destination,
                new byte[] { 0xD0 },
                new byte[] { 0xA1, 0xB1 },
                new byte[] { 0xA2, 0xB2, 0xC2 });
        }
        finally
        {
            source.Dispose();
            destination.Dispose();
        }
    }

    [Test]
    public void Append_LastIndex_IncludesLastMessage()
    {
        var destination = new NetworkSendBuffer(Allocator.Temp);
        var source = new NetworkSendBuffer(Allocator.Temp);

        try
        {
            Write(ref source, 0xA0);
            Write(ref source, 0xA1, 0xB1);
            Write(ref source, 0xA2, 0xB2, 0xC2);

            destination.Append(source, 2);

            AssertFrames(
                ref destination,
                new byte[] { 0xA2, 0xB2, 0xC2 });
        }
        finally
        {
            source.Dispose();
            destination.Dispose();
        }
    }

    [Test]
    public void Append_IndexAtEnd_IsNoOp()
    {
        var destination = new NetworkSendBuffer(Allocator.Temp);
        var source = new NetworkSendBuffer(Allocator.Temp);

        try
        {
            Write(ref destination, 0xD0);
            Write(ref source, 0xA0);
            Write(ref source, 0xA1);

            destination.Append(source, 2);

            AssertFrames(
                ref destination,
                new byte[] { 0xD0 });
        }
        finally
        {
            source.Dispose();
            destination.Dispose();
        }
    }

    private static void Write(ref NetworkSendBuffer buffer, params byte[] payload)
    {
        Assert.That(payload.Length, Is.GreaterThan(0));
        Assert.That(payload.Length, Is.LessThanOrEqualTo(ushort.MaxValue));
        Assert.That(buffer.BeginWrite(out var writer, (ushort)payload.Length), Is.True);
        for (int i = 0; i < payload.Length; ++i)
            writer.WriteByte(payload[i]);

        buffer.EndWrite(writer);
    }

    private static void AssertFrames(
        ref NetworkSendBuffer buffer,
        params byte[][] expectedFrames)
    {
        int readIndex = 0;
        for (int frameIndex = 0; frameIndex < expectedFrames.Length; ++frameIndex)
        {
            Assert.That(
                buffer.ReadNext(ref readIndex, out var bytes),
                Is.True,
                $"Missing frame {frameIndex}.");

            byte[] expected = expectedFrames[frameIndex];
            Assert.That(bytes.Length, Is.EqualTo(expected.Length));
            for (int byteIndex = 0; byteIndex < expected.Length; ++byteIndex)
                Assert.That(
                    bytes[byteIndex],
                    Is.EqualTo(expected[byteIndex]),
                    $"Frame {frameIndex}, byte {byteIndex} differs.");
        }

        Assert.That(
            buffer.ReadNext(ref readIndex, out _),
            Is.False,
            "The buffer contained unexpected trailing frames.");
    }
}
