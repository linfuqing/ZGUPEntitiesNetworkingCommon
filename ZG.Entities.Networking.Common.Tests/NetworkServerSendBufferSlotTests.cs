using NUnit.Framework;
using Unity.Collections;

public class NetworkServerSendBufferSlotTests
{
    [Test]
    public void DisconnectMiddle_ShiftsActiveSlotsAndRecyclesRemovedSlot()
    {
        var slots = CreateSlots(10, 20, 30, 99);

        try
        {
            int recycled = -20;
            NetworkServerSendBuffer.ShiftSlotsForDisconnect(
                ref slots,
                1,
                3,
                in recycled);

            CollectionAssert.AreEqual(
                new[] { 10, 30, -20, 99 },
                slots.AsArray().ToArray());
        }
        finally
        {
            slots.Dispose();
        }
    }

    [Test]
    public void DisconnectFirst_ShiftsEveryFollowingActiveSlotInOrder()
    {
        var slots = CreateSlots(10, 20, 30, 99);

        try
        {
            int recycled = -10;
            NetworkServerSendBuffer.ShiftSlotsForDisconnect(
                ref slots,
                0,
                3,
                in recycled);

            CollectionAssert.AreEqual(
                new[] { 20, 30, -10, 99 },
                slots.AsArray().ToArray());
        }
        finally
        {
            slots.Dispose();
        }
    }

    [Test]
    public void RepeatedDisconnectReconnect_PreservesSlotOwnership()
    {
        var slots = CreateSlots(8751, 8737, 8758, 0);

        try
        {
            int recycled8751 = 0;
            NetworkServerSendBuffer.ShiftSlotsForDisconnect(
                ref slots,
                0,
                3,
                in recycled8751);
            slots[2] = 8751;

            int recycled8737 = 0;
            NetworkServerSendBuffer.ShiftSlotsForDisconnect(
                ref slots,
                0,
                3,
                in recycled8737);
            slots[2] = 8737;

            CollectionAssert.AreEqual(
                new[] { 8758, 8751, 8737, 0 },
                slots.AsArray().ToArray());
        }
        finally
        {
            slots.Dispose();
        }
    }

    private static NativeList<int> CreateSlots(params int[] values)
    {
        var slots = new NativeList<int>(values.Length, Allocator.Temp);
        for (int i = 0; i < values.Length; ++i)
            slots.Add(values[i]);

        return slots;
    }
}
