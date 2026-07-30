using NUnit.Framework;
using Unity.Collections;
using ZG;

public class NetworkRelayInjectGroupingTests
{
    [Test]
    public void InterleavedIds_AreGroupedStablyInFirstSeenOrder()
    {
        var injects = new NativeList<NetworkRelayServerInjectSingleton.Inject>(Allocator.Temp);
        var groups = new NativeList<NetworkRelayServer.InjectGroup>(Allocator.Temp);
        var groupedInjectIndices = new NativeList<int>(Allocator.Temp);
        var groupIndices = new NativeHashMap<uint, int>(1, Allocator.Temp);

        try
        {
            AddInject(ref injects, 1214);
            AddInject(ref injects, 1215);
            AddInject(ref injects, 1214);
            AddInject(ref injects, 1216);
            AddInject(ref injects, 1215);
            AddInject(ref injects, 1214);

            ExecuteGrouping(
                ref injects,
                ref groups,
                ref groupedInjectIndices,
                ref groupIndices);

            Assert.That(groups.Length, Is.EqualTo(3));
            AssertGroup(groups[0], 1214, 0, 3);
            AssertGroup(groups[1], 1215, 3, 2);
            AssertGroup(groups[2], 1216, 5, 1);
            CollectionAssert.AreEqual(
                new[] { 0, 2, 5, 1, 4, 3 },
                groupedInjectIndices.AsArray().ToArray());
        }
        finally
        {
            groupIndices.Dispose();
            groupedInjectIndices.Dispose();
            groups.Dispose();
            injects.Dispose();
        }
    }

    [Test]
    public void SameIdBurst_ProducesOneOrderedSerialGroup()
    {
        var injects = new NativeList<NetworkRelayServerInjectSingleton.Inject>(Allocator.Temp);
        var groups = new NativeList<NetworkRelayServer.InjectGroup>(Allocator.Temp);
        var groupedInjectIndices = new NativeList<int>(Allocator.Temp);
        var groupIndices = new NativeHashMap<uint, int>(1, Allocator.Temp);

        try
        {
            const int injectCount = 32;
            for (int i = 0; i < injectCount; ++i)
                AddInject(ref injects, 9001);

            ExecuteGrouping(
                ref injects,
                ref groups,
                ref groupedInjectIndices,
                ref groupIndices);

            Assert.That(groups.Length, Is.EqualTo(1));
            AssertGroup(groups[0], 9001, 0, injectCount);
            Assert.That(groupedInjectIndices.Length, Is.EqualTo(injectCount));
            for (int i = 0; i < injectCount; ++i)
                Assert.That(groupedInjectIndices[i], Is.EqualTo(i));
        }
        finally
        {
            groupIndices.Dispose();
            groupedInjectIndices.Dispose();
            groups.Dispose();
            injects.Dispose();
        }
    }

    private static void AddInject(
        ref NativeList<NetworkRelayServerInjectSingleton.Inject> injects,
        uint id)
    {
        injects.Add(new NetworkRelayServerInjectSingleton.Inject
        {
            id = id
        });
    }

    private static void ExecuteGrouping(
        ref NativeList<NetworkRelayServerInjectSingleton.Inject> injects,
        ref NativeList<NetworkRelayServer.InjectGroup> groups,
        ref NativeList<int> groupedInjectIndices,
        ref NativeHashMap<uint, int> groupIndices)
    {
        var job = new NetworkRelayServer.BuildInjectGroups
        {
            injects = injects,
            groups = groups,
            groupedInjectIndices = groupedInjectIndices,
            groupIndices = groupIndices
        };
        job.Execute();
    }

    private static void AssertGroup(
        in NetworkRelayServer.InjectGroup group,
        uint id,
        int offset,
        int count)
    {
        Assert.That(group.id, Is.EqualTo(id));
        Assert.That(group.offset, Is.EqualTo(offset));
        Assert.That(group.count, Is.EqualTo(count));
    }
}
