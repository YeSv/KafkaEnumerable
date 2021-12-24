using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace KafkaEnumerable.UnitTests.Tests;

public class SingleTests
{
    static readonly string Topic = nameof(SingleTests);

    [Fact]
    public void Single_Should_Return_All_Records()
    {
        var consumer = new InMemoryConsumer<byte[], byte[]>(Topic, v => v, new Dictionary<int, byte[][]>
        {
            [0] = Enumerable.Range(0, 100).Select(v => Array.Empty<byte>()).ToArray(),
            [1] = Enumerable.Range(0, 100).Select(v => Array.Empty<byte>()).ToArray()
        });

        var stream = KafkaEnumerable.Single(consumer);

        stream.Take(100).All(m => m.HasData && m.ConsumeResult!.Partition == 0).Should().BeTrue();
        stream.First().ConsumeResult!.IsPartitionEOF.Should().BeTrue();

        stream.Take(100).All(m => m.HasData && m.ConsumeResult!.Partition == 1).Should().BeTrue();
        stream.First().ConsumeResult!.IsPartitionEOF.Should().BeTrue();
    }
}