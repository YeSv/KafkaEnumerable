using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Xunit;

namespace KafkaEnumerable.UnitTests.Tests;

public class PriorityTests
{
    static readonly string Topic = nameof(PriorityTests);
    static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(10);

    [Fact]
    public void Priority_Should_Consume_Messages_In_Prioritized_Manner()
    {
        using var cts = new CancellationTokenSource(TestTimeout);
        var consumers = Enumerable.Range(0, 3).Select(_ => new InMemoryConsumer<byte[], byte[]>(Topic, v => v, new Dictionary<int, byte[][]>
        {
            [0] = Enumerable.Repeat(0, 100).Select(_ => Array.Empty<byte>()).ToArray()
        })).ToArray();
        var stream = KafkaEnumerable.Priority(consumers, cancellationToken: cts.Token, thresholds: new[] { 1, 1, 1 });

        stream.Take(100).All(m => m.HasData && m.Priority == 0).Should().BeTrue();
        stream.First().ConsumeResult!.IsPartitionEOF.Should().BeTrue();
        stream.Take(100).All(m => m.HasData && m.Priority == 1).Should().BeTrue();
        stream.First().ConsumeResult!.IsPartitionEOF.Should().BeTrue();
        stream.Take(100).All(m => m.HasData && m.Priority == 2).Should().BeTrue();
        stream.First().ConsumeResult!.IsPartitionEOF.Should().BeTrue();
    }
}

