using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Xunit;

namespace KafkaEnumerable.UnitTests.Tests;

public class MultipleTests
{
    static readonly string Topic = nameof(MultipleTests);
    static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(10);

    [Fact]
    public void Multiple_Should_Process_Messages_In_Round_Robin_Manner()
    {
        using var cts = new CancellationTokenSource(TestTimeout);
        var consumers = Enumerable.Range(0, 3).Select(_ => new InMemoryConsumer<byte[], byte[]>(Topic, v => v, new Dictionary<int, byte[][]>
        {
            [0] = Enumerable.Repeat(0, 100).Select(_ => Array.Empty<byte>()).ToArray()
        })).ToArray();
        
        var stream = KafkaEnumerables.Multiple(consumers, cancellationToken: cts.Token, thresholds: new [] { 50, 50, 50 });

        stream.Take(50).All(m => m.HasData && m.Consumer == consumers[0]).Should().BeTrue();
        stream.Take(50).All(m => m.HasData && m.Consumer == consumers[1]).Should().BeTrue();
        stream.Take(50).All(m => m.HasData && m.Consumer == consumers[2]).Should().BeTrue();
        stream.Take(50).All(m => m.HasData && m.Consumer == consumers[0]).Should().BeTrue();
        stream.Take(50).All(m => m.HasData && m.Consumer == consumers[1]).Should().BeTrue();
        stream.Take(50).All(m => m.HasData && m.Consumer == consumers[2]).Should().BeTrue();
        stream.Take(3).All(m => m.HasData && m.ConsumeResult!.IsPartitionEOF).Should().BeTrue();
    }
}
