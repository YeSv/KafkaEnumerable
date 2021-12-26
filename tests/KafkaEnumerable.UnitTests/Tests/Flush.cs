using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Xunit;

namespace KafkaEnumerable.UnitTests.Tests;

public class FlushTests
{
    static readonly string Topic = nameof(FlushTests);
    static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(10);

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Multiple_Should_Flush_Messages(bool returnNulls)
    {
        using var cts = new CancellationTokenSource(TestTimeout);
        var consumers = Enumerable.Range(0, 3).Select(_ => new InMemoryConsumer<byte[], byte[]>(Topic, v => v, new Dictionary<int, byte[][]>
        {
            [0] = Enumerable.Repeat(0, 100).Select(_ => Array.Empty<byte>()).ToArray()
        })).ToArray();
        var stream = KafkaEnumerables.Multiple(consumers, cancellationToken: cts.Token, flushInterval: TestTimeout.Divide(4), returnNulls: returnNulls);

        _ = stream.First(); // ignore

        Thread.Sleep(TestTimeout.Divide(2));

        stream.Take(3).Reverse().Select((m, i) => m.IsFlush && consumers[i] == m.Consumer).All(m => m).Should().BeTrue();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Priority_Should_Flush_Messages(bool returnNulls)
    {
        using var cts = new CancellationTokenSource(TestTimeout);
        var consumers = Enumerable.Range(0, 3).Select(_ => new InMemoryConsumer<byte[], byte[]>(Topic, v => v, new Dictionary<int, byte[][]>
        {
            [0] = Enumerable.Repeat(0, 100).Select(_ => Array.Empty<byte>()).ToArray()
        })).ToArray();
        var stream = KafkaEnumerables.Priority(consumers, cancellationToken: cts.Token, flushInterval: TestTimeout.Divide(4), returnNulls: returnNulls);

        _ = stream.First(); // ignore

        Thread.Sleep(TestTimeout.Divide(2));

        stream.Take(3).Reverse().Select((m, i) => m.IsFlush && consumers[i] == m.Consumer && i == m.Priority).All(m => m).Should().BeTrue();
    }
}
