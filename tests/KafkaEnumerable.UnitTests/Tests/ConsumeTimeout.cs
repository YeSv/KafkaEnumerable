using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace KafkaEnumerable.UnitTests.Tests;

public class ConsumeTimeoutTests
{
    static readonly string Topic = nameof(ConsumeTimeoutTests);
    static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    [Fact]
    public void Single_Should_Respect_Consume_Timeout()
    {
        using var cts = new CancellationTokenSource(Timeout);
        var consumer = new InMemoryConsumer<byte[], byte[]>(Topic, v => v, new Dictionary<int, byte[][]>
        {
            [0] = new byte[0][]
        }, sleep: true);


        var stream = KafkaEnumerables.Single(consumer, consumeTimeout: Timeout.Divide(2), returnNulls: true, cancellationToken: cts.Token);

        stream.First().ConsumeResult!.IsPartitionEOF.Should().BeTrue();

        var sw = Stopwatch.StartNew();
        stream.First().HasData.Should().BeFalse();
        sw.Stop();

        sw.Elapsed.Should().BeGreaterThanOrEqualTo(Timeout.Divide(2)).And.BeLessThan(Timeout);
    }

    [Fact]
    public void Multiple_Should_Respect_Consume_Timeout()
    {
        using var cts = new CancellationTokenSource(Timeout);
        var consumers = Enumerable.Repeat(new InMemoryConsumer<byte[], byte[]>(Topic, v => v, new Dictionary<int, byte[][]>
        {
            [0] = new byte[0][]
        }, sleep: true),1).ToArray();


        var stream = KafkaEnumerables.Multiple(consumers, consumeTimeout: Timeout.Divide(2), returnNulls: true, cancellationToken: cts.Token);

        stream.First().ConsumeResult!.IsPartitionEOF.Should().BeTrue();

        var sw = Stopwatch.StartNew();
        stream.First().HasData.Should().BeFalse();
        sw.Stop();

        sw.Elapsed.Should().BeGreaterThanOrEqualTo(Timeout.Divide(2)).And.BeLessThan(Timeout);
    }

    [Fact]
    public void Priority_Should_Respect_Consume_Timeout()
    {
        using var cts = new CancellationTokenSource(Timeout);
        var consumers = Enumerable.Repeat(new InMemoryConsumer<byte[], byte[]>(Topic, v => v, new Dictionary<int, byte[][]>
        {
            [0] = new byte[0][]
        }, sleep: true), 1).ToArray();


        var stream = KafkaEnumerables.Priority(consumers, consumeTimeout: Timeout.Divide(2), returnNulls: true, cancellationToken: cts.Token);

        stream.First().ConsumeResult!.IsPartitionEOF.Should().BeTrue();

        var sw = Stopwatch.StartNew();
        stream.First().HasData.Should().BeFalse();
        sw.Stop();

        sw.Elapsed.Should().BeGreaterThanOrEqualTo(Timeout.Divide(2)).And.BeLessThan(Timeout);
    }
}

