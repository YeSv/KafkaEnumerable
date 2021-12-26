using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Xunit;

namespace KafkaEnumerable.UnitTests.Tests;

public class BlockingTests
{
    static readonly string Topic = nameof(BlockingTests);
    static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(10);

    [Fact]
    public void Single_Should_Block_Until_Cancellation_Occures()
    {
        using var cts = new CancellationTokenSource(TestTimeout);
        var consumer = new InMemoryConsumer<byte[], byte[]>(Topic, v => v, new Dictionary<int, byte[][]>
        {
            [0] = new byte[0][]
        });

        var stream = KafkaEnumerables.Single(consumer, cancellationToken: cts.Token);

        stream.First().ConsumeResult!.IsPartitionEOF.Should().BeTrue();
        Assert.Throws<OperationCanceledException>(() => stream.First());
    }

    [Fact]
    public void Multiple_Should_Block_Until_Cancellation_Occures()
    {
        using var cts = new CancellationTokenSource(TestTimeout);
        var consumers = Enumerable.Range(0, 3).Select(_ => new InMemoryConsumer<byte[], byte[]>(Topic, v => v, new Dictionary<int, byte[][]>
        {
            [0] = new byte[0][]
        })).ToArray();

        var stream = KafkaEnumerables.Multiple(consumers, cancellationToken: cts.Token);

        stream.Take(consumers.Length).All(m => m.ConsumeResult!.IsPartitionEOF).Should().BeTrue();
        Assert.Throws<OperationCanceledException>(() => stream.First());
    }

    [Fact]
    public void Priority_Should_Block_Until_Cancellation_Occures()
    {
        using var cts = new CancellationTokenSource(TestTimeout);
        var consumers = Enumerable.Range(0, 3).Select(_ => new InMemoryConsumer<byte[], byte[]>(Topic, v => v, new Dictionary<int, byte[][]>
        {
            [0] = new byte[0][]
        })).ToArray();

        var stream = KafkaEnumerables.Priority(consumers, cancellationToken: cts.Token);

        stream.Take(consumers.Length).All(m => m.ConsumeResult!.IsPartitionEOF).Should().BeTrue();
        Assert.Throws<OperationCanceledException>(() => stream.First());
    }

    [Fact]
    public void Single_Should_Not_Block_If_Data_Available()
    {
        using var cts = new CancellationTokenSource(TestTimeout);
        var consumer = new InMemoryConsumer<byte[], byte[]>(Topic, v => v, new Dictionary<int, byte[][]>
        {
            [0] = Enumerable.Repeat(0, 100).Select(_ => Array.Empty<byte>()).ToArray()
        });
        var stream = KafkaEnumerables.Single(consumer, cancellationToken: cts.Token);

        stream.Take(50).All(m => m.HasData).Should().BeTrue();
        stream.Take(50).All(m => m.HasData).Should().BeTrue();
        stream.First().ConsumeResult!.IsPartitionEOF.Should().BeTrue();
    }

    [Fact]
    public void Multiple_Should_Not_Block_If_Data_Available()
    {
        using var cts = new CancellationTokenSource(TestTimeout);
        var consumers = Enumerable.Range(0, 3).Select(_ => new InMemoryConsumer<byte[], byte[]>(Topic, v => v, new Dictionary<int, byte[][]>
        {
            [0] = Enumerable.Repeat(0, 100).Select(_ => Array.Empty<byte>()).ToArray()
        })).ToArray();
        var stream = KafkaEnumerables.Multiple(consumers, cancellationToken: cts.Token);

        stream.Take(100).All(m => m.HasData).Should().BeTrue();
        stream.Take(100).All(m => m.HasData).Should().BeTrue();
        stream.Take(100).All(m => m.HasData).Should().BeTrue();
        stream.Take(3).All(m => m.HasData && m.ConsumeResult!.IsPartitionEOF).Should().BeTrue();
    }

    [Fact]
    public void Priority_Should_Not_Block_If_Data_Available()
    {
        using var cts = new CancellationTokenSource(TestTimeout);
        var consumers = Enumerable.Range(0, 3).Select(_ => new InMemoryConsumer<byte[], byte[]>(Topic, v => v, new Dictionary<int, byte[][]>
        {
            [0] = Enumerable.Repeat(0, 100).Select(_ => Array.Empty<byte>()).ToArray()
        })).ToArray();
        var stream = KafkaEnumerables.Priority(consumers, cancellationToken: cts.Token);

        stream.Take(100).All(m => m.HasData).Should().BeTrue();
        stream.First().ConsumeResult!.IsPartitionEOF.Should().BeTrue();
        stream.Take(100).All(m => m.HasData).Should().BeTrue();
        stream.First().ConsumeResult!.IsPartitionEOF.Should().BeTrue();
        stream.Take(100).All(m => m.HasData).Should().BeTrue();
        stream.First().ConsumeResult!.IsPartitionEOF.Should().BeTrue();
    }
}
