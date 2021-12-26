using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Xunit;

namespace KafkaEnumerable.UnitTests.Tests;

public class ReturnNullsTests
{
    static readonly string Topic = nameof(ReturnNullsTests);
    static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(10);

    [Fact]
    public void Single_Should_Not_Block_If_Consumer_Is_Empty()
    {
        using var cts = new CancellationTokenSource(TestTimeout);
        var consumer = new InMemoryConsumer<byte[], byte[]>(Topic, v => v, new Dictionary<int, byte[][]>
        {
            [0] = new byte[0][]
        });

        var stream = KafkaEnumerables.Single(consumer, returnNulls: true, cancellationToken: cts.Token);

        stream.First().ConsumeResult!.IsPartitionEOF.Should().BeTrue();
        stream.Take(100).All(m => !m.HasData).Should().BeTrue();
    }

    [Fact]
    public void Multiple_Should_Not_Block_If_Consumers_Are_Empty()
    {
        using var cts = new CancellationTokenSource(TestTimeout);
        var consumers = Enumerable.Range(0, 3).Select(_ => new InMemoryConsumer<byte[], byte[]>(Topic, v => v, new Dictionary<int, byte[][]>
        {
            [0] = new byte[0][]
        })).ToArray();

        var stream = KafkaEnumerables.Multiple(consumers, returnNulls: true, cancellationToken: cts.Token);

        stream.Take(consumers.Length * 2).Where((e, i) => i % 2 == 0).All(m => m.ConsumeResult!.IsPartitionEOF).Should().BeTrue();
        stream.Take(100).All(m => !m.HasData).Should().BeTrue();
    }

    [Fact]
    public void Priority_Should_Not_Block_If_Consumers_Are_Empty()
    {
        using var cts = new CancellationTokenSource(TestTimeout);
        var consumers = Enumerable.Range(0, 3).Select(_ => new InMemoryConsumer<byte[], byte[]>(Topic, v => v, new Dictionary<int, byte[][]>
        {
            [0] = new byte[0][]
        })).ToArray();

        var stream = KafkaEnumerables.Priority(consumers, returnNulls: true, cancellationToken: cts.Token);

        stream.Take(consumers.Length * 2).Where((e, i) => i % 2 == 0).All(m => m.ConsumeResult!.IsPartitionEOF).Should().BeTrue();
        stream.Take(100).All(m => !m.HasData).Should().BeTrue();
    }

    [Fact]
    public void Single_Should_Start_Returning_Empty_Batches_After_EOF()
    {
        using var cts = new CancellationTokenSource(TestTimeout);
        var consumer = new InMemoryConsumer<byte[], byte[]>(Topic, v => v, new Dictionary<int, byte[][]>
        {
            [0] = Enumerable.Range(0, 100).Select(_ => Array.Empty<byte>()).ToArray()
        });

        var stream = KafkaEnumerables.Single(consumer, returnNulls: true, cancellationToken: cts.Token);
        
        stream.Take(100).All(m => m.HasData).Should().BeTrue();
        stream.Take(1).All(m => m.HasData && m.ConsumeResult!.IsPartitionEOF).Should().BeTrue();
        stream.Take(100).Where(m => m.HasData).ToArray().Should().BeEmpty();
    }

    [Fact]
    public void Multiple_Should_Start_Returning_Empty_Batches_After_EOF()
    {
        using var cts = new CancellationTokenSource(TestTimeout);
        var consumers = Enumerable.Range(0, 3).Select(_ => new InMemoryConsumer<byte[], byte[]>(Topic, v => v, new Dictionary<int, byte[][]>
        {
            [0] = Enumerable.Range(0, 100).Select(_ => Array.Empty<byte>()).ToArray()
        })).ToArray();

        var stream = KafkaEnumerables.Multiple(consumers, returnNulls: true, cancellationToken: cts.Token);

        _ = stream.Take(306).ToArray();
        stream.Take(100).Where(m => m.HasData).ToArray().Should().BeEmpty();
    }

    [Fact]
    public void Priority_Should_Start_Returning_Empty_Batches_After_EOF()
    {
        using var cts = new CancellationTokenSource(TestTimeout);
        var consumers = Enumerable.Range(0, 3).Select(_ => new InMemoryConsumer<byte[], byte[]>(Topic, v => v, new Dictionary<int, byte[][]>
        {
            [0] = Enumerable.Range(0, 20).Select(_ => Array.Empty<byte>()).ToArray()
        })).ToArray();

        var stream = KafkaEnumerables.Priority(consumers, returnNulls: true, cancellationToken: cts.Token);

        var firstBatch = stream.Take(22).ToArray();
        var secondBatch = stream.Take(21).ToArray();
        var thirdBatch = stream.Take(22).ToArray();
        var skipped = stream.Take(9).ToArray();
        stream.Take(100).Where(m => m.HasData).ToArray().Should().BeEmpty();
    }
}
