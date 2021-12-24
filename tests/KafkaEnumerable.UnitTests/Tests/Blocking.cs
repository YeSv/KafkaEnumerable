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
        using var cts = new CancellationTokenSource(TestTimeout.Divide(2));
        var consumer = new InMemoryConsumer<byte[], byte[]>(Topic, v => v, new Dictionary<int, byte[][]>
        {
            [0] = new byte[0][]
        });

        var stream = KafkaEnumerable.Single(consumer, cancellationToken: cts.Token);
        var eof = stream.First();

        eof.ConsumeResult!.IsPartitionEOF.Should().BeTrue();
        Assert.Throws<OperationCanceledException>(() => stream.First());
    }

    [Fact]
    public void Multiple_Should_Block_Until_Cancellation_Occures()
    {
        using var cts = new CancellationTokenSource(TestTimeout.Divide(2));
        var consumers = Enumerable.Range(0, 3).Select(_ => new InMemoryConsumer<byte[], byte[]>(Topic, v => v, new Dictionary<int, byte[][]>
        {
            [0] = new byte[0][]
        })).ToArray();

        var stream = KafkaEnumerable.Multiple(consumers, cancellationToken: cts.Token);
        var eofs = stream.Take(consumers.Length).ToArray();

        eofs.All(m => m.ConsumeResult!.IsPartitionEOF).Should().BeTrue();
        Assert.Throws<OperationCanceledException>(() => stream.First());
    }

    [Fact]
    public void Priority_Should_Block_Until_Cancellation_Occures()
    {
        using var cts = new CancellationTokenSource(TestTimeout.Divide(2));
        var consumers = Enumerable.Range(0, 3).Select(_ => new InMemoryConsumer<byte[], byte[]>(Topic, v => v, new Dictionary<int, byte[][]>
        {
            [0] = new byte[0][]
        })).ToArray();

        var stream = KafkaEnumerable.Priority(consumers, cancellationToken: cts.Token);
        var eofs = stream.Take(consumers.Length).ToArray();

        eofs.All(m => m.ConsumeResult!.IsPartitionEOF).Should().BeTrue();
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
        var stream = KafkaEnumerable.Single(consumer, cancellationToken: cts.Token);

        var firstBatch = stream.Take(50).ToArray();
        var secondBatch = stream.Take(50).ToArray();
        var eof = stream.First();


        eof.HasData.Should().BeTrue();
        eof.ConsumeResult!.IsPartitionEOF.Should().BeTrue();
        firstBatch.All(m => m.HasData).Should().BeTrue();
        secondBatch.All(m => m.HasData).Should().BeTrue();
    }

    [Fact]
    public void Multiple_Should_Not_Block_If_Data_Available()
    {
        using var cts = new CancellationTokenSource(TestTimeout);
        var consumers = Enumerable.Range(0, 3).Select(_ => new InMemoryConsumer<byte[], byte[]>(Topic, v => v, new Dictionary<int, byte[][]>
        {
            [0] = Enumerable.Repeat(0, 100).Select(_ => Array.Empty<byte>()).ToArray()
        })).ToArray();
        var stream = KafkaEnumerable.Multiple(consumers, cancellationToken: cts.Token);

        var firstBatch = stream.Take(100).ToArray();
        var secondBatch = stream.Take(100).ToArray();
        var thirdBatch = stream.Take(100).ToArray();
        var eofs = stream.Take(3).ToArray();

        eofs.All(m => m.HasData && m.ConsumeResult!.IsPartitionEOF).Should().BeTrue();
        firstBatch.All(m => m.HasData).Should().BeTrue();
        secondBatch.All(m => m.HasData).Should().BeTrue();
        thirdBatch.All(m => m.HasData).Should().BeTrue();
    }

    [Fact]
    public void Priority_Should_Not_Block_If_Data_Available()
    {
        using var cts = new CancellationTokenSource(TestTimeout);
        var consumers = Enumerable.Range(0, 3).Select(_ => new InMemoryConsumer<byte[], byte[]>(Topic, v => v, new Dictionary<int, byte[][]>
        {
            [0] = Enumerable.Repeat(0, 100).Select(_ => Array.Empty<byte>()).ToArray()
        })).ToArray();
        var stream = KafkaEnumerable.Priority(consumers, cancellationToken: cts.Token);

        var firstBatch = stream.Take(101).ToArray();
        var secondBatch = stream.Take(101).ToArray();
        var thirdBatch = stream.Take(101).ToArray();

        firstBatch.All(m => m.HasData).Should().BeTrue();
        firstBatch[^1].ConsumeResult!.IsPartitionEOF.Should().BeTrue();
        secondBatch.All(m => m.HasData).Should().BeTrue();
        secondBatch[^1].ConsumeResult!.IsPartitionEOF.Should().BeTrue();
        thirdBatch.All(m => m.HasData).Should().BeTrue();
        thirdBatch[^1].ConsumeResult!.IsPartitionEOF.Should().BeTrue();
    }
}
