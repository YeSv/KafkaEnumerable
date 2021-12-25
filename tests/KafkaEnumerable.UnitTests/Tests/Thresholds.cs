using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace KafkaEnumerable.UnitTests.Tests;

public class ThresholdsTests
{
    static readonly string Topic = nameof(ThresholdsTests);
    static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(10);

    [Fact]
    public void Multi_Should_Respect_Thresholds_When_Configured()
    {
        using var cts = new CancellationTokenSource(TestTimeout);
        var consumers = Enumerable.Range(0, 3).Select(_ => new InMemoryConsumer<byte[], byte[]>(
            Topic,
            v => v,
            new Dictionary<int, byte[][]>
            {
                [0] = Enumerable.Range(0, 100).Select(_ => Array.Empty<byte>()).ToArray()
            })).ToArray();

        var stream = KafkaEnumerable.Multiple(consumers, thresholds: new[] { 100, 100, 100 }, cancellationToken: cts.Token);

        stream.Take(100).All(m => m.Consumer == consumers[0] && m.HasData).Should().BeTrue();
        stream.Take(100).All(m => m.Consumer == consumers[1] && m.HasData).Should().BeTrue();
        stream.Take(100).All(m => m.Consumer == consumers[2] && m.HasData).Should().BeTrue();

        stream.Take(3).All(m => m.HasData && m.ConsumeResult!.IsPartitionEOF).Should().BeTrue();
    }

    [Fact]
    public void Priority_Should_Respect_Thresholds_When_Configured()
    {
        using var cts = new CancellationTokenSource(TestTimeout);
        var consumers = Enumerable.Range(0, 3).Select(_ => new InMemoryConsumer<byte[], byte[]>(
            Topic,
            v => v,
            new Dictionary<int, byte[][]>
            {
                [0] = Enumerable.Range(0, 100).Select(_ => Array.Empty<byte>()).ToArray()
            })).ToArray();

        var stream = KafkaEnumerable.Priority(consumers, thresholds: new[] { 100, 100, 100 }, cancellationToken: cts.Token);

        stream.Take(100).All(m => m.Priority == 0 && m.HasData).Should().BeTrue();
        stream.First().ConsumeResult!.IsPartitionEOF.Should().BeTrue();
        stream.Take(100).All(m => m.Priority == 1 && m.HasData).Should().BeTrue();
        stream.First().ConsumeResult!.IsPartitionEOF.Should().BeTrue();
        stream.Take(100).All(m => m.Priority == 2 && m.HasData).Should().BeTrue();
        stream.First().ConsumeResult!.IsPartitionEOF.Should().BeTrue();
    }

    [Fact]
    public void Multiple_Should_Move_Between_Priorities_Once_Threshold_Reached()
    {
        using var cts = new CancellationTokenSource(TestTimeout);
        var consumers = Enumerable.Range(0, 3).Select(_ => new InMemoryConsumer<byte[], byte[]>(
            Topic,
            v => v,
            new Dictionary<int, byte[][]>
            {
                [0] = Enumerable.Range(0, 100).Select(_ => Array.Empty<byte>()).ToArray()
            })).ToArray();

        var stream = KafkaEnumerable.Multiple(consumers, thresholds: new[] { 50, 50, 50 }, cancellationToken: cts.Token);

        stream.Take(50).All(m => m.Consumer == consumers[0] && m.HasData).Should().BeTrue();
        stream.Take(50).All(m => m.Consumer == consumers[1] && m.HasData).Should().BeTrue();
        stream.Take(50).All(m => m.Consumer == consumers[2] && m.HasData).Should().BeTrue();
        stream.Take(50).All(m => m.Consumer == consumers[0] && m.HasData).Should().BeTrue();
        stream.Take(50).All(m => m.Consumer == consumers[1] && m.HasData).Should().BeTrue();
        stream.Take(50).All(m => m.Consumer == consumers[2] && m.HasData).Should().BeTrue();
        stream.Take(3).All(m => m.HasData && m.ConsumeResult!.IsPartitionEOF).Should().BeTrue();
    }

    [Fact]
    public void Multiple_Should_Update_Priority_When_Null_Is_Returned()
    {
        using var cts = new CancellationTokenSource(TestTimeout);

        var messages = Enumerable.Range(0, 50).Select(_ => Array.Empty<byte>()).ToArray();
        var @break = new[] { (byte[])null! };

        var consumers = Enumerable.Range(0, 3).Select(_ => new InMemoryConsumer<byte[], byte[]>(
            Topic,
            v => v,
            new Dictionary<int, byte[][]>
            {
                [0] = messages.Concat(@break).Concat(messages).Concat(messages).ToArray()
            })).ToArray();

        var stream = KafkaEnumerable.Multiple(consumers, thresholds: new[] { 100, 100, 100 }, cancellationToken: cts.Token);

        stream.Take(50).All(m => m.Consumer == consumers[0] && m.HasData).Should().BeTrue();
        stream.Take(50).All(m => m.Consumer == consumers[1] && m.HasData).Should().BeTrue();
        stream.Take(50).All(m => m.Consumer == consumers[2] && m.HasData).Should().BeTrue();
        stream.Take(100).All(m => m.Consumer == consumers[0] && m.HasData).Should().BeTrue();
        stream.Take(100).All(m => m.Consumer == consumers[1] && m.HasData).Should().BeTrue();
        stream.Take(100).All(m => m.Consumer == consumers[2] && m.HasData).Should().BeTrue();
        stream.Take(3).All(m => m.HasData && m.ConsumeResult!.IsPartitionEOF).Should().BeTrue();
    }

    [Fact]
    public void Priority_Should_Still_Process_First_Priority_If_Threshold_Reached()
    {
        using var cts = new CancellationTokenSource(TestTimeout);
        var consumers = Enumerable.Range(0, 3).Select(_ => new InMemoryConsumer<byte[], byte[]>(
            Topic,
            v => v,
            new Dictionary<int, byte[][]>
            {
                [0] = Enumerable.Range(0, 100).Select(_ => Array.Empty<byte>()).ToArray()
            })).ToArray();

        var stream = KafkaEnumerable.Priority(consumers, thresholds: new[] { 50, 50, 50 }, cancellationToken: cts.Token);

        stream.Take(100).All(m => m.Priority == 0 && m.HasData).Should().BeTrue();
        stream.First().ConsumeResult!.IsPartitionEOF.Should().BeTrue();
        stream.Take(100).All(m => m.Priority == 1 && m.HasData).Should().BeTrue();
        stream.First().ConsumeResult!.IsPartitionEOF.Should().BeTrue();
        stream.Take(100).All(m => m.Priority == 2 && m.HasData).Should().BeTrue();
        stream.First().ConsumeResult!.IsPartitionEOF.Should().BeTrue();
    }

    [Fact]
    public void Priority_Should_Move_To_Next_Priority_If_Returned_Null()
    {
        var @break = new byte[][] { null, null };
        var messages = Enumerable.Range(0, 50).Select(_ => Array.Empty<byte>()).ToArray();

        using var cts = new CancellationTokenSource(TestTimeout);
        var consumers = Enumerable.Range(0, 3).Select(_ => new InMemoryConsumer<byte[], byte[]>(
            Topic,
            v => v,
            new Dictionary<int, byte[][]>
            {
                [0] = messages.Concat(@break).Concat(messages).ToArray()
            })).ToArray();

        var stream = KafkaEnumerable.Priority(consumers, thresholds: new[] { 100, 100, 100 }, cancellationToken: cts.Token);

        stream.Take(50).All(m => m.Priority == 0 && m.HasData).Should().BeTrue();
        stream.Take(50).All(m => m.Priority == 1 && m.HasData).Should().BeTrue();
        stream.Take(50).All(m => m.Priority == 0 && m.HasData).Should().BeTrue();
        stream.First().ConsumeResult!.IsPartitionEOF.Should().BeTrue();
        stream.Take(50).All(m => m.Priority == 1 && m.HasData).Should().BeTrue();
        stream.First().ConsumeResult!.IsPartitionEOF.Should().BeTrue();
        stream.Take(100).All(m => m.Priority == 2 && m.HasData).Should().BeTrue();
        stream.First().ConsumeResult!.IsPartitionEOF.Should().BeTrue();
    }
}
