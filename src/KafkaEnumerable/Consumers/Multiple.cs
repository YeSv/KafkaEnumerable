using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

namespace KafkaEnumerable.Consumers.Multiple;

public readonly struct MultiMessage<TKey, TValue>
{
    readonly object? _data;

    public MultiMessage(IConsumer<TKey, TValue> consumer, ConsumeResult<TKey, TValue>? result, bool isFlush) => (Consumer, _data, IsFlush) = (consumer, result, isFlush);
    public MultiMessage(IConsumer<TKey, TValue> consumer, ConsumeException error, bool isFlush) => (Consumer, _data, IsFlush) = (consumer, error, isFlush);

    public bool IsFlush { get; }
    public IConsumer<TKey, TValue> Consumer { get; }

    public bool HasData => _data is not null;
    public bool IsError => _data is not null and ConsumeException;
    public KafkaException? Error => _data as ConsumeException;
    public ConsumeResult<TKey, TValue>? ConsumeResult => _data as ConsumeResult<TKey, TValue>;

    public void Deconstruct(out ConsumeResult<TKey, TValue>? result, out ConsumeException? error)
    {
        result = _data as ConsumeResult<TKey, TValue>;
        error = _data as ConsumeException;
    }
}

internal readonly struct ConsumeOptions
{
    public readonly bool ReturnNulls;
    public readonly TimeSpan ConsumeTimeout;
    public readonly TimeSpan FlushInterval;
    public readonly int[] Thresholds;

    public ConsumeOptions(bool? returnNulls, TimeSpan? consumeTimeout, TimeSpan? flushInterval, int[]? thresholds, int consumersNum)
    {
        ReturnNulls = returnNulls ?? Defaults.ReturnNulls;
        ConsumeTimeout = consumeTimeout ?? Defaults.ConsumeTimeout;
        FlushInterval = flushInterval ?? Defaults.FlushInterval;
        Thresholds = thresholds ?? Enumerable.Repeat(Defaults.MessagesThreshold, consumersNum).ToArray();
    }
}

internal static class Consumer
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IEnumerable<MultiMessage<TKey, TValue>> Consume<TKey, TValue>(IConsumer<TKey, TValue>[] consumers, ConsumeOptions options, CancellationToken token) => 
        EnumerableOwner.Wrap(Generator(consumers, options, token));

    static IEnumerable<MultiMessage<TKey, TValue>> Generator<TKey, TValue>(
        IConsumer<TKey, TValue>[] consumers,
        ConsumeOptions options,
        CancellationToken token)
    {
        var (consumer, moveNext, nextFlush, currentThresholds) = (0, false, DateTime.UtcNow.Add(options.FlushInterval), options.Thresholds.ToArray());
        while (true)
        {
            if (DateTime.UtcNow > nextFlush) goto Flush; // If flush interval is reached - flush
            if (moveNext) goto MoveToNext; // Check if we need to select another consumer

            var message = Consume(consumers[consumer], options.ConsumeTimeout, false, token);
            if (--currentThresholds[consumer] <= 0 || !message.HasData) // Reached available threshold or no meaningful data - update thresholds and try to move to next consumer
            {
                moveNext = true;
                currentThresholds[consumer] = options.Thresholds[consumer];
            }
            if (options.ReturnNulls || message.HasData) yield return message; // Yield if returnNulls is enabled or if received meaningful data

            continue;


        MoveToNext:
            moveNext = false;
            if (++consumer >= consumers.Length) consumer = 0;

            continue;


        Flush:
            nextFlush = DateTime.Now.Add(options.FlushInterval); // Update flush deadline
            for (var i = consumers.Length - 1; i >= 0; i--) // Consume from all consumers to handle repartitioning/(un)assign events
            {
                var flushMessage = Consume(consumers[i], options.ConsumeTimeout, true, token);
                if (flushMessage.HasData) yield return flushMessage; // Yield if received meaningful data
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static MultiMessage<TKey, TValue> Consume<TKey, TValue>(
        IConsumer<TKey, TValue> consumer, 
        TimeSpan timeout, 
        bool isFlush,
        CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        try
        {
            return new(consumer, consumer.Consume(timeout), isFlush);
        }
        catch (ConsumeException ex)
        {
            return new(consumer, ex, isFlush);
        }
    }
}
