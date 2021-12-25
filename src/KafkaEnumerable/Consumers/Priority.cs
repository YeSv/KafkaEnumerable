using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

namespace KafkaEnumerable.Consumers.Priority;

public readonly struct PriorityMessage<TKey, TValue>
{
    readonly object? _data;

    public PriorityMessage(
        IConsumer<TKey, TValue> consumer,
        ConsumeResult<TKey, TValue>? result,
        bool isFlush,
        int priority) => (Consumer, _data, IsFlush, Priority) = (consumer, result, isFlush, priority);

    public PriorityMessage(
        IConsumer<TKey, TValue> consumer,
        ConsumeException error,
        bool isFlush,
        int priority) => (Consumer, _data, IsFlush, Priority) = (consumer, error, isFlush, priority);

    public int Priority { get; }
    public bool IsFlush { get; }
    public IConsumer<TKey, TValue> Consumer { get; }

    public bool HasData => _data is not null;
    public bool IsError => _data is not null and ConsumeException;

    public ConsumeException? Error => _data as ConsumeException;
    public ConsumeResult<TKey, TValue>? ConsumeResult => _data as ConsumeResult<TKey, TValue>;
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
        Thresholds = thresholds ?? Enumerable.Repeat(Defaults.Infinity, 1).Concat(Enumerable.Repeat(Defaults.MessagesThreshold, consumersNum - 1)).ToArray();
    }
}

internal static class Consumer
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IEnumerable<PriorityMessage<TKey, TValue>> Consume<TKey, TValue>(IConsumer<TKey, TValue>[] consumers, ConsumeOptions options, CancellationToken token) =>
        EnumerableOwner.Wrap(Generator(consumers, options, token));

    static IEnumerable<PriorityMessage<TKey, TValue>> Generator<TKey, TValue>(IConsumer<TKey, TValue>[] consumers, ConsumeOptions options, CancellationToken token)
    {
        var (priority, checkPriority, nextFlush, currentThresholds) = (0, false, DateTime.UtcNow.Add(options.FlushInterval), options.Thresholds.ToArray());
        while (true)
        {
            if (DateTime.UtcNow > nextFlush) goto Flush; // We need to flush due to timeout
            if (checkPriority) goto UpdatePriority; // If we need to check priorities - jump

            var message = Consume(consumers[priority], options.ConsumeTimeout, false, priority, token);
            if (currentThresholds[priority] != Defaults.Infinity) currentThresholds[priority]--; // Update current threshold
            if (currentThresholds[priority] == 0 || !message.HasData) // Current priority threshold is reached or no meaningful data - reset threshold and try to move to next priority
            {
                checkPriority = true;
                currentThresholds[priority] = options.Thresholds[priority];
            }
            if (options.ReturnNulls || message.HasData) yield return message; // Yield message only if it has meaningful data or returnNulls is enabled
            continue;


        UpdatePriority:
            checkPriority = false;
            if (++priority >= consumers.Length) priority = 0;
            for (var i = 0; i < priority; i++) // Check priorities before chosen one
            {
                var prevMessage = Consume(consumers[i], options.ConsumeTimeout, false, i, token);
                if (prevMessage.HasData) // Just check here if previous priority has data (returnNulls is ignored here)
                {
                    priority = i; // We can jump to previous priority because consumer returned meaningful data
                    yield return prevMessage; 
                    break;
                }
            }
            continue;

        Flush:
            nextFlush = DateTime.UtcNow.Add(options.FlushInterval); // Update flush deadline
            for (var i = consumers.Length - 1; i >= 0; i--) // Consume in reverse order to handle repartitioning/(un)assign events
            {
                var flushMessage = Consume(consumers[i], options.ConsumeTimeout, true, i, token);
                if (flushMessage.HasData) yield return flushMessage; // Yield message only if it has meaningful data
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static PriorityMessage<TKey, TValue> Consume<TKey, TValue>(
        IConsumer<TKey, TValue> consumer,
        TimeSpan timeout,
        bool isFlush,
        int priority,
        CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        try
        {
            return new(consumer, consumer.Consume(timeout), isFlush, priority);
        }
        catch (ConsumeException ex)
        {
            return new(consumer, ex, isFlush, priority);
        }
    }
}