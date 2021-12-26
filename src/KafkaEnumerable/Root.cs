using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

namespace KafkaEnumerable;

public static class KafkaEnumerables
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IEnumerable<Consumers.Single.SingleMessage<TKey, TValue>> Single<TKey, TValue>(
        IConsumer<TKey, TValue> consumer,
        TimeSpan? consumeTimeout = default,
        bool? returnNulls = default,
        CancellationToken cancellationToken = default) => Consumers.Single.Consumer.Consume(consumer, new(returnNulls, consumeTimeout), cancellationToken);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IEnumerable<Consumers.Multiple.MultiMessage<TKey, TValue>> Multiple<TKey, TValue>(
        IConsumer<TKey, TValue>[] consumers,
        TimeSpan? consumeTimeout = default,
        TimeSpan? flushInterval = default,
        int[]? thresholds = default,
        bool? returnNulls = default,
        CancellationToken cancellationToken = default) => Consumers.Multiple.Consumer.Consume(consumers, new(returnNulls, consumeTimeout, flushInterval, thresholds, consumers.Length), cancellationToken);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IEnumerable<Consumers.Priority.PriorityMessage<TKey, TValue>> Priority<TKey, TValue>(
        IConsumer<TKey, TValue>[] consumers,
        TimeSpan? consumeTimeout = default,
        TimeSpan? flushInterval = default,
        int[]? thresholds = default,
        bool? returnNulls = default,
        CancellationToken cancellationToken = default) => Consumers.Priority.Consumer.Consume(consumers, new(returnNulls, consumeTimeout, flushInterval, thresholds, consumers.Length), cancellationToken);
}