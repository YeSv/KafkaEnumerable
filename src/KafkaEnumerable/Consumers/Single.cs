using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

namespace KafkaEnumerable.Consumers.Single;

public readonly struct SingleMessage<TKey, TValue>
{
    readonly object? _data;

    public SingleMessage(ConsumeResult<TKey, TValue>? result) => _data = result;
    public SingleMessage(ConsumeException error) => _data = error;

    public bool HasData => _data is not null;
    public bool IsError => _data is not null and ConsumeException;

    public ConsumeException? Error => _data as ConsumeException;
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

    public ConsumeOptions(bool? returnNulls, TimeSpan? consumeTimeout) => 
        (ReturnNulls, ConsumeTimeout) = (returnNulls ?? Defaults.ReturnNulls, consumeTimeout ?? Defaults.ConsumeTimeout);
}

internal static class Consumer
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IEnumerable<SingleMessage<TKey, TValue>> Consume<TKey, TValue>(IConsumer<TKey, TValue> consumer, ConsumeOptions options, CancellationToken token) => 
        EnumerableOwner.Wrap(Generator(consumer, options, token));

    static IEnumerable<SingleMessage<TKey, TValue>> Generator<TKey, TValue>(
        IConsumer<TKey, TValue> consumer, 
        ConsumeOptions options,
        CancellationToken token)
    {
        while (true)
        {
            var message = Consume(consumer, options.ConsumeTimeout, token);
            if (options.ReturnNulls || message.HasData) yield return message;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static SingleMessage<TKey, TValue> Consume<TKey, TValue>(IConsumer<TKey, TValue> consumer, TimeSpan timeout, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        try
        {
            return new(consumer.Consume(timeout));
        }
        catch (ConsumeException ex)
        {
            return new(ex);
        }
    }
}
