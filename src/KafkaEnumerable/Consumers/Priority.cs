using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

namespace KafkaEnumerable.Consumers.Priority
{
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

    internal sealed class ConsumerState
    {
        DateTime _nextFlush;

        public int ConsumerIndex { get; set; }
        public ConsumeOptions Options { get; set; }
        public CancellationToken Token { get; set; }
        public int[] CurrentThresholds { get; set; }

        public ConsumerState(ConsumeOptions options, CancellationToken token)
        {
            _nextFlush = DateTime.UtcNow.Add(options.FlushInterval);

            Token = token;
            Options = options;
            CurrentThresholds = options.Thresholds.ToArray();
        }

        public bool ShouldFlush()
        {
            if (_nextFlush > DateTime.UtcNow) return false;

            _nextFlush = DateTime.UtcNow.Add(Options.FlushInterval);
            return true;
        }
    }

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
            KafkaException error, 
            bool isFlush,
            int priority) => (Consumer, _data, IsFlush, Priority) = (consumer, error, isFlush, priority);

        public int Priority { get; }
        public bool IsFlush { get; }
        public IConsumer<TKey, TValue> Consumer { get; }

        public bool HasData => _data is not null;
        public bool IsError => _data is not null and KafkaException;

        public KafkaException? Error => _data as KafkaException;
        public ConsumeResult<TKey, TValue>? ConsumeResult => _data as ConsumeResult<TKey, TValue>;
    }

    internal static class Consumer
    {
        public static IEnumerable<PriorityMessage<TKey, TValue>> Consume<TKey, TValue>(IConsumer<TKey, TValue>[] consumers, ConsumeOptions options, CancellationToken token) =>
            Generator(consumers, new(options, token));

        static IEnumerable<PriorityMessage<TKey, TValue>> Generator<TKey, TValue>(IConsumer<TKey, TValue>[] consumers, ConsumerState state)
        {
            while (true)
            {
                if (state.ShouldFlush()) goto Flush; // We need to flush due to timeout
                if (state.CurrentThresholds[state.ConsumerIndex] == 0) // Current priority threshold is reached, reset threshold and update priority
                {
                    state.CurrentThresholds[state.ConsumerIndex] = state.Options.Thresholds![state.ConsumerIndex];
                    goto UpdatePriority;
                }
                
                var message = Consume(consumers[state.ConsumerIndex], state.Options.ConsumeTimeout, false, state.ConsumerIndex, state.Token);
                if (state.CurrentThresholds[state.ConsumerIndex] != Defaults.Infinity) state.CurrentThresholds[state.ConsumerIndex]--; // Update current threshold
                if (state.Options.ReturnNulls || message.HasData) yield return message; // Yield message only if it has meaningful data or returnNulls is enabled
                if (message.HasData) continue; // HasData - no need to update priority

            UpdatePriority:
                if (++state.ConsumerIndex >= consumers.Length) state.ConsumerIndex = 0;
                for (var i = 0; i < consumers.Length; i++)
                {
                    var prevMessage = Consume(consumers[i], state.Options.ConsumeTimeout, false, i, state.Token);
                    if (prevMessage.HasData) // Just check here if previous priority has data (returnNulls is ignored here)
                    {
                        state.ConsumerIndex = i; // We can jump to previous priority because consumer returned meaningful data
                        yield return prevMessage; 
                        break;
                    }
                }
                continue;

            Flush:
                for (var i = consumers.Length - 1; i >= 0; i--) // Consume in reverse order to handle repartitioning/(un)assign events
                {
                    var flushMessage = Consume(consumers[i], state.Options.ConsumeTimeout, true, i, state.Token);
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
            catch (KafkaException ex)
            {
                return new(consumer, ex, isFlush, priority);
            }
        }
    }
}