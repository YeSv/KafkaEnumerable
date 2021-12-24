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
            Thresholds = thresholds ?? Enumerable.Repeat(Defaults.Infinity, 1).Concat(Enumerable.Repeat(Defaults.MessagesThreshold, consumersNum - 1)).ToArray();
        }
    }

    internal sealed class ConsumerState
    {
        DateTime _nextFlush;

        public int Priority { get; set; }
        public ConsumeOptions Options { get; set; }
        public CancellationToken Token { get; set; }
        public int[] CurrentThresholds { get; set; }
        public bool ShouldCheckPriorities { get; set; }

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

    internal static class Consumer
    {
        public static IEnumerable<PriorityMessage<TKey, TValue>> Consume<TKey, TValue>(IConsumer<TKey, TValue>[] consumers, ConsumeOptions options, CancellationToken token) =>
            Generator(consumers, new(options, token));

        static IEnumerable<PriorityMessage<TKey, TValue>> Generator<TKey, TValue>(IConsumer<TKey, TValue>[] consumers, ConsumerState state)
        {
            while (true)
            {
                if (state.ShouldFlush()) goto Flush; // We need to flush due to timeout
                if (state.ShouldCheckPriorities) goto UpdatePriority; // If we need to check priorities - jump

                var message = Consume(consumers[state.Priority], state.Options.ConsumeTimeout, false, state.Priority, state.Token);
                if (state.CurrentThresholds[state.Priority] != Defaults.Infinity) state.CurrentThresholds[state.Priority]--; // Update current threshold
                if (state.CurrentThresholds[state.Priority] == 0 || !message.HasData) // Current priority threshold is reached or no meaningful data - reset threshold and try to move to next priority
                {
                    state.ShouldCheckPriorities = true;
                    state.CurrentThresholds[state.Priority] = state.Options.Thresholds[state.Priority];
                }
                if (state.Options.ReturnNulls || message.HasData) yield return message; // Yield message only if it has meaningful data or returnNulls is enabled
                continue;


            UpdatePriority:
                state.ShouldCheckPriorities = false;
                if (++state.Priority >= consumers.Length) state.Priority = 0;
                for (var i = 0; i < state.Priority; i++) // Check priorities before current
                {
                    var prevMessage = Consume(consumers[i], state.Options.ConsumeTimeout, false, i, state.Token);
                    if (prevMessage.HasData) // Just check here if previous priority has data (returnNulls is ignored here)
                    {
                        state.Priority = i; // We can jump to previous priority because consumer returned meaningful data
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
            catch (ConsumeException ex)
            {
                return new(consumer, ex, isFlush, priority);
            }
        }
    }
}