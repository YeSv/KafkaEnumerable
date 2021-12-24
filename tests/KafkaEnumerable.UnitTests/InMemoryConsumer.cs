using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace KafkaEnumerable.UnitTests;

public sealed class InMemoryConsumer<TKey, TValue> : IConsumer<TKey, TValue>
{
    private int _partitionIndex = 0;

    readonly string _topic;
    readonly int[] _partitions;
    readonly Func<TValue, TKey> _keyMapper;
    readonly Dictionary<int, long> _offsets;
    readonly Dictionary<int, TValue[]> _messagesPerPartiton;


    public InMemoryConsumer(
        string topic,
        Func<TValue, TKey> keyMapper,
        Dictionary<int, TValue[]> messagesPerPartition)
    {
        _topic = topic;
        _keyMapper = keyMapper;
        _messagesPerPartiton = messagesPerPartition;
        _partitions = messagesPerPartition.Keys.ToArray();
        _offsets = messagesPerPartition.Keys.ToDictionary(k => k, k => 0L);
    }

    public ConsumeResult<TKey, TValue>? Consume(CancellationToken cancellationToken = default)
    {
        if (_partitionIndex >= _partitions.Length) return null; // No new messages

        var partition = _partitions[_partitionIndex];
        if (_offsets[partition] == _messagesPerPartiton[partition].Length) // Reached the end of partition - return EOF
        {
            _partitionIndex++;
            return new ConsumeResult<TKey, TValue>
            {
                Topic = _topic,
                Partition = partition,
                Offset = _offsets[partition],
                IsPartitionEOF = true
            };
        }

        // Get current offset and increment for the next call
        var (currentOffset, _) = (_offsets[partition], _offsets[partition] = _offsets[partition] + 1);

        var data = _messagesPerPartiton[partition][currentOffset];
        if (data == null) return null; // If data is null - emulate no data from broker

        return new ConsumeResult<TKey, TValue>
        {
            Topic = _topic,
            Partition = partition,
            Offset = currentOffset,
            Message = new Message<TKey, TValue>
            {
                Key = _keyMapper(data),
                Value = data
            }
        };
    }

    public ConsumeResult<TKey, TValue> Consume(TimeSpan timeout) => Consume(default(CancellationToken));

    public ConsumeResult<TKey, TValue> Consume(int millisecondsTimeout) => Consume(default(CancellationToken));

    #region Not Implemented

    public string MemberId => throw new NotImplementedException();

    public List<TopicPartition> Assignment => throw new NotImplementedException();

    public List<string> Subscription => throw new NotImplementedException();

    public IConsumerGroupMetadata ConsumerGroupMetadata => throw new NotImplementedException();

    public Handle Handle => throw new NotImplementedException();

    public string Name => throw new NotImplementedException();

    public int AddBrokers(string brokers) => 0;

    public void Assign(TopicPartition partition) { }

    public void Assign(TopicPartitionOffset partition) { }

    public void Assign(IEnumerable<TopicPartitionOffset> partitions) { }

    public void Assign(IEnumerable<TopicPartition> partitions) { }

    public void Close() { }

    public List<TopicPartitionOffset> Commit() => new List<TopicPartitionOffset>();

    public void Commit(IEnumerable<TopicPartitionOffset> offsets) { }

    public void Commit(ConsumeResult<TKey, TValue> result) { }

    public List<TopicPartitionOffset> Committed(TimeSpan timeout) => new List<TopicPartitionOffset>();

    public List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout) => new List<TopicPartitionOffset>();

    public void Dispose() { }

    public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition) => throw new NotImplementedException();

    public void IncrementalAssign(IEnumerable<TopicPartitionOffset> partitions) { }

    public void IncrementalAssign(IEnumerable<TopicPartition> partitions) { }

    public void IncrementalUnassign(IEnumerable<TopicPartition> partitions) { }

    public List<TopicPartitionOffset> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout) => new List<TopicPartitionOffset>();

    public void Pause(IEnumerable<TopicPartition> partitions) { }

    public Offset Position(TopicPartition partition) => default;

    public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout) => throw new NotImplementedException();

    public void Resume(IEnumerable<TopicPartition> partitions) { }

    public void Seek(TopicPartitionOffset tpo) { }

    public void StoreOffset(ConsumeResult<TKey, TValue> result) { }

    public void StoreOffset(TopicPartitionOffset offset) { }

    public void Subscribe(IEnumerable<string> topics) { }

    public void Subscribe(string topic) { }

    public void Unassign() { }

    public void Unsubscribe() { }

    #endregion
}
