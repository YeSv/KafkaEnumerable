## NuGet

[KafkaEnumerable](https://www.nuget.org/packages/KafkaEnumerable/)

## Table of contents
1. [Motivation](#1-motivation)
2. [API](#2-api)
3. [Single](#3-single)
4. [Multiple](#4-multiple)
5. [Priority](#5-priority)
6. [Advantages and disadvantages](#6-advantages-and-disadvantages)
7. [Suggestions](#7-suggestions)

# 1. Motivation

In `Rust` you are able to use Kafka consumers as `streams` (in-process, not Kafka streams) so you can use apis like `skip`, `take`, `filter`, etc using [StreamConsumer](https://docs.rs/rdkafka/0.28.0/rdkafka/consumer/stream_consumer/struct.StreamConsumer.html) for example. Unfortunatelly you can't treat Kafka consumer as a `stream` of data in C# - you have to use `Consume` in the while loop which is okay, but it would be great to have a syntatic sugar to use `IEnumerable<T>` (LINQ) extensions on a `IConsumer<TKey, TValue>` instance. This package allows you to do that. It provides a thin, almost allocation free wrapper around `IConsumer<TKey, TValue>` instance allowing you to use `foreach` on a consumer and LINQ as a result. It also provides ability to join multiple consumers in one stream in round-robin or prioritized manner. 

`KafkaEnumerable` is a thin wrapper, it does not enforce you to use specific deserializer and do not create or manage consumer instance for you. You are responsible for creating and disposing your consumer instances and also providing `Assigned` or `Unassigned` handlers which from my perspective is a huge benefit.

# 2. API

To transform consumer into `IEnumerable` use `KafkaEnumerable` static class with `Single` method, for example:

``` csharp

// Pseudocode

// Pretend that consumer is build here
var consumer = new ConsumerBuilder<byte[], byte[]>(...).Build(); 

consumer.Subscribe("test-topic1", "test-topic2");

// Create IEnumerable
var stream = KafkaEnumerables.Single(consumer);

foreach (var message in stream) {
   // Do stuff
}

// or

var batch = stream.Take(100).Where(m => m.HasData).Select(m => m.ConsumeResult).Reverse().ToList(); // Or any other query

// Do something with a batch :)

consumer.Close();
consumer.Dispose();

```

You can also wrap multiple consumers in a stream if you need to consume messages from multiple clusters:

``` csharp

// Pseudocode

// Pretend that consumers are created
var consumers = new[] {
   new ConsumerBuilder<byte[], byte[]>(...).Build(),
   new ConsumerBuilder<byte[], byte[]>(...).Build(),
   new ConsumerBuilder<byte[], byte[]>(...).Build()
};

// Create IEnumerable
var stream = KafkaEnumerables.Multiple(consumers);

foreach (var message in stream) {
   // Do stuff
}

```

And also you can consume messages in prioritized manner using special stream:

``` csharp

// Pseudocode

// Pretend that consumers are created
var consumers = new[] {
   new ConsumerBuilder<byte[], byte[]>(...).Build(),
   new ConsumerBuilder<byte[], byte[]>(...).Build(),
   new ConsumerBuilder<byte[], byte[]>(...).Build()
};

// Subscribe (omitted)

// Create IEnumerable
var stream = KafkaEnumerables.Priority(consumers);

foreach (var message in stream) {
   // Do stuff
}

```

Actually the code looks the same but the messages from the first consumer in array have greater than second and third, the same for second consumer - messages from second consumer have greater priority than messages from third consumer and so on.

Let's dive deeper in next sections dedicated for each consumer stream directly.

# 3. Single

`Single` stream API is the simplest of all. You just need to wrap a consumer and that's it. But there are some configuration options available:

1. `returnNulls` (`bool`, false by default) - set as `true` so `IEnumerable` would return nulls from Kafka consumer as `ConsumeResult<TKey, TValue>`. C# kafka client returns nulls from `.Consume()` method if there is no messages in the internal in-memory queue. It's a very important setting explained deeply later.

2. `consumeTimeout` (`TimeSpan`, 0 milliseconds by default) - used in `.Consume(int milliseconds)` API to poll message from Kafka. Actually 1 millisecond is a good choice by default but you can override it if you want (but better to leave it as is).

3. `cancellationToken` (`CancellationToken`) - used to `stop` an `Enumerable` instance :D (explained later)

`KafkaEnumerables.Single` returns `IEnumerable<SingleMessage<TKey, TValue>>` type. What is this `SingleMessage`? No worries, it is a `struct` wrapper around `ConsumeResult<TKey, TValue>` or `ConsumeException` - basically it may contain either consume result or an error. It was decided that it's better not to throw exceptions from a stream but rather return them:

``` csharp
// Some things are omitted

public readonly struct SingleMessage<TKey, TValue>
{
    public bool HasData { get; }
    public bool IsError { get; }

    public ConsumeException? Error { get; }
    public ConsumeResult<TKey, TValue>? ConsumeResult { get; }
}


```

Basically `SingleMessage` has a property to check if message contains a `ConsumeException` - `IsError`.

Additional property - `HasData` to check if `ConsumeResult<TKey, TValue>` returned from `IConsumer<TKey, TValue>` instance is null or has meaningful data, nulls can only be returned when `returnNulls` is set to true, otherwise - it's either something or an error :)


Let's try to create a simple stream:

``` csharp

// Create a consumer with auto commit but with manual store offset setting
var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
{
   GroupId = "test",
   EnableAutoCommit = true,
   EnableAutoOffsetStore = false,
   BootstrapServers = "localhost:9092"
}).Build();


consumer.Subscribe("test-topic1", "test-topic2");

// Create cancellation token source to stop after 30 seconds
var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

// Create a stream
// NOTE: we are passing cancellation token here
var stream = KafkaEnumerables.Single(consumer, consumeTimeout: TimeSpan.FromMilliseconds(1), returnNulls: false, cancellationToken: cts.Token);

// Start consuming from a stream
// Will throw OperationCancelledException eventually
foreach (var message in stream)
{
   // Check if error occurred during Consume() call internally
   if (message.IsError)
   {
         Console.WriteLine($"ConsumeException occurred: {message.Error}");
         continue;
   }

   if (!message.HasData) 
   {
      Console.WriteLine($"Consume result is null"); // Only when 
      continue;
   }
   
   // Message is returned
   Console.WriteLine($"Received ConsumeResult. TPO: {message.ConsumeResult.TopicPartitionOffset}");

   // Store offset if needed
   consumer.StoreOffset(message.ConsumeResult);

   // Commit
   consumer.Commit();
}

// Finally (or after catch)
consumer.Close();
consumer.Dispose(); // Dispose consumer

```

As you can see you can use any API you wish with your consumer instance. It's your responsibility to store offsets or to commit. Also you can build your consumer with custom repartitioning handlers - assigned/unassigned etc, library only uses `.Consume()` API internally. As it was mentioned before - it's a thin wrapper, it does not depend on a serializers/deserializers repartitioning logic or something else, the only thing you have to check is `SimpleMessage` and it's `IsError` or `HasData` properties.

You might wonder - why `CancellationToken` is needed? It is not used anywhere. Or is there some sort of background threading magic under the hood?

Well, good news - no threading and no internal buffers or such things. Under the hood, C# `yield` generator is used in while loop, so the code above is rewritten like this:


``` csharp

// Consumer creation is omitted

// Pseudocode (not the real code in the library but the principle behind is the same :) )

Func<IEnumerable<SingleMessage<TKey, TValue>>> generator = () => {
   while (true) {
      try {
         cancellationToken.ThrowIfCancellationRequested(); // Passed cancellation token
         var consumeResult = consumer.Consume(consumeTimeout); // Passed consumer timeout described above
         if (returnNulls || consumeResult != null) yield return new SingleMessage(consumeResult); // Simply yielding 
      }
      catch (ConsumeException ex) {
         yield return new SingleMessage(ex);
      }

   }
};

// Using created generator
foreach (var message in generator()) {
   // Code in a foreach loop from the example above :)
}


```

As you can see the while loop is effectively hidden from you like it does not exist at all. But something has to stop it. Of course you can also use `token` in the `foreach` loop:

``` csharp

// Omit consumer and stream creation:

foreach (var message in stream) {
   token.ThrowIfCancellationRequested(); 
   // or
   if (token.IsCancellationRequested) break;
}

```

But it won't work if `returnNulls` is not specified. If message is not returned from `.Consume` call - the code in the `foreach` call is not executed :) Basically `while (true)` above is blocking the thread and it's expected. To overcome it you should use `returnNulls: true` configuration setting so `while` propagates nulls and you have a chance to cancel a loop if needed.

Also `returnNulls` is important if you are using LINQ:

``` csharp

// Pseudocode

// Create a consumer with auto commit but with manual store offset setting
var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
{
   GroupId = "test",
   EnableAutoCommit = true,
   EnableAutoOffsetStore = false,
   BootstrapServers = "localhost:9092"
}).Build();

var stream = KafkaEnumerables.Single(consumer, returnNulls: false);

var batch = stream.Take(100).ToArray(); // Take 100 messages from a consumer
// or
var message = stream.First(); // Take a message at a time
var nextMessage = stream.First(); // Take next message

// The lines above will block the thread until 100 messages are returned or there is a single message available
// Sometimes it is not the desired behavior - we need to gather batches with at most 100 messages for example:

// Wrap consumer in another stream once more (it's completely safe)
var anotherStream = KafkaEnumerables.Single(consumer, returnNulls: true);

// This call does not block the thread indefinately because we also returning nulls from kafka (but the whole time depends on consumeTimeout property - better leave it as 1 millisecond)
// Saying something like: "Please return at most 100 messages, they can be nulls I will filter them out"
var upToBatch = anotherStream.Take(100).Where(m => m.HasData).ToArray();  // Can contain 30/1/99/59 - up to 100 messages

var maybeMessage = anotherStream.First(); // may have m.HasData = false or m.ConsumeResult = null

consumer.Close();
consumer.Dispose(); // Dispose consumer

```

Well, hopefully now you understand importance of configuration properties :)

As you can see - you are responsible for disposing/creating consumer and setting up all required handlers and deserializers. You can change configuration of stream on the fly by creating a new one whenever you want, for example on repartitioning, etc, the only thing left is thread safety - stream is not thread safe of course.


# 4. Multiple

Another type of a stream is called `Multiple` stream which allows you to `wrap` multiple consumers in one stream hence the name. By default `IConsumer<TKey, TValue>` can subscribe to multiple topics but sometimes we need to consume from different DCs or for some reason separate consumers consuming in different threads (creating multiple threads with `Single` streams) or one thread and consume in round-robin fashion - which `Multiple` stream is for.

Available options:

1. `returnNulls` (`bool`, false by default) - set as `true` so `IEnumerable` would return nulls from Kafka consumer as `ConsumeResult<TKey, TValue>`. C# kafka client returns nulls from `.Consume()` api if there is no messages in internal in-memory queue (see [Single](#3-single) section).

2. `consumeTimeout` (`TimeSpan`, 0 milliseconds by default) - used in `.Consume(int milliseconds)` API to poll message from Kafka. Actually 1 millisecond is a good choice by default but you can override it if you want.

3. `cancellationToken` (`CancellationToken`) - well, it does not require any explanation. It is used to `stop` an `Enumerable` instance :D (see [Single](#3-single) section)

4. `flushInterval` (`TimeSpan`, 5 seconds by default) - interval after which one message will be consumed from each consumer to allow rebalancing/assigning/unassigning and not to leave the group

5. `thresholds` (`int[]`, [10, 10, ..., 10] by default) - number of messages to consume from each consumer before continue with the next one (unless null is returned from `Consume` method so next consumer is chosen). As you can see by default 10 messages are greedily consumed for each consumer, but can be overriden of course, for example you can set [1, 1, 1, 1] so each consumer is allowed to consume only one message per iteration or [100, 10] to allow some consumers to consume more than others. This array should have the same length as a consumers array.

General algorithm:

1. Receive consumers and thresholds for each consumer and set `i = 0`
2. Flush is required -> flush (yield one message for each consumer) and jump to (3)
3. Try to consume `thresholds[i]` messages from current consumer
4. If received null or yielded `thresholds[i]` messages - reset threshold to specified (from 0 to 10, for example)
5. If can continue to the next consumer (`i++`) - jump to (2) if not - start from the first one (`i = 0`) once more and jump to (2)


As you can see this type of stream will try to consume from each consumer as much messages as you allow before moving to the next one :)

The API is the same as in [Single](#3-single) consumer, the only difference is number of options described above and a message, now `IEnumerable<MultiMessage<TKey, TValue>>` is returned:

``` csharp

public readonly struct MultiMessage<TKey, TValue>
{
   // IsFlush - to understand if the message is returned from the flush process (can be ignored)
    public bool IsFlush { get; } 
    // Consumer property is added so you know from which consumer this message is returned
    public IConsumer<TKey, TValue> Consumer { get; } 

   // Same properties as in SingleMessage<TKey, TValue>
    public bool HasData { get; }
    public bool IsError { get; }
    public KafkaException? Error { get; }
    public ConsumeResult<TKey, TValue>? { get; }
}

```

For example:

``` csharp

// Pseudocode:

// Create two consumers
var firstConsumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig {
   GroupId = "test",
   EnableAutoCommit = true,
   EnableAutoOffsetStore = false,
   BootstrapServers = "localhost:9092"
}).Build();

var secondConsumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig {
   GroupId = "test",
   EnableAutoCommit = true,
   EnableAutoOffsetStore = false,
   BootstrapServers = "localhost:9094"
}).Build();

firstConsumer.Subscribe("test-topic");
secondConsumer.Subscribe("test-topic-dc2");


var stream = KafkaEnumerables.Multiple(
   new[] { firstConsumer, secondConsumer },
   consumeTimeout: TimeSpan.FromMilliseconds(1), // Can be omitted
   flushInterval: TimeSpan.FromDays(1000), // To disable flush if needed (by setting a huge interval) (can be ommited)
   thresholds: new { 1000, 1000 }, // Thresholds (can be omitted)
   returnNulls: true, // Can be omitted
   cancellationToken: default); // Can be omitted


// The same:
foreach (var message in stream) {
   if (!message.HasData) continue; // Do nothing if null :D
   if (message.IsError) {
      Console.WriteLine($"Received error from consumer: {message.Consumer}. Error: {message.Error}");
      continue;
   }

   var consumeResult = message.ConsumeResult;
   // Do something with consume result


   // Using consumer property to gain access to `IConsumer` API 
   message.Consumer.StoreOffset(consumeResult); // Store offset if required
   message.Consumer.Commit(); // Commit if required
}


// Or using LINQ

var batch = stream.Take(100).Where(m => m.HasData).ToArray();

// Dispose consumers:
firstConsumer.Close();
firstConsumer.Dispose();
secondConsumer.Close();
secondConsumer.Dispose();

```

As you can see the API is the same but message has some additional properties. Also you are responsible for disposing/creating consumers and so on. The principle is the same - state machine is created under the hood that jumps between consumer instances for you :P


# 5. Priority

Another type of a stream is called `Priority` stream which allows you to `wrap` multiple consumers in one stream too but the messages will be returned in prioritized manner (not always guaranteed, but mostly yes :D). Configuration is the same as in `Multi` stream but consumers are aligned in priority order so messages will probably be consumed from the highest priorities first in greedy manner.

By default thresholds are configured like this `[-1, 10, 10, ..., 10]` where -1 means `Infinity` (so stream will return as much as possible messages from the first consumer before moving to others). 

Also there is one trick in place. Whenever it is decided to move to the next priority consumer all consumers with higher priorities are checked. If consumer with higher priority returned a message the priority is set to this consumer once more, this behavior can be described in such steps:


1. Receive consumers and thresholds for each consumer and set priority = 0
2. Flush is required -> flush (yield one message for each consumer) and jump to (3)
3. Try to consume `thresholds[priority]` messages from current consumer
4. If received null or yielded `thresholds[priority]` messages - reset threshold to specified (from 0 to 10, for example)
5. Update priority (`priority++`)
6. Check all priorities before current `for (var i = 0; i < priority; i++) { if (consumed(i)) priority = i; }` and reset priority to more prioritized if needed or left priority as is
7. If priority number is higher than number of consumers than set priority to 0 (`priority = 0`)
8. jump to 2

NOTE: `thresholds` setting for this type of stream specifies how many messages is allowed for current consumer to consume before checking consumers with higher priorities.

Stream also returns another type of message called `PriorityMessage<TKey, TValue>` which has one additional property compared to `MultiMessage<TKey, TValue>`:

```csharp

public readonly struct PriorityMessage<TKey, TValue>
{
   // To understand which priority is assigned to a message
   public int Priority { get; }

   // Same properties as in MultiMessage<TKey, TValue>
    public bool IsFlush { get; } 
    public IConsumer<TKey, TValue> Consumer { get; } 

   // Same properties as in SingleMessage<TKey, TValue>
    public bool HasData { get; }
    public bool IsError { get; }
    public KafkaException? Error { get; }
    public ConsumeResult<TKey, TValue>? { get; }
}

```

Example is pretty much the same as for multiple consumer:

``` csharp

// Pseudocode:

// Create two consumers
var firstConsumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig {
   GroupId = "test",
   EnableAutoCommit = true,
   EnableAutoOffsetStore = false,
   BootstrapServers = "localhost:9092"
}).Build();

var secondConsumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig {
   GroupId = "test",
   EnableAutoCommit = true,
   EnableAutoOffsetStore = false,
   BootstrapServers = "localhost:9094"
}).Build();

firstConsumer.Subscribe("priority-high"); // first consumer has higher priority
secondConsumer.Subscribe("priority-usual");


// The only difference is .Priority call :D
var stream = KafkaEnumerables.Priority(
   new[] { firstConsumer, secondConsumer },
   consumeTimeout: TimeSpan.FromMilliseconds(1), // Can be omitted
   flushInterval: TimeSpan.FromDays(1000), // To disable flush if needed (by setting a huge interval) (can be ommited)
   thresholds: new { 100, 100 }, // Thresholds (can be omitted)
   returnNulls: true, // Can be omitted
   cancellationToken: default); // Can be omitted

foreach (var message in stream) {
   Console.WriteLine($"Received message with priority {message.Priority}");

   if (!message.HasData) continue; // Do nothing if null :D

   if (message.IsError) {
      Console.WriteLine($"Received error from consumer: {message.Consumer}. Error: {message.Error}");
      continue;
   }

   var consumeResult = message.ConsumeResult;
   // Do something with consume result

   // Using consumer property to gain access to `IConsumer` API 
   message.Consumer.StoreOffset(consumeResult); // Store offset if required
   message.Consumer.Commit(); // Commit if required
}

// Or using LINQ

var batch = stream.Take(100).Where(m => m.HasData).ToArray();

// Dispose consumers:
firstConsumer.Close();
firstConsumer.Dispose();
secondConsumer.Close();
secondConsumer.Dispose();

```

# 6. Advantages and disadvantages

As I see this NuGet provides such advantages:

1. Consumer is now treated as an `IEnumerable<T>` stream so you can use LINQ
2. You can join multiple consumers in a single stream
3. You can consume messages in prioritized manner without writing custom code
4. All this with allocations in mind - only state machine is allocated and a wrapper around it. Returned messages are structs.
5. Simple API which hides complexity

Disadvantages:

1. Can't be used in multi-threaded manner (not sure why it is needed but still it's a case)
2. You don't have access to this source code in your code base
3. To use LINQ without blocking a thread `returnNulls: true` should be specified
4. Lots of configuration properties which could bring confusion
5. Supported from 1.8.0 version of `kafka-dotnet` library (please open an issue and I'll ad a separate branch with older versions)

# 7. Suggestions

Any ideas, suggestions and contributions are welcome :D
