﻿using System;

namespace KafkaEnumerable
{
    internal static class Defaults
    {
        public static readonly int Infinity = -1;
        public static readonly bool ReturnNulls = false;
        public static readonly int MessagesThreshold = 10;
        public static readonly TimeSpan FlushInterval = TimeSpan.FromSeconds(5);
        public static readonly TimeSpan ConsumeTimeout = TimeSpan.FromMilliseconds(1);
    }
}
