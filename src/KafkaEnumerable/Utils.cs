using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace KafkaEnumerable;

// Wraps enumerable and caches enumerator so generators do no instantiate new enumerator each time
// For example when .Take(100) is called on IEnumerable<T>
internal sealed class EnumerableOwner<T> : IEnumerable<T>
{
    IEnumerator<T>? _enumerator;
    readonly IEnumerable<T> _enumerable;

    [DebuggerStepThrough]
    public EnumerableOwner(IEnumerable<T> enumerable) => _enumerable = enumerable;

    [DebuggerStepThrough, MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IEnumerator<T> GetEnumerator() => _enumerator ??= _enumerable.GetEnumerator();

    [DebuggerStepThrough, MethodImpl(MethodImplOptions.AggressiveInlining)]
    IEnumerator IEnumerable.GetEnumerator() => _enumerator ??= _enumerable.GetEnumerator();
}

internal static class EnumerableOwner
{
    [DebuggerStepThrough, MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static EnumerableOwner<T> Wrap<T>(IEnumerable<T> enumerable) => new(enumerable);
}