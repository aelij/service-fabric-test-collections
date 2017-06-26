using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data;

namespace ServiceFabric.TestCollections
{
    internal static class EnumerableExtensions
    {
        internal static IAsyncEnumerable<T> AsFabricAsyncEnumerable<T>(this IEnumerable<T> enumerable)
        {
            return new FabricEnumerableWrapper<T>(enumerable);
        }

        private sealed class FabricEnumerableWrapper<T> : IAsyncEnumerable<T>
        {
            private readonly IEnumerable<T> _inner;

            public FabricEnumerableWrapper(IEnumerable<T> inner)
            {
                _inner = inner;
            }

            public IAsyncEnumerator<T> GetAsyncEnumerator()
            {
                return new AsyncEnumeratorWrapper(_inner.GetEnumerator());
            }

            private class AsyncEnumeratorWrapper : IAsyncEnumerator<T>
            {
                private readonly IEnumerator<T> _inner;

                public AsyncEnumeratorWrapper(IEnumerator<T> inner)
                {
                    _inner = inner;
                }

                public void Dispose() => _inner.Dispose();

                public Task<bool> MoveNextAsync(CancellationToken cancellationToken) =>
                    _inner.MoveNext() ? SpecialTasks.True : SpecialTasks.False;

                public void Reset() => _inner.Reset();

                public T Current => _inner.Current;
            }
        }
    }
}