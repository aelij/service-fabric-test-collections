using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;

namespace ServiceFabric.TestCollections
{
    internal sealed class TestReliableQueue<T> : IReliableQueue<T>
    {
        private readonly Queue<TaskCompletionSource<object>> _waiters = new Queue<TaskCompletionSource<object>>();
        private readonly object _syncLock = new object();
        private readonly TimeSpan _defaultTimeout;

        private ConcurrentQueue<T> _internalQueue = new ConcurrentQueue<T>();
        private TestTransaction _currentTransaction;
        private ConcurrentQueue<T> _txQueue;
        private bool _txCompleted;

        public TestReliableQueue() : this(TimeSpan.FromSeconds(10))
        {
        }

        public TestReliableQueue(TimeSpan defaultTimeout)
        {
            _defaultTimeout = defaultTimeout;
        }

        public Uri Name { get; } = new Uri("urn:ReliableQueue");

        public Task<long> GetCountAsync(ITransaction tx)
        {
            lock (_syncLock)
            {
                return Task.FromResult<long>(GetQueue(tx).Count);
            }
        }

        public Task ClearAsync()
        {
            lock (_syncLock)
            {
                while (_waiters.Count > 0)
                {
                    var waiter = _waiters.Dequeue();
                    Task.Run(() => waiter.TrySetCanceled());
                }
                while (_internalQueue.TryDequeue(out _)) { }
            }
            return SpecialTasks.True;
        }

        public Task EnqueueAsync(ITransaction tx, T item)
        {
            return EnqueueAsync(tx, item, _defaultTimeout, CancellationToken.None);
        }

        public async Task EnqueueAsync(ITransaction tx, T item, TimeSpan timeout, CancellationToken cancellationToken)
        {
            await WaitAsync(tx, timeout).ConfigureAwait(false);
            _txQueue.Enqueue(item);
        }

        public Task<ConditionalValue<T>> TryDequeueAsync(ITransaction tx)
        {
            return TryDequeueAsync(tx, _defaultTimeout, CancellationToken.None);
        }

        public async Task<ConditionalValue<T>> TryDequeueAsync(ITransaction tx, TimeSpan timeout, CancellationToken cancellationToken)
        {
            await WaitAsync(tx, timeout).ConfigureAwait(false);
            var hasValue = _txQueue.TryDequeue(out var value);
            return new ConditionalValue<T>(hasValue, value);
        }

        public Task<ConditionalValue<T>> TryPeekAsync(ITransaction tx)
        {
            return TryPeekAsync(tx, LockMode.Default, _defaultTimeout, CancellationToken.None);
        }

        public Task<ConditionalValue<T>> TryPeekAsync(ITransaction tx, TimeSpan timeout, CancellationToken cancellationToken)
        {
            return TryPeekAsync(tx, LockMode.Default, timeout, cancellationToken);
        }

        public Task<ConditionalValue<T>> TryPeekAsync(ITransaction tx, LockMode lockMode)
        {
            return TryPeekAsync(tx, lockMode, _defaultTimeout, CancellationToken.None);
        }

        public async Task<ConditionalValue<T>> TryPeekAsync(ITransaction tx, LockMode lockMode, TimeSpan timeout, CancellationToken cancellationToken)
        {
            await WaitAsync(tx, timeout).ConfigureAwait(false);
            var hasValue = _txQueue.TryPeek(out var value);
            return new ConditionalValue<T>(hasValue, value);
        }

        public Task<IAsyncEnumerable<T>> CreateEnumerableAsync(ITransaction tx)
        {
            lock (_syncLock)
            {
                var queue = GetQueue(tx);
                return Task.FromResult(queue.ToArray().AsFabricAsyncEnumerable());
            }
        }

        private ConcurrentQueue<T> GetQueue(ITransaction tx)
        {
            return _currentTransaction?.TransactionId == tx.TransactionId ? _txQueue : _internalQueue;
        }

        private async Task WaitAsync(ITransaction tx, TimeSpan timeout, Task antecedent = null)
        {
            if (antecedent != null)
            {
                await antecedent.ConfigureAwait(false);
            }

            lock (_syncLock)
            {
                if (_currentTransaction == null)
                {
                    _txCompleted = false;
                    _currentTransaction = tx as TestTransaction;
                    if (_currentTransaction == null)
                    {
                        throw new InvalidOperationException("Incompatible transaction");
                    }

                    _txQueue = new ConcurrentQueue<T>(_internalQueue);
                    _currentTransaction.Completed += OnCompleted;
                    if (_currentTransaction.Result != null)
                    {
                        _txCompleted = true;
                        OnCompletedNoCheck(_currentTransaction.Result.Value);
                    }
                    return;
                }

                if (_currentTransaction.TransactionId == tx.TransactionId)
                {
                    return;
                }

                var tcs = new TaskCompletionSource<object>();
                var cts = new CancellationTokenSource();
                cts.Token.Register(() => tcs.TrySetException(new TimeoutException()));
                _waiters.Enqueue(tcs);
                antecedent = tcs.Task;
                cts.CancelAfter(timeout);
            }

            await WaitAsync(tx, timeout, antecedent).ConfigureAwait(false);
        }

        private void OnCompleted(bool committed)
        {
            lock (_syncLock)
            {
                if (_txCompleted) return;

                OnCompletedNoCheck(committed);
            }
        }

        private void OnCompletedNoCheck(bool committed)
        {
            lock (_syncLock)
            {
                if (committed)
                {
                    _internalQueue = _txQueue;
                }
                while (_waiters.Count > 0)
                {
                    var waiter = _waiters.Dequeue();
                    Task.Run(() => waiter.TrySetResult(null));
                }
                _waiters.Clear();
                _currentTransaction = null;
            }
        }
    }
}