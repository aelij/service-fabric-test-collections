using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Data.Notifications;

namespace ServiceFabric.TestCollections
{
    internal sealed class TestReliableDictionary<TKey, TValue> : IReliableDictionary<TKey, TValue> where TKey : IComparable<TKey>, IEquatable<TKey>
    {
        private readonly ConcurrentDictionary<TKey, TValue> _internalDictionary = new ConcurrentDictionary<TKey, TValue>();
        private readonly Dictionary<TKey, TransactionInformation> _pendingTransactions = new Dictionary<TKey, TransactionInformation>();
        private readonly object _syncLock = new object();
        private readonly TimeSpan _defaultTimeout;

        public TestReliableDictionary() : this(TimeSpan.FromSeconds(10))
        {
        }

        public TestReliableDictionary(TimeSpan defaultTimeout)
        {
            _defaultTimeout = defaultTimeout;
        }

        public Uri Name { get; } = new Uri("urn:ReliableDictionary");

        public Func<IReliableDictionary<TKey, TValue>, NotifyDictionaryRebuildEventArgs<TKey, TValue>, Task> RebuildNotificationAsyncCallback
        {
            set { }
        }

        public event EventHandler<NotifyDictionaryChangedEventArgs<TKey, TValue>> DictionaryChanged;

        public Task AddAsync(ITransaction tx, TKey key, TValue value)
        {
            return AddAsync(tx, key, value, _defaultTimeout, CancellationToken.None);
        }

        public async Task AddAsync(ITransaction tx, TKey key, TValue value, TimeSpan timeout, CancellationToken cancellationToken)
        {
            await AddUpdateOrRemoveCore(tx, key, false, timeout).ConfigureAwait(false);
            if (!_internalDictionary.TryAdd(key, value))
            {
                throw new ArgumentException("Key already exists", nameof(key));
            }
        }

        public Task<TValue> AddOrUpdateAsync(ITransaction tx, TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory)
        {
            return AddOrUpdateAsync(tx, key, addValue, updateValueFactory, _defaultTimeout, CancellationToken.None);
        }

        public Task<TValue> AddOrUpdateAsync(ITransaction tx, TKey key, Func<TKey, TValue> addValueFactory, Func<TKey, TValue, TValue> updateValueFactory)
        {
            return AddOrUpdateAsync(tx, key, addValueFactory, updateValueFactory, _defaultTimeout, CancellationToken.None);
        }

        public async Task<TValue> AddOrUpdateAsync(ITransaction tx, TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory, TimeSpan timeout, CancellationToken cancellationToken)
        {
            await AddUpdateOrRemoveCore(tx, key, false, timeout).ConfigureAwait(false);
            return _internalDictionary.AddOrUpdate(key, addValue, updateValueFactory);
        }

        public async Task<TValue> AddOrUpdateAsync(ITransaction tx, TKey key, Func<TKey, TValue> addValueFactory, Func<TKey, TValue, TValue> updateValueFactory, TimeSpan timeout, CancellationToken cancellationToken)
        {
            await AddUpdateOrRemoveCore(tx, key, false, timeout).ConfigureAwait(false);
            return _internalDictionary.AddOrUpdate(key, addValueFactory, updateValueFactory);
        }

        public Task ClearAsync()
        {
            return ClearAsync(_defaultTimeout, CancellationToken.None);
        }

        public Task ClearAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            lock (_syncLock)
            {
                foreach (var transactionInformation in _pendingTransactions.Values)
                {
                    transactionInformation.Cancel();
                }

                _internalDictionary.Clear();
                _pendingTransactions.Clear();
            }

            OnDictionaryChanged(new NotifyDictionaryClearEventArgs<TKey, TValue>(default(long)));

            return SpecialTasks.True;
        }

        public Task<bool> ContainsKeyAsync(ITransaction tx, TKey key)
        {
            return ContainsKeyAsync(tx, key, LockMode.Default, _defaultTimeout, CancellationToken.None);
        }

        public Task<bool> ContainsKeyAsync(ITransaction tx, TKey key, LockMode lockMode)
        {
            return ContainsKeyAsync(tx, key, lockMode, _defaultTimeout, CancellationToken.None);
        }

        public Task<bool> ContainsKeyAsync(ITransaction tx, TKey key, TimeSpan timeout, CancellationToken cancellationToken)
        {
            return ContainsKeyAsync(tx, key, LockMode.Default, timeout, CancellationToken.None);
        }

        public async Task<bool> ContainsKeyAsync(ITransaction tx, TKey key, LockMode lockMode, TimeSpan timeout, CancellationToken cancellationToken)
        {
            var value = await GetCore(tx, key, lockMode, timeout).ConfigureAwait(false);
            return value.HasValue;
        }

        public Task<IAsyncEnumerable<KeyValuePair<TKey, TValue>>> CreateEnumerableAsync(ITransaction txn)
        {
            return CreateEnumerableAsync(txn, null, EnumerationMode.Unordered);
        }

        public Task<IAsyncEnumerable<KeyValuePair<TKey, TValue>>> CreateEnumerableAsync(ITransaction txn, EnumerationMode enumerationMode)
        {
            return CreateEnumerableAsync(txn, null, enumerationMode);
        }

        public Task<IAsyncEnumerable<KeyValuePair<TKey, TValue>>> CreateEnumerableAsync(ITransaction txn, Func<TKey, bool> filter, EnumerationMode enumerationMode)
        {
            var enumerable = CreateEnumerable(txn, filter, enumerationMode);
            return Task.FromResult(enumerable);
        }

        internal IAsyncEnumerable<KeyValuePair<TKey, TValue>> CreateEnumerable(ITransaction txn, Func<TKey, bool> filter, EnumerationMode enumerationMode)
        {
            var dictionary = GetSnapshot(txn);
            var enumeration = dictionary.AsEnumerable();
            if (filter != null)
            {
                enumeration = enumeration.Where(kv => filter(kv.Key));
            }

            if (enumerationMode == EnumerationMode.Ordered)
            {
                enumeration = enumeration.OrderBy(x => x.Key);
            }

            var enumerable = enumeration.AsFabricAsyncEnumerable();
            return enumerable;
        }

        internal Dictionary<TKey, TValue> GetSnapshot(ITransaction txn)
        {
            var dictionary = new Dictionary<TKey, TValue>();
            lock (_syncLock)
            {
                foreach (var current in _internalDictionary)
                {
                    if (!_pendingTransactions.TryGetValue(current.Key, out var transactionInformation) ||
                        transactionInformation.TransactionId != txn.TransactionId)
                    {
                        dictionary[current.Key] = current.Value;
                    }
                    else if (transactionInformation.Exists)
                    {
                        dictionary[current.Key] = transactionInformation.OriginalValue;
                    }
                }
            }

            return dictionary;
        }

        public Task<long> GetCountAsync(ITransaction tx)
        {
            return Task.FromResult<long>(GetSnapshot(tx).Count);
        }

        public Task<TValue> GetOrAddAsync(ITransaction tx, TKey key, TValue value)
        {
            return GetOrAddAsync(tx, key, value, _defaultTimeout, CancellationToken.None);
        }

        public Task<TValue> GetOrAddAsync(ITransaction tx, TKey key, Func<TKey, TValue> valueFactory)
        {
            return GetOrAddAsync(tx, key, valueFactory, _defaultTimeout, CancellationToken.None);
        }

        public async Task<TValue> GetOrAddAsync(ITransaction tx, TKey key, TValue value, TimeSpan timeout, CancellationToken cancellationToken)
        {
            await AddUpdateOrRemoveCore(tx, key, false, timeout).ConfigureAwait(false);
            return _internalDictionary.GetOrAdd(key, value);
        }

        public async Task<TValue> GetOrAddAsync(ITransaction tx, TKey key, Func<TKey, TValue> valueFactory, TimeSpan timeout, CancellationToken cancellationToken)
        {
            await AddUpdateOrRemoveCore(tx, key, false, timeout).ConfigureAwait(false);
            return _internalDictionary.GetOrAdd(key, valueFactory);
        }

        public Task SetAsync(ITransaction tx, TKey key, TValue value)
        {
            return SetAsync(tx, key, value, _defaultTimeout, CancellationToken.None);
        }

        public async Task SetAsync(ITransaction tx, TKey key, TValue value, TimeSpan timeout, CancellationToken cancellationToken)
        {
            await AddUpdateOrRemoveCore(tx, key, false, timeout).ConfigureAwait(false);
            _internalDictionary[key] = value;
        }

        public Task<bool> TryAddAsync(ITransaction tx, TKey key, TValue value)
        {
            return TryAddAsync(tx, key, value, _defaultTimeout, CancellationToken.None);
        }

        public async Task<bool> TryAddAsync(ITransaction tx, TKey key, TValue value, TimeSpan timeout, CancellationToken cancellationToken)
        {
            await AddUpdateOrRemoveCore(tx, key, false, timeout).ConfigureAwait(false);
            return _internalDictionary.TryAdd(key, value);
        }

        public Task<ConditionalValue<TValue>> TryGetValueAsync(ITransaction tx, TKey key)
        {
            return TryGetValueAsync(tx, key, LockMode.Default, _defaultTimeout, CancellationToken.None);
        }

        public Task<ConditionalValue<TValue>> TryGetValueAsync(ITransaction tx, TKey key, LockMode lockMode)
        {
            return TryGetValueAsync(tx, key, lockMode, _defaultTimeout, CancellationToken.None);
        }

        public Task<ConditionalValue<TValue>> TryGetValueAsync(ITransaction tx, TKey key, TimeSpan timeout, CancellationToken cancellationToken)
        {
            return TryGetValueAsync(tx, key, LockMode.Default, timeout, cancellationToken);
        }

        public Task<ConditionalValue<TValue>> TryGetValueAsync(ITransaction tx, TKey key, LockMode lockMode, TimeSpan timeout, CancellationToken cancellationToken)
        {
            return GetCore(tx, key, lockMode, timeout);
        }

        public Task<ConditionalValue<TValue>> TryRemoveAsync(ITransaction tx, TKey key)
        {
            return TryRemoveAsync(tx, key, _defaultTimeout, CancellationToken.None);
        }

        public async Task<ConditionalValue<TValue>> TryRemoveAsync(ITransaction tx, TKey key, TimeSpan timeout, CancellationToken cancellationToken)
        {
            await AddUpdateOrRemoveCore(tx, key, true, timeout).ConfigureAwait(false);
            var success = _internalDictionary.TryRemove(key, out var removed);
            return new ConditionalValue<TValue>(success, removed);
        }

        public Task<bool> TryUpdateAsync(ITransaction tx, TKey key, TValue newValue, TValue comparisonValue)
        {
            return TryUpdateAsync(tx, key, newValue, comparisonValue, _defaultTimeout, CancellationToken.None);
        }

        public async Task<bool> TryUpdateAsync(ITransaction tx, TKey key, TValue newValue, TValue comparisonValue, TimeSpan timeout, CancellationToken cancellationToken)
        {
            await AddUpdateOrRemoveCore(tx, key, false, timeout).ConfigureAwait(false);
            return _internalDictionary.TryUpdate(key, newValue, comparisonValue);
        }

        private void OnDictionaryChanged(NotifyDictionaryChangedEventArgs<TKey, TValue> e)
        {
            DictionaryChanged?.Invoke(this, e);
        }

        private Task<ConditionalValue<TValue>> GetCore(ITransaction txn, TKey key, LockMode lockMode, TimeSpan timeout)
        {
            var id = txn?.TransactionId ?? 0;
            Task<ConditionalValue<TValue>> task;
            lock (_syncLock)
            {
                if (_pendingTransactions.TryGetValue(key, out var transactionInformation) && transactionInformation.TransactionId != id)
                {
                    task = transactionInformation.WaitAsync(timeout);
                }
                else
                {
                    var hasValue = _internalDictionary.TryGetValue(key, out var value);
                    if (lockMode == LockMode.Update)
                    {
                        _pendingTransactions[key] = new TransactionInformation(txn, key, value, !hasValue, this);
                    }
                    task = Task.FromResult(new ConditionalValue<TValue>(hasValue, value));
                }
            }

            return task;
        }

        private async Task AddUpdateOrRemoveCore(ITransaction txn, TKey key, bool remove, TimeSpan timeout, Task antecedent = null)
        {
            if (antecedent != null)
            {
                await antecedent.ConfigureAwait(false);
            }

            Task task = null;
            lock (_syncLock)
            {
                var exists = _internalDictionary.TryGetValue(key, out var previousValue);
                if (!remove || exists)
                {
                    if (_pendingTransactions.TryGetValue(key, out var transactionInformation))
                    {
                        if (transactionInformation.TransactionId != txn.TransactionId)
                        {
                            task = AddUpdateOrRemoveCore(txn, key, remove, timeout, transactionInformation.WaitAsync(timeout));
                        }
                    }
                    else
                    {
                        _pendingTransactions[key] = new TransactionInformation(txn, key, previousValue, exists, this);
                    }
                }
            }

            if (task != null)
            {
                await task.ConfigureAwait(false);
            }
        }

        private sealed class TransactionalWaiter
        {
            private readonly TaskCompletionSource<ConditionalValue<TValue>> _taskCompletionSource;
            private readonly CancellationTokenSource _cancellationTokenSource;

            public TransactionalWaiter()
            {
                _taskCompletionSource = new TaskCompletionSource<ConditionalValue<TValue>>();
                _cancellationTokenSource = new CancellationTokenSource();
                _cancellationTokenSource.Token.Register(() => _taskCompletionSource.TrySetException(new TimeoutException()));
            }

            public void Cancel()
            {
                _taskCompletionSource.TrySetCanceled();
            }

            public void SetTimeout(TimeSpan timeout)
            {
                _cancellationTokenSource.CancelAfter(timeout);
            }

            public void Signal(bool hasValue, TValue value)
            {
                _taskCompletionSource.TrySetResult(new ConditionalValue<TValue>(hasValue, value));
            }

            public Task<ConditionalValue<TValue>> Task => _taskCompletionSource.Task;
        }

        private sealed class TransactionInformation
        {
            private readonly TKey _key;
            private readonly TestReliableDictionary<TKey, TValue> _owner;
            private readonly TestTransaction _transaction;
            private readonly object _lock;

            private Queue<TransactionalWaiter> _waiters;
            private bool _completed;

            public bool Exists { get; }

            public TValue OriginalValue { get; }

            public long TransactionId => _transaction.TransactionId;

            public TransactionInformation(ITransaction transaction, TKey key, TValue originalValue, bool exists, TestReliableDictionary<TKey, TValue> owner)
            {
                _lock = new object();
                _key = key;
                _owner = owner;
                Exists = exists;
                OriginalValue = originalValue;
                _transaction = transaction as TestTransaction;
                if (_transaction == null)
                {
                    throw new InvalidOperationException("Incompatible transaction");
                }

                RegisterTransaction();
            }

            private void RegisterTransaction()
            {
                lock (_lock)
                {
                    _transaction.Completed += OnCompleted;

                    if (!_completed)
                    {
                        if (_transaction.Result != null)
                        {
                            _completed = true;
                            OnCompletedNoCheck(_transaction.Result.Value);
                        }
                    }
                }
            }

            private void OnCompleted(bool committed)
            {
                lock (_lock)
                {
                    if (_completed) return;
                }

                OnCompletedNoCheck(committed);
            }

            private void OnCompletedNoCheck(bool committed)
            {
                bool existsAfterTx;
                TValue valueToSignal;
                NotifyDictionaryChangedEventArgs<TKey, TValue> changeArgs = null;
                lock (_owner._syncLock)
                {
                    _owner._pendingTransactions.Remove(_key);
                    if (!committed)
                    {
                        if (Exists)
                        {
                            _owner._internalDictionary[_key] = OriginalValue;
                        }
                        else
                        {
                            _owner._internalDictionary.TryRemove(_key, out _);
                        }

                        existsAfterTx = _owner._internalDictionary.TryGetValue(_key, out valueToSignal);
                    }
                    else
                    {
                        existsAfterTx = _owner._internalDictionary.TryGetValue(_key, out valueToSignal);

                        if (Exists)
                        {
                            changeArgs = existsAfterTx
                                ? (NotifyDictionaryChangedEventArgs<TKey, TValue>)
                                  new NotifyDictionaryItemUpdatedEventArgs<TKey, TValue>(_transaction, _key, valueToSignal)
                                : new NotifyDictionaryItemRemovedEventArgs<TKey, TValue>(_transaction, _key);
                        }
                        else
                        {
                            changeArgs = new NotifyDictionaryItemAddedEventArgs<TKey, TValue>(_transaction, _key, valueToSignal);
                        }
                    }
                }

                while (_waiters?.Count > 0)
                {
                    var waiter = _waiters.Dequeue();
                    Task.Run(() => waiter.Signal(existsAfterTx, valueToSignal));
                }

                if (changeArgs != null)
                {
                    _owner.OnDictionaryChanged(changeArgs);
                }
            }

            public Task<ConditionalValue<TValue>> WaitAsync(TimeSpan timeout)
            {
                var transactionalAsyncWaiter = new TransactionalWaiter();
                if (_waiters == null)
                {
                    _waiters = new Queue<TransactionalWaiter>();
                }

                _waiters.Enqueue(transactionalAsyncWaiter);
                transactionalAsyncWaiter.SetTimeout(timeout);
                return transactionalAsyncWaiter.Task;
            }

            public void Cancel()
            {
                while (_waiters?.Count > 0)
                {
                    var state = _waiters.Dequeue();
                    Task.Run(() => state.Cancel());
                }
            }
        }
    }
}