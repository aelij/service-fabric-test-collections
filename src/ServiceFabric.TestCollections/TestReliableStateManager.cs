using System;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Data.Notifications;

namespace ServiceFabric.TestCollections
{
    public sealed class TestReliableStateManager : IReliableStateManagerReplica
    {
        private readonly TestReliableDictionary<string, IReliableState> _states;
        private readonly TimeSpan _defaultTimeout = TimeSpan.FromSeconds(10);

        public TestReliableStateManager()
        {
            _states = new TestReliableDictionary<string, IReliableState>();
            _states.DictionaryChanged += OnStatesOnDictionaryChanged;
        }

        private void OnStatesOnDictionaryChanged(object sender, NotifyDictionaryChangedEventArgs<string, IReliableState> dictionaryChangedEventArgs)
        {
            NotifyStateManagerSingleEntityChangedEventArgs args = null;
            switch (dictionaryChangedEventArgs)
            {
                case NotifyDictionaryItemAddedEventArgs<string, IReliableState> added:
                    args = new NotifyStateManagerSingleEntityChangedEventArgs(added.Transaction, added.Value, NotifyStateManagerChangedAction.Add);
                    break;
                case NotifyDictionaryItemRemovedEventArgs<string, IReliableState> removed:
                    args = new NotifyStateManagerSingleEntityChangedEventArgs(removed.Transaction, null, NotifyStateManagerChangedAction.Remove);
                    break;
            }

            if (args != null)
            {
                OnStateManagerChanged(args);
            }
        }

        public IAsyncEnumerator<IReliableState> GetAsyncEnumerator()
        {
            using (var tx = CreateTransaction())
            {
                return _states.GetSnapshot(tx).Select(x => x.Value).AsFabricAsyncEnumerable().GetAsyncEnumerator();
            }
        }

        public bool TryAddStateSerializer<T>(IStateSerializer<T> stateSerializer)
        {
            return false;
        }

        public ITransaction CreateTransaction()
        {
            var transaction = new TestTransaction();

            void OnTransactionOnCompleted(bool completed)
            {
                transaction.Completed -= OnTransactionOnCompleted;

                if (completed)
                {
                    OnTransactionChanged(new NotifyTransactionChangedEventArgs(transaction, NotifyTransactionChangedAction.Commit));
                }
            }

            transaction.Completed += OnTransactionOnCompleted;

            return transaction;
        }

        public async Task<T> GetOrAddAsync<T>(ITransaction tx, Uri name, TimeSpan timeout) where T : IReliableState
        {
            var value = await _states.GetOrAddAsync(tx, name.ToString(), _ => Create(typeof(T)), timeout, CancellationToken.None).ConfigureAwait(false);
            return (T)value;
        }

        public Task<T> GetOrAddAsync<T>(ITransaction tx, Uri name) where T : IReliableState
        {
            return GetOrAddAsync<T>(tx, name, _defaultTimeout);
        }

        public async Task<T> GetOrAddAsync<T>(Uri name, TimeSpan timeout) where T : IReliableState
        {
            using (var tx = CreateTransaction())
            {
                var result = await GetOrAddAsync<T>(tx, name, timeout).ConfigureAwait(false);
                await tx.CommitAsync().ConfigureAwait(false);
                return result;
            }
        }

        public Task<T> GetOrAddAsync<T>(Uri name) where T : IReliableState
        {
            return GetOrAddAsync<T>(name, _defaultTimeout);
        }

        public Task<T> GetOrAddAsync<T>(ITransaction tx, string name, TimeSpan timeout) where T : IReliableState
        {
            return GetOrAddAsync<T>(GetUriName(name), timeout);
        }

        public Task<T> GetOrAddAsync<T>(ITransaction tx, string name) where T : IReliableState
        {
            return GetOrAddAsync<T>(GetUriName(name));
        }

        public Task<T> GetOrAddAsync<T>(string name, TimeSpan timeout) where T : IReliableState
        {
            return GetOrAddAsync<T>(GetUriName(name), timeout);
        }

        public Task<T> GetOrAddAsync<T>(string name) where T : IReliableState
        {
            return GetOrAddAsync<T>(GetUriName(name));
        }

        public async Task RemoveAsync(ITransaction tx, Uri name, TimeSpan timeout)
        {
            await _states.TryRemoveAsync(tx, name.ToString(), timeout, CancellationToken.None).ConfigureAwait(false);
        }

        public Task RemoveAsync(ITransaction tx, Uri name)
        {
            return RemoveAsync(tx, name, _defaultTimeout);
        }

        public async Task RemoveAsync(Uri name, TimeSpan timeout)
        {
            using (var tx = CreateTransaction())
            {
                await RemoveAsync(tx, name, timeout).ConfigureAwait(false);
                await tx.CommitAsync().ConfigureAwait(false);
            }
        }

        public Task RemoveAsync(Uri name)
        {
            return RemoveAsync(name, _defaultTimeout);
        }

        public Task RemoveAsync(ITransaction tx, string name, TimeSpan timeout)
        {
            return RemoveAsync(tx, GetUriName(name), timeout);
        }

        public Task RemoveAsync(ITransaction tx, string name)
        {
            return RemoveAsync(tx, GetUriName(name), _defaultTimeout);
        }

        public Task RemoveAsync(string name, TimeSpan timeout)
        {
            return RemoveAsync(GetUriName(name), timeout);
        }

        public Task RemoveAsync(string name)
        {
            return RemoveAsync(GetUriName(name));
        }

        public async Task<ConditionalValue<T>> TryGetAsync<T>(Uri name) where T : IReliableState
        {
            using (var tx = CreateTransaction())
            {
                var value = await _states.TryGetValueAsync(tx, name.ToString()).ConfigureAwait(false);
                await tx.CommitAsync().ConfigureAwait(false);
                return new ConditionalValue<T>(value.HasValue, (T)value.Value);
            }
        }

        public Task<ConditionalValue<T>> TryGetAsync<T>(string name) where T : IReliableState
        {
            return TryGetAsync<T>(GetUriName(name));
        }

        public event EventHandler<NotifyTransactionChangedEventArgs> TransactionChanged;

        public event EventHandler<NotifyStateManagerChangedEventArgs> StateManagerChanged;

        private void OnTransactionChanged(NotifyTransactionChangedEventArgs e)
        {
            TransactionChanged?.Invoke(this, e);
        }

        private void OnStateManagerChanged(NotifyStateManagerChangedEventArgs e)
        {
            StateManagerChanged?.Invoke(this, e);
        }

        private static Uri GetUriName(string name)
        {
            return new Uri("urn:" + Uri.EscapeDataString(name));
        }

        private IReliableState Create(Type type)
        {
            if (type.IsGenericType)
            {
                var genericType = type.GetGenericTypeDefinition();
                if (genericType == typeof(IReliableDictionary<,>))
                {
                    // ReSharper disable once PossibleNullReferenceException
                    return (IReliableState)typeof(TestReliableDictionary<,>).MakeGenericType(type.GetGenericArguments())
                        .GetConstructor(Type.EmptyTypes).Invoke(null);
                }

                if (genericType == typeof(IReliableQueue<>))
                {
                    // ReSharper disable once PossibleNullReferenceException
                    return (IReliableState)typeof(TestReliableQueue<>).MakeGenericType(type.GetGenericArguments())
                        .GetConstructor(Type.EmptyTypes).Invoke(null);
                }
            }

            throw new ArgumentException("Unrecognized type", nameof(type));
        }

        #region IStateProviderReplica

        void IStateProviderReplica.Initialize(StatefulServiceInitializationParameters initializationParameters)
        {
        }

        Task<IReplicator> IStateProviderReplica.OpenAsync(ReplicaOpenMode openMode, IStatefulServicePartition partition,
            CancellationToken cancellationToken)
        {
            return Task.FromResult<IReplicator>(null);
        }

        Task IStateProviderReplica.ChangeRoleAsync(ReplicaRole newRole, CancellationToken cancellationToken)
        {
            return SpecialTasks.True;
        }

        Task IStateProviderReplica.CloseAsync(CancellationToken cancellationToken)
        {
            return SpecialTasks.True;
        }

        void IStateProviderReplica.Abort()
        {
        }

        Task IStateProviderReplica.BackupAsync(Func<BackupInfo, CancellationToken, Task<bool>> backupCallback)
        {
            return SpecialTasks.True;
        }

        Task IStateProviderReplica.BackupAsync(BackupOption option, TimeSpan timeout, CancellationToken cancellationToken,
            Func<BackupInfo, CancellationToken, Task<bool>> backupCallback)
        {
            return SpecialTasks.True;
        }

        Task IStateProviderReplica.RestoreAsync(string backupFolderPath)
        {
            return SpecialTasks.True;
        }

        Task IStateProviderReplica.RestoreAsync(string backupFolderPath, RestorePolicy restorePolicy, CancellationToken cancellationToken)
        {
            return SpecialTasks.True;
        }

        Func<CancellationToken, Task<bool>> IStateProviderReplica.OnDataLossAsync
        {
            set { }
        }

        #endregion
    }
}