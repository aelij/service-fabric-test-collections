using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Data;

namespace ServiceFabric.TestCollections
{
    internal sealed class TestTransaction : ITransaction
    {
        private static int _id;

        public void Dispose()
        {
            if (Result == null)
            {
                Abort();
            }
        }

        public Task CommitAsync()
        {
            Result = true;
            Completed?.Invoke(true);
            return SpecialTasks.True;
        }

        public void Abort()
        {
            Result = false;
            Completed?.Invoke(false);
        }

        public Task<long> GetVisibilitySequenceNumberAsync()
        {
            return SpecialTasks.ZeroInt64;
        }

        // ReSharper disable once UnassignedGetOnlyAutoProperty
        public long CommitSequenceNumber { get; }

        public long TransactionId { get; } = Interlocked.Increment(ref _id);

        internal event Action<bool> Completed;

        internal bool? Result { get; private set; }
    }
}