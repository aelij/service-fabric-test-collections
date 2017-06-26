using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ServiceFabric.TestCollections.Tests
{
    [SuppressMessage("ReSharper", "ConsiderUsingConfigureAwait")]
    [TestClass]
    public class TestReliableQueueTests
    {
        private readonly TestReliableQueue<int> _queue = new TestReliableQueue<int>(TimeSpan.FromSeconds(1));

        [TestMethod]
        public async Task SingleValueWithinTransaction()
        {
            using (var tx = new TestTransaction())
            {
                await _queue.EnqueueAsync(tx, 1);
                var value = await _queue.TryDequeueAsync(tx);
                Assert.AreEqual(1, value.Value);
            }
        }

        [TestMethod]
        public async Task SingleValueTransactionAborted()
        {
            using (var tx = new TestTransaction())
            {
                await _queue.EnqueueAsync(tx, 1);
            }

            using (var tx = new TestTransaction())
            {
                var value = await _queue.TryDequeueAsync(tx);
                Assert.IsFalse(value.HasValue);
            }
        }

        [TestMethod]
        public async Task SingleValueTransactionComitted()
        {
            using (var tx = new TestTransaction())
            {
                await _queue.EnqueueAsync(tx, 1);
                await tx.CommitAsync();
            }

            using (var tx = new TestTransaction())
            {
                var value = await _queue.TryDequeueAsync(tx);
                Assert.AreEqual(1, value.Value);
            }
        }

        [TestMethod]
        public async Task TwoConcurrentTransactions()
        {
            using (var tx1 = new TestTransaction())
            using (var tx2 = new TestTransaction())
            {
                await _queue.EnqueueAsync(tx1, 1);
                var task = _queue.EnqueueAsync(tx2, 2);
                await tx1.CommitAsync();
                await task;
                await tx2.CommitAsync();
            }

            using (var tx = new TestTransaction())
            {
                var item = await _queue.TryDequeueAsync(tx);
                Assert.AreEqual(1, item.Value);
                item = await _queue.TryDequeueAsync(tx);
                Assert.AreEqual(2, item.Value);
            }
        }

        [TestMethod]
        public async Task TwoConcurrentTransactionsTimeout()
        {
            using (var tx1 = new TestTransaction())
            using (var tx2 = new TestTransaction())
            {
                await _queue.EnqueueAsync(tx1, 1);
                await Assert.ThrowsExceptionAsync<TimeoutException>(async () =>
                    // ReSharper disable once AccessToDisposedClosure
                    await _queue.EnqueueAsync(tx2, 2));
            }
        }

        [TestMethod]
        public async Task SingleValueRemoveAbort()
        {
            using (var tx = new TestTransaction())
            {
                await _queue.EnqueueAsync(tx, 1);
                await tx.CommitAsync();
            }

            using (var tx = new TestTransaction())
            {
                await _queue.TryDequeueAsync(tx);
            }

            using (var tx = new TestTransaction())
            {
                var value = await _queue.TryDequeueAsync(tx);
                Assert.AreEqual(1, value.Value);
            }
        }

        [TestMethod]
        public async Task StressTest()
        {
            // this test adds the integers min..max to the queue
            // then, in multiple threads (ratio * max tasks),
            // creates transactions that (in the given ratio)
            // either enqueues or dequeues

            const int min = 1;
            const int max = 1000;
            const int ratio = 10;

            var rng = new ThreadLocal<Random>(() => new Random());

            using (var tx = new TestTransaction())
            {
                for (var i = min; i <= max; i++)
                {
                    await _queue.EnqueueAsync(tx, i);
                }

                await tx.CommitAsync();
            }

            var added = 0;

            var tasks = Enumerable.Range(1, max * ratio).Select(async x =>
            {
                using (var tx = new TestTransaction())
                {
                    var key = x / ratio;
                    if (rng.Value.Next(ratio) == 0)
                    {
                        await _queue.TryDequeueAsync(tx);
                        Interlocked.Decrement(ref added);
                    }
                    else
                    {
                        await _queue.EnqueueAsync(tx, key);
                        Interlocked.Increment(ref added);
                    }

                    await tx.CommitAsync();
                }
            }).ToArray();

            await Task.WhenAll(tasks);

            using (var tx = new TestTransaction())
            {
                var count = await _queue.GetCountAsync(tx);
                Assert.AreEqual(max + added, count);
            }
        }
    }
}