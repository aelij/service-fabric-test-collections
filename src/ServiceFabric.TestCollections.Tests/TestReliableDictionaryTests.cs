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
    public class TestReliableDictionaryTests
    {
        private readonly TestReliableDictionary<int, int> _dictionary = new TestReliableDictionary<int, int>(TimeSpan.FromSeconds(1));

        [TestMethod]
        public async Task SingleValueWithinTransaction()
        {
            using (var tx = new TestTransaction())
            {
                await _dictionary.AddAsync(tx, 1, 1);
                var value = await _dictionary.TryGetValueAsync(tx, 1);
                Assert.AreEqual(1, value.Value);
            }
        }

        [TestMethod]
        public async Task SingleValueTransactionAborted()
        {
            using (var tx = new TestTransaction())
            {
                await _dictionary.AddAsync(tx, 1, 1);
            }

            using (var tx = new TestTransaction())
            {
                var value = await _dictionary.TryGetValueAsync(tx, 1);
                Assert.IsFalse(value.HasValue);
            }
        }

        [TestMethod]
        public async Task SingleValueTransactionComitted()
        {
            using (var tx = new TestTransaction())
            {
                await _dictionary.AddAsync(tx, 1, 1);
                await tx.CommitAsync();
            }

            using (var tx = new TestTransaction())
            {
                var value = await _dictionary.TryGetValueAsync(tx, 1);
                Assert.AreEqual(1, value.Value);
            }
        }

        [TestMethod]
        public async Task SameKeyTwoTransactionsTimeout()
        {
            using (var tx1 = new TestTransaction())
            using (var tx2 = new TestTransaction())
            {
                await _dictionary.AddAsync(tx1, 1, 1);
                await Assert.ThrowsExceptionAsync<TimeoutException>(async () =>
                    // ReSharper disable once AccessToDisposedClosure
                    await _dictionary.AddAsync(tx2, 1, 1));
            }
        }

        [TestMethod]
        public async Task DifferentKeysTwoTransactionsConcurrent()
        {
            using (var tx1 = new TestTransaction())
            using (var tx2 = new TestTransaction())
            {
                await _dictionary.AddAsync(tx1, 1, 1);
                await _dictionary.AddAsync(tx2, 2, 2);

                var value1 = await _dictionary.TryGetValueAsync(tx1, 1);
                Assert.AreEqual(1, value1.Value);
                var value2 = await _dictionary.TryGetValueAsync(tx2, 2);
                Assert.AreEqual(2, value2.Value);
            }
        }

        [TestMethod]
        public async Task SingleValueRemoveAbort()
        {
            using (var tx = new TestTransaction())
            {
                await _dictionary.AddAsync(tx, 1, 1);
                await tx.CommitAsync();
            }

            using (var tx = new TestTransaction())
            {
                await _dictionary.TryRemoveAsync(tx, 1);
            }

            using (var tx = new TestTransaction())
            {
                var value = await _dictionary.TryGetValueAsync(tx, 1);
                Assert.AreEqual(1, value.Value);
            }
        }

        [TestMethod]
        public async Task StressTest()
        {
            // this test adds the integers min..max to the dictionary
            // then, in multiple threads (ratio * max tasks),
            // creates transactions that (in the given ratio)
            // either updates the value to its negative or removes it

            const int min = 1;
            const int max = 10000;
            const int ratio = 10;

            var rng = new ThreadLocal<Random>(() => new Random());

            using (var tx = new TestTransaction())
            {
                for (var i = min; i <= max; i++)
                {
                    await _dictionary.AddAsync(tx, i, i);
                }

                await tx.CommitAsync();
            }

            var tasks = Enumerable.Range(1, max * ratio).OrderBy(x => rng.Value.Next()).Select(async x =>
            {
                using (var tx = new TestTransaction())
                {
                    var key = x / ratio;
                    if (rng.Value.Next(ratio) == 0)
                    {
                        await _dictionary.TryRemoveAsync(tx, key);
                    }
                    else
                    {
                        await _dictionary.TryUpdateAsync(tx, key, -key, key);
                    }

                    await tx.CommitAsync();
                }
            }).ToArray();

            await Task.WhenAll(tasks);

            using (var tx = new TestTransaction())
            {
                var enumerable = await _dictionary.CreateEnumerableAsync(tx);
                using (var enumerator = enumerable.GetAsyncEnumerator())
                {
                    while (await enumerator.MoveNextAsync(CancellationToken.None))
                    {
                        var x = enumerator.Current.Value;
                        Assert.IsTrue(x >= -max);
                        Assert.IsTrue(x <= -min);
                    }
                }
            }
        }
    }
}