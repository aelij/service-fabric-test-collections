using System.Threading.Tasks;

namespace ServiceFabric.TestCollections
{
    internal static class SpecialTasks
    {
        public static Task<bool> False { get; } = Task.FromResult(false);
        public static Task<bool> True { get; } = Task.FromResult(true);
        public static Task<long> ZeroInt64 { get; } = Task.FromResult(0L);
    }
}