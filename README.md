# Service Fabric Test Collections

Test implementations of Service Fabric Reliable Collections.

Implements IReliableStateManagerReplica, ITransaction, IReliableDictionary, and IReliableQueue.

The collections **maintin the same semantics*** as Service Fabric, including transaction commits and aborts, concurrency, and timeouts.

# NuGet

Package versions correspond to the Service Fabric SDK (major and minor).

```powershell
Install-Package ServiceFabric.TestCollections
```

# Usage

```csharp

class MyService : StatefulService
{
	public MyService(IReliableStateManagerReplica stateManager) : base(stateManager)  { }
}

using ServiceFabric.TestCollections;

class MyServiceTest
{
	public async Task TestService()
	{
		var service =  new MyService(new TestReliableStateManager());
		await service.UpdateSomething();
	}
}

```