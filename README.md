# Velox.Sql

Velox.Sql is a powerful, high-performance SQL builder for .NET. It lets you write complex queries using a fluent, type-safe API, specifically optimized for **PostgreSQL** and **ClickHouse**.

The main goal is to generate clean, parameterized SQL without being tied to any specific database execution engine.

### Why Velox.Sql?

*   **Pure SQL Generation**: Built to produce SQL strings and parameters, leaving the execution to you.
*   **Type Safe**: Uses C# Expressions to map your models to table columns safely.
*   **Optimized Dialects**: Specialized support for advanced PostgreSQL and ClickHouse functions.
*   **Core stays lean**: The main `Velox.Sql` package has no DI dependency; optional integration ships as a separate NuGet package.

---

### It's simple.

1.  **Entity**: Your plain C# model.
2.  **Mapper**: Define how properties map to columns.

```csharp
public class User {
    public int Id { get; set; }
    public string Email { get; set; }
}

public class UserMapper : Mapper<User> {
    public UserMapper() {
        Table("users");
        Map(x => x.Id).Column("user_id");
        Map(x => x.Email).Column("email");
        Build();
    }
}
```

3.  **Config**: Register mappings — see [Mapper configuration](#mapper-configuration) below (manual list, discovery, or DI).

4.  **Builder**: Obtain `IVeloxSql` (see [Mapper configuration](#mapper-configuration)), then build queries.

```csharp
var builder = sql.Postgres<User>()
    .Select(x => x.Id)
    .Where(x => x.Email.Contains("@gmail.com"))
    .Where(w => w.Between(x => x.Id, 1, 100));
```

#### Production Ready (Parametrized)
```csharp
var query = builder.ToSql();
// SQL: SELECT ... WHERE "email" LIKE @p0 AND "user_id" BETWEEN @p1 AND @p2
// Parameters: { "@p0": "%@gmail.com%", "@p1": 1, "@p2": 100 }
```

#### Debug & Logging (One-line)
```csharp
var debugSql = builder.ToDebugSql();
// SQL: SELECT ... WHERE "email" LIKE '%@gmail.com%' AND "user_id" BETWEEN 1 AND 100
```

---

### Mapper configuration

Mark mapper classes with `[VeloxSqlMapper(SqlEngine.PostgreSQL)]`, `[VeloxSqlMapper(SqlEngine.ClickHouse)]`, or combine flags for both engines. `IClassMapper` is unchanged — only metadata on the class.

#### 1. Manual (explicit configs)

Explicit registration; no reflection. Build `PgSqlConfiguration` / `ClickHouseSqlConfiguration`, then `VeloxSql`.

```csharp
using Velox.Sql;
using Velox.Sql.Impl;
using Velox.Sql.Impl.Map;

var pg = new PgSqlConfiguration(new List<IClassMapper> { new UserMapper() });
var ch = new ClickHouseSqlConfiguration(new List<IClassMapper> { new RawEventsMapper() });
IVeloxSql sql = new VeloxSql(pg, ch);
```

#### 2. Discovery (attributes + scan)

Scan assemblies for attributed mappers, then create `IVeloxSql` (or use the configs separately).

```csharp
using Velox.Sql;
using Velox.Sql.Registration;

// var result = VeloxSqlMapperDiscovery.DiscoverTypes(typeof(UserMapper), typeof(RawEventsMapper));
var result = VeloxSqlMapperDiscovery.Discover(typeof(UserMapper).Assembly);

IVeloxSql sql = result.CreateVeloxSql();
// Or: new VeloxSql(result) — same as CreateVeloxSql()
```

#### 3. Dependency Injection (separate NuGet package)

Install **`Velox.Sql.DependencyInjection`**. It registers `PgSqlConfiguration`, `ClickHouseSqlConfiguration`, `VeloxSqlDiscoveryResult`, and `IVeloxSql` as singletons.

```csharp
using Microsoft.Extensions.DependencyInjection;
using Velox.Sql;
using Velox.Sql.DependencyInjection;

services.AddVeloxSql(typeof(UserMapper).Assembly);

// var sql = serviceProvider.GetRequiredService<IVeloxSql>();
// sql.Postgres<User>()
```

Use `AddVeloxSql` with an optional callback if you need to adjust the assembly list after the fact.

---

### NuGet packages

| Package | Purpose |
|--------|---------|
| `Velox.Sql.Core` | Shared primitives |
| `Velox.Sql` | SQL builder, mappers, discovery |
| `Velox.Sql.DependencyInjection` | `AddVeloxSql` for `Microsoft.Extensions.DependencyInjection` |

---

### Examples & Documentation

*   [PostgreSQL Examples](Velox.Sql.Tests/Postgres/)
*   [ClickHouse Examples](Velox.Sql.Tests/ClickHouse/)
*   [Mapper discovery & DI examples](Velox.Sql.Tests/Registration/) — configuration styles demonstrated in tests

### License

Velox.Sql is licensed under the MIT License.
