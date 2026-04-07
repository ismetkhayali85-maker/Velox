# Velox.Sql

Velox.Sql is a powerful, high-performance SQL builder for .NET. It lets you write complex queries using a fluent, type-safe API, specifically optimized for **PostgreSQL** and **ClickHouse**.

The main goal is to generate clean, parameterized SQL without being tied to any specific database execution engine.

### Why Velox.Sql?

*   **Pure SQL Generation**: Built to produce SQL strings and parameters, leaving the execution to you.
*   **Type Safe**: Uses C# Expressions to map your models to table columns safely.
*   **Optimized Dialects**: Specialized support for advanced PostgreSQL and ClickHouse functions.
*   **Zero Dependencies**: Minimal footprint, maximum performance.

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

3.  **Config**: Register mappings (PostgreSql or ClickHouse).

```csharp
DbQuery.DefaultPostgresConfig = new PgSqlConfiguration(new List<IClassMapper> { new UserMapper() });
```

4.  **Builder**: Build your queries anywhere.

```csharp
var builder = DbQuery<User>.GetPostgresBuilder()
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

### Examples & Documentation

*   [PostgreSQL Examples](file:///C:/Users/user/Documents/lib/Velox.Sql.Tests/Postgres/)
*   [ClickHouse Examples](file:///C:/Users/user/Documents/lib/Velox.Sql.Tests/ClickHouse/)

### License

Velox.Sql is licensed under the MIT License.
