using Velox.Sql.Core.Impl;

#pragma warning disable CS0618

namespace Velox.Sql.Tests.Postgres;

public class AdvancedQueryTests : TestBase
{
    [Fact]
    public void FluentHaving_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Select(x => x.Id)
            .From<PostgresTestEntity>()
            .GroupBy(x => x.Id)
            .Having(h => h.Count(x => x.Id, Operators.GreaterThan, 1))
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\" FROM \"pg_table\" GROUP BY \"pg_table\".\"id\" HAVING count(\"pg_table\".\"id\") > 1;", sql);
    }


    [Fact]
    public void Union_ReturnsCorrectSql()
    {
        var query1 = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Select(x => x.Id);
        var query2 = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Select(x => x.Id);
        
        var sql = query1.Union(query2)
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\" FROM \"pg_table\" UNION SELECT \"pg_table\".\"id\" AS \"Id\" FROM \"pg_table\";", sql);
    }

    [Fact]
    public void UnionAll_ReturnsCorrectSql()
    {
        var query1 = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Select(x => x.Id);
        var query2 = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Select(x => x.Id);
        
        var sql = query1.UnionAll(query2)
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\" FROM \"pg_table\" UNION ALL SELECT \"pg_table\".\"id\" AS \"Id\" FROM \"pg_table\";", sql);
    }

    [Fact]
    public void Intersect_ReturnsCorrectSql()
    {
        var query1 = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Select(x => x.Id);
        var query2 = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Select(x => x.Id);
        
        var sql = query1.Intersect(query2)
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\" FROM \"pg_table\" INTERSECT SELECT \"pg_table\".\"id\" AS \"Id\" FROM \"pg_table\";", sql);
    }

    [Fact]
    public void Except_ReturnsCorrectSql()
    {
        var query1 = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Select(x => x.Id);
        var query2 = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Select(x => x.Id);
        
        var sql = query1.Except(query2)
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\" FROM \"pg_table\" EXCEPT SELECT \"pg_table\".\"id\" AS \"Id\" FROM \"pg_table\";", sql);
    }

    [Fact]
    public void SubQuery_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Select(x => x.Id)
            .SubQuery<PostgresTestEntity>(sub => sub.Select(s => s.Description).Where(w => w.Id == 1))
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\", (SELECT \"pg_table\".\"description\" AS \"Description\" FROM \"pg_table\" WHERE \"pg_table\".\"id\" = 1)  FROM \"pg_table\";", sql);
    }

    [Fact]
    public void ComplexJoins_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Select(x => x.Id)
            .FullJoin<PostgresTestEntity, PostgresTestEntity>(
                x => x.Id, 
                y => y.Id,
                (x, y) => new { x.Description })
            .ToDebugSql();

        Assert.Contains("FULL JOIN", sql);
    }

    [Fact]
    public void Returning_ReturnsCorrectSql()
    {
        var entity = new PostgresTestEntity { Id = 1, Description = "Test" };
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Insert(entity)
            .Returning(x => x.Id)
            .ToDebugSql();

        Assert.Equal("INSERT INTO \"pg_table\" (\"id\", \"description\") VALUES(1, 'Test') RETURNING \"pg_table\".\"id\" AS \"Id\";", sql);
    }
}
