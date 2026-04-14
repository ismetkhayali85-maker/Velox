namespace Velox.Sql.Tests.ClickHouse;

public class ArchitectureTests : TestBase
{
    [Fact]
    public void Select_ThenInsert_MaintainsCleanSlate()
    {
        // 1. Start with a Select builder and add some state
        var factory = VeloxRuntime.ClickHouse<TestEntity>();
        factory.Where(x => x.Id == 1);
        
        // 2. Switch to Insert - it should return a NEW clean builder
        var insertBuilder = factory.Insert(new TestEntity { Id = 2, Name = "New" });
        
        // 3. Verify original factory still has its WHERE, but Insert builder is clean (no WHERE from select)
        var selectSql = factory.ToDebugSql();
        var insertSql = insertBuilder.ToDebugSql();
        
        Assert.Contains("WHERE \"test_table\".\"id\" = 1", selectSql);
        Assert.Equal("INSERT INTO \"test_table\" (\"id\",\"name\") VALUES (2, 'New');", insertSql);
        
        // They must be different instances to ensure isolation
        Assert.NotSame(factory, insertBuilder);
    }

    [Fact]
    public void WhereClause_FluentAction_Showcase()
    {
        var builder = VeloxRuntime.ClickHouse<TestEntity>();
        
        // Using the new WhereClause via Action
        builder.Where(where => 
        {
            where.SetValue(x => x.Id, Velox.Sql.Core.Impl.Operators.Equal, 1);
            where.And().In(x => x.Name, new[] { "A", "B" });
        });
        
        var sql = builder.ToDebugSql();
        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\", \"test_table\".\"name\" AS \"Name\" FROM \"test_table\" WHERE \"test_table\".\"id\" = 1 AND \"test_table\".\"name\" IN ('A','B');", sql);
    }

    [Fact]
    public void Select_WithGroupByAndHaving_Example()
    {
        // 1. Initial setup of a Select query with aggregation
        var builder = VeloxRuntime.ClickHouse<TestEntity>();
        
        // 2. Add aggregation, grouping and Having using the new architecture
        builder.Select(x => x.Id)
               .GroupBy(x => x.Id)
               .Having(having => 
               {
                   having.Count(x => x.Id, Velox.Sql.Core.Impl.Operators.GreaterThan, 5);
                   having.And().Sum(x => x.Id, Velox.Sql.Core.Impl.Operators.LessThan, 100);
               });
        
        var sql = builder.ToDebugSql();
        
        // 3. Verify it produces a valid SELECT ... FROM ... GROUP BY ... HAVING ... chain
        Assert.Contains("SELECT \"test_table\".\"id\" AS \"Id\"", sql);
        Assert.Contains("GROUP BY \"test_table\".\"id\"", sql);
        Assert.Contains("HAVING count(\"test_table\".\"id\") > 5 AND sum(\"test_table\".\"id\") < 100", sql);
    }
}
