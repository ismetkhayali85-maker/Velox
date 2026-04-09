using System;
using System.Collections.Generic;
using Velox.Sql;
using Velox.Sql.Impl;
using Velox.Sql.Interfaces;
using Xunit;

namespace Velox.Sql.Tests.Postgres;

public class ArchitectureTests : TestBase
{
    [Fact]
    public void Select_ThenUpdate_MaintainsCleanSlate()
    {
        // 1. Start with a Select builder and add some state
        var factory = VeloxRuntime.Postgres<PostgresTestEntity>();
        factory.Where(x => x.Id == 1);
        
        // 2. Switch to Update - it should return a NEW clean builder
        var updateBuilder = factory.Update(new PostgresTestEntity { Id = 1, Description = "Updated" }, x => x.Id == 1);
        
        // 3. Verify Select builder still has its WHERE, but Update builder is independent
        var selectSql = factory.ToDebugSql();
        var updateSql = updateBuilder.ToDebugSql();
        
        Assert.Contains("WHERE \"pg_table\".\"id\" = 1", selectSql);
        Assert.Equal("UPDATE \"pg_table\" SET \"id\" = 1, \"description\" = 'Updated' WHERE \"pg_table\".\"id\" = 1;", updateSql);
        
        Assert.NotSame(factory, updateBuilder);
    }

    [Fact]
    public void WhereClause_FluentAction_Showcase()
    {
        var builder = VeloxRuntime.Postgres<PostgresTestEntity>();
        
        // Using the new WhereClause via Action
        builder.Where(where => 
        {
            where.IsNotNull(x => x.Description);
            where.Or().SetValue(x => x.Id, Velox.Sql.Core.Impl.Operators.GreaterThan, 100);
        });
        
        var sql = builder.ToDebugSql();
        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\", \"pg_table\".\"description\" AS \"Description\" FROM \"pg_table\" WHERE \"pg_table\".\"description\" IS NOT NULL OR \"pg_table\".\"id\" > 100;", sql);
    }

    [Fact]
    public void Select_WithGroupByAndHaving_Example()
    {
        // 1. Initial setup
        var builder = VeloxRuntime.Postgres<PostgresTestEntity>();
        
        // 2. Complex chain with Having
        builder.Select(x => x.Id)
               .GroupBy(x => x.Id)
               .Having(having => 
               {
                   having.Sum(x => x.Id, Velox.Sql.Core.Impl.Operators.LessThan, 1000);
               });
        
        var sql = builder.ToDebugSql();
        
        // 3. Verification
        Assert.Contains("FROM \"pg_table\"", sql);
        Assert.Contains("GROUP BY \"pg_table\".\"id\"", sql);
        Assert.Contains("HAVING sum(\"pg_table\".\"id\") < 1000", sql);
    }
}
