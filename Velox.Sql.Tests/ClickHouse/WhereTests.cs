#pragma warning disable CS0618

namespace Velox.Sql.Tests.ClickHouse;

public class WhereTests : TestBase
{
    [Fact]
    public void Where_Simple_ReturnsCorrectSql()
    {
        var sql = DbQuery<TestEntity>.GetClickHouseBuilder()
            .Where(x => x.Id == 1)
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\", \"test_table\".\"name\" AS \"Name\" FROM \"test_table\" WHERE \"test_table\".\"id\" = 1;", sql);
    }

    [Fact]
    public void WhereAnd_ReturnsCorrectSql()
    {
        var sql = DbQuery<TestEntity>.GetClickHouseBuilder()
            .Where(x => x.Id == 1 && x.Name == "test")
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\", \"test_table\".\"name\" AS \"Name\" FROM \"test_table\" WHERE \"test_table\".\"id\" = 1 AND \"test_table\".\"name\" = 'test';", sql);
    }

    [Fact]
    public void WhereOr_ReturnsCorrectSql()
    {
        var sql = DbQuery<TestEntity>.GetClickHouseBuilder()
            .Where(x => x.Id == 1 || x.Name == "test")
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\", \"test_table\".\"name\" AS \"Name\" FROM \"test_table\" WHERE \"test_table\".\"id\" = 1 OR \"test_table\".\"name\" = 'test';", sql);
    }

    [Fact]
    public void Where_EscapeSingleQuote_ReturnsCorrectSql()
    {
        var sql = DbQuery<TestEntity>.GetClickHouseBuilder()
            .Where(x => x.Name == "O'Brien")
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\", \"test_table\".\"name\" AS \"Name\" FROM \"test_table\" WHERE \"test_table\".\"name\" = 'O\\'Brien';", sql);
    }

    [Fact]
    public void Where_EscapeBackslash_ReturnsCorrectSql()
    {
        var sql = DbQuery<TestEntity>.GetClickHouseBuilder()
            .Where(x => x.Name == @"C:\Users\Admin")
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\", \"test_table\".\"name\" AS \"Name\" FROM \"test_table\" WHERE \"test_table\".\"name\" = 'C:\\\\Users\\\\Admin';", sql);
    }

    [Fact]
    public void Where_In_EscapeSpecialChars_ReturnsCorrectSql()
    {
        var sql = DbQuery<TestEntity>.GetClickHouseBuilder()
            .Where(w => w.In(x => x.Name, new[] { "A'B", @"C\D" }))
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\", \"test_table\".\"name\" AS \"Name\" FROM \"test_table\" WHERE \"test_table\".\"name\" IN ('A\\'B','C\\\\D');", sql);
    }

    [Fact]
    public void Where_Security_InjectionPrevention_ReturnsCorrectSql()
    {
        var sql = DbQuery<TestEntity>.GetClickHouseBuilder()
            .Where(x => x.Name == "'; DROP TABLE test_table; --")
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\", \"test_table\".\"name\" AS \"Name\" FROM \"test_table\" WHERE \"test_table\".\"name\" = '\\'; DROP TABLE test_table; --';", sql);
    }

    [Fact]
    public void Where_WithBetween_ReturnsCorrectSql()
    {
        var sql = DbQuery<TestEntity>.GetClickHouseBuilder()
            .Where(w => w.Between(x => x.Id, 1, 100))
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\", \"test_table\".\"name\" AS \"Name\" FROM \"test_table\" WHERE \"test_table\".\"id\" BETWEEN 1 AND 100;", sql);
    }

    [Fact]
    public void Where_WithILike_ReturnsCorrectSql()
    {
        var sql = DbQuery<TestEntity>.GetClickHouseBuilder()
            .Where(w => w.ILike(x => x.Name, "test%"))
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\", \"test_table\".\"name\" AS \"Name\" FROM \"test_table\" WHERE \"test_table\".\"name\" ILIKE 'test%';", sql);
    }

    [Fact]
    public void Where_WithLike_ReturnsCorrectSql()
    {
        var sql = DbQuery<TestEntity>.GetClickHouseBuilder()
            .Where(w => w.Like(x => x.Name, "test%"))
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\", \"test_table\".\"name\" AS \"Name\" FROM \"test_table\" WHERE \"test_table\".\"name\" LIKE 'test%';", sql);
    }

    [Fact]
    public void Where_WithAnyAllSome_ReturnsCorrectSql()
    {
        var subquery = DbQuery<TestEntity>.GetClickHouseBuilder().Select(x => x.Id);

        var sqlAny = DbQuery<TestEntity>.GetClickHouseBuilder().Where(w => w.Any(x => x.Id, Velox.Sql.Core.Impl.Operators.Equal, subquery)).ToDebugSql();
        var sqlAll = DbQuery<TestEntity>.GetClickHouseBuilder().Where(w => w.All(x => x.Id, Velox.Sql.Core.Impl.Operators.GreaterThan, subquery)).ToDebugSql();
        var sqlSome = DbQuery<TestEntity>.GetClickHouseBuilder().Where(w => w.Some(x => x.Id, Velox.Sql.Core.Impl.Operators.NotEqual, subquery)).ToDebugSql();

        Assert.Contains("WHERE \"test_table\".\"id\" = ANY (SELECT \"test_table\".\"id\"", sqlAny);
        Assert.Contains("WHERE \"test_table\".\"id\" > ALL (SELECT \"test_table\".\"id\"", sqlAll);
        Assert.Contains("WHERE \"test_table\".\"id\" <> SOME (SELECT \"test_table\".\"id\"", sqlSome);
    }

    [Fact]
    public void Where_Security_ClassicInjection_ReturnsEscapedSql()
    {
        var sql = DbQuery<TestEntity>.GetClickHouseBuilder()
            .Where(x => x.Name == "1' OR '1'='1")
            .ToDebugSql();

        Assert.Equal("SELECT \"test_table\".\"id\" AS \"Id\", \"test_table\".\"name\" AS \"Name\" FROM \"test_table\" WHERE \"test_table\".\"name\" = '1\\' OR \\'1\\'=\\'1';", sql);
    }
}
