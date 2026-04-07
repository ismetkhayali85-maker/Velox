#pragma warning disable CS0618

namespace Velox.Sql.Tests.Postgres;

public class WhereTests : TestBase
{
    [Fact]
    public void Where_WithExists_ReturnsCorrectSql()
    {
        var subquery = DbQuery<PostgresJoinEntity>.GetPostgresBuilder()
            .Select(x => x.Id)
            .Where(x => x.ParentId == 1);

        var sql = DbQuery<PostgresTestEntity>.GetPostgresBuilder()
            .Select(x => x.Id)
            .From<PostgresTestEntity>()
            .Where(w => w.Exists(subquery))
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\" FROM \"pg_table\" WHERE EXISTS (SELECT \"pg_join_table\".\"id\" AS \"Id\" FROM \"pg_join_table\" WHERE \"pg_join_table\".\"parent_id\" = 1);", sql);
    }

    [Fact]
    public void Where_WithNotExists_ReturnsCorrectSql()
    {
        var subquery = DbQuery<PostgresJoinEntity>.GetPostgresBuilder()
            .Select(x => x.Id)
            .Where(x => x.ParentId == 1);

        var sql = DbQuery<PostgresTestEntity>.GetPostgresBuilder()
            .Select(x => x.Id)
            .From<PostgresTestEntity>()
            .Where(w => w.NotExists(subquery))
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\" FROM \"pg_table\" WHERE NOT EXISTS (SELECT \"pg_join_table\".\"id\" AS \"Id\" FROM \"pg_join_table\" WHERE \"pg_join_table\".\"parent_id\" = 1);", sql);
    }

    [Fact]
    public void Where_WithInSubquery_ReturnsCorrectSql()
    {
        var subquery = DbQuery<PostgresJoinEntity>.GetPostgresBuilder()
            .Select(x => x.ParentId);

        var sql = DbQuery<PostgresTestEntity>.GetPostgresBuilder()
            .Select(x => x.Id)
            .From<PostgresTestEntity>()
            .Where(w => w.In(x => x.Id, subquery))
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\" FROM \"pg_table\" WHERE \"pg_table\".\"id\" IN (SELECT \"pg_join_table\".\"parent_id\" AS \"ParentId\" FROM \"pg_join_table\");", sql);
    }

    [Fact]
    public void Where_WithBetween_ReturnsCorrectSql()
    {
        var sql = DbQuery<PostgresTestEntity>.GetPostgresBuilder()
            .Where(w => w.Between(x => x.Id, 1, 10))
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\", \"pg_table\".\"description\" AS \"Description\" FROM \"pg_table\" WHERE \"pg_table\".\"id\" BETWEEN 1 AND 10;", sql);
    }

    [Fact]
    public void Where_WithILike_ReturnsCorrectSql()
    {
        var sql = DbQuery<PostgresTestEntity>.GetPostgresBuilder()
            .Where(w => w.ILike(x => x.Description, "test%"))
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\", \"pg_table\".\"description\" AS \"Description\" FROM \"pg_table\" WHERE \"pg_table\".\"description\"::text ILIKE 'test%';", sql);
    }

    [Fact]
    public void Where_WithLike_ReturnsCorrectSql()
    {
        var sql = DbQuery<PostgresTestEntity>.GetPostgresBuilder()
            .Where(w => w.Like(x => x.Description, "test%"))
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\", \"pg_table\".\"description\" AS \"Description\" FROM \"pg_table\" WHERE \"pg_table\".\"description\" LIKE 'test%';", sql);
    }

    [Fact]
    public void Where_WithAnyAllSome_ReturnsCorrectSql()
    {
        var subquery = DbQuery<PostgresJoinEntity>.GetPostgresBuilder().Select(x => x.Id);

        var sqlAny = DbQuery<PostgresTestEntity>.GetPostgresBuilder().Where(w => w.Any(x => x.Id, Velox.Sql.Core.Impl.Operators.Equal, subquery)).ToDebugSql();
        var sqlAll = DbQuery<PostgresTestEntity>.GetPostgresBuilder().Where(w => w.All(x => x.Id, Velox.Sql.Core.Impl.Operators.GreaterThan, subquery)).ToDebugSql();
        var sqlSome = DbQuery<PostgresTestEntity>.GetPostgresBuilder().Where(w => w.Some(x => x.Id, Velox.Sql.Core.Impl.Operators.NotEqual, subquery)).ToDebugSql();

        Assert.Contains("WHERE \"pg_table\".\"id\" = ANY (SELECT \"pg_join_table\".\"id\"", sqlAny);
        Assert.Contains("WHERE \"pg_table\".\"id\" > ALL (SELECT \"pg_join_table\".\"id\"", sqlAll);
        Assert.Contains("WHERE \"pg_table\".\"id\" <> SOME (SELECT \"pg_join_table\".\"id\"", sqlSome);
    }

    [Fact]
    public void Where_MixedExpressionAndFluent_ReturnsCorrectSql()
    {
        var subquery = DbQuery<PostgresJoinEntity>.GetPostgresBuilder()
            .Select(x => x.Id);

        var sql = DbQuery<PostgresTestEntity>.GetPostgresBuilder()
            .Select(x => x.Id)
            .From<PostgresTestEntity>()
            .Where(x => x.Id > 10)
            .Where(w => w.Exists(subquery))
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\" FROM \"pg_table\" WHERE (\"pg_table\".\"id\" > 10) AND EXISTS (SELECT \"pg_join_table\".\"id\" AS \"Id\" FROM \"pg_join_table\");", sql);
    }
}
