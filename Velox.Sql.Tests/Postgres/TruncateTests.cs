#pragma warning disable CS0618

namespace Velox.Sql.Tests.Postgres;

public class TruncateTests : TestBase
{
    [Fact]
    public void Truncate_Default_ReturnsTruncateTableOnly()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Truncate()
            .ToDebugSql();

        Assert.Equal("TRUNCATE TABLE \"pg_table\";", sql);
    }

    [Fact]
    public void Truncate_RestartIdentity_AppendsClause()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Truncate()
            .RestartIdentity()
            .ToDebugSql();

        Assert.Equal("TRUNCATE TABLE \"pg_table\" RESTART IDENTITY;", sql);
    }

    [Fact]
    public void Truncate_ContinueIdentity_AppendsClause()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Truncate()
            .ContinueIdentity()
            .ToDebugSql();

        Assert.Equal("TRUNCATE TABLE \"pg_table\" CONTINUE IDENTITY;", sql);
    }

    [Fact]
    public void Truncate_Cascade_AppendsClause()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Truncate()
            .Cascade()
            .ToDebugSql();

        Assert.Equal("TRUNCATE TABLE \"pg_table\" CASCADE;", sql);
    }

    [Fact]
    public void Truncate_Restrict_AppendsClause()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Truncate()
            .Restrict()
            .ToDebugSql();

        Assert.Equal("TRUNCATE TABLE \"pg_table\" RESTRICT;", sql);
    }

    [Fact]
    public void Truncate_RestartIdentityAndCascade_OrderMatchesPostgres()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Truncate()
            .RestartIdentity()
            .Cascade()
            .ToDebugSql();

        Assert.Equal("TRUNCATE TABLE \"pg_table\" RESTART IDENTITY CASCADE;", sql);
    }

    [Fact]
    public void Truncate_ContinueIdentityAndRestrict_OrderMatchesPostgres()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Truncate()
            .ContinueIdentity()
            .Restrict()
            .ToDebugSql();

        Assert.Equal("TRUNCATE TABLE \"pg_table\" CONTINUE IDENTITY RESTRICT;", sql);
    }

    [Fact]
    public void Truncate_CascadeThenRestartIdentity_StillValidOrder()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Truncate()
            .Cascade()
            .RestartIdentity()
            .ToDebugSql();

        Assert.Equal("TRUNCATE TABLE \"pg_table\" RESTART IDENTITY CASCADE;", sql);
    }

    [Fact]
    public void Truncate_AddSql_AppendsFragment()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Truncate()
            .AddSql(" /* maintenance */")
            .ToDebugSql();

        Assert.Equal("TRUNCATE TABLE \"pg_table\" /* maintenance */;", sql);
    }

    [Fact]
    public void Truncate_RestartIdentityThenContinueIdentity_Throws()
    {
        Assert.Throws<InvalidOperationException>(() =>
        {
            VeloxRuntime.Postgres<PostgresTestEntity>()
                .Truncate()
                .RestartIdentity()
                .ContinueIdentity();
        });
    }

    [Fact]
    public void Truncate_CascadeThenRestrict_Throws()
    {
        Assert.Throws<InvalidOperationException>(() =>
        {
            VeloxRuntime.Postgres<PostgresTestEntity>()
                .Truncate()
                .Cascade()
                .Restrict();
        });
    }

    [Fact]
    public void Truncate_ContinueIdentityThenRestartIdentity_Throws()
    {
        Assert.Throws<InvalidOperationException>(() =>
        {
            VeloxRuntime.Postgres<PostgresTestEntity>()
                .Truncate()
                .ContinueIdentity()
                .RestartIdentity();
        });
    }

    [Fact]
    public void Truncate_RestrictThenCascade_Throws()
    {
        Assert.Throws<InvalidOperationException>(() =>
        {
            VeloxRuntime.Postgres<PostgresTestEntity>()
                .Truncate()
                .Restrict()
                .Cascade();
        });
    }
}
