#pragma warning disable CS0618
using Velox.Sql.Impl;
using Velox.Sql.Impl.Builders;
using Velox.Sql.Impl.Map;

namespace Velox.Sql.Tests.Postgres;

public class JoinTests : TestBase
{
    [Fact]
    public void InnerJoin_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Select(x => x.Id)
            .InnerJoin<PostgresJoinEntity, PostgresTestEntity>(
                j => j.ParentId,
                p => p.Id,
                (j, p) => new { j.Name })
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\", \"pg_join_table\".\"name\" AS \"Name\" FROM \"pg_table\" INNER JOIN \"pg_join_table\" ON \"pg_table\".\"id\" = \"pg_join_table\".\"parent_id\";", sql);
    }

    [Fact]
    public void InnerJoin_WithSelector_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .InnerJoin<PostgresJoinEntity, PostgresTestEntity>(
                x => x.ParentId,
                y => y.Id,
                (x, y) => new { x.Name, y.Description })
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_join_table\".\"name\" AS \"Name\", \"pg_table\".\"description\" AS \"Description\" FROM \"pg_table\" INNER JOIN \"pg_join_table\" ON \"pg_table\".\"id\" = \"pg_join_table\".\"parent_id\";", sql);
    }

    [Fact]
    public void LeftJoin_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Select(x => x.Id)
            .LeftJoin<PostgresJoinEntity, PostgresTestEntity>(
                j => j.ParentId,
                p => p.Id)
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\" FROM \"pg_table\" LEFT JOIN \"pg_join_table\" ON \"pg_join_table\".\"parent_id\" = \"pg_table\".\"id\";", sql);
    }

    [Fact]
    public void RightJoin_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Select(x => x.Id)
            .RightJoin<PostgresJoinEntity, PostgresTestEntity>(
                j => j.ParentId,
                p => p.Id)
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\" FROM \"pg_table\" RIGHT JOIN \"pg_join_table\" ON \"pg_join_table\".\"parent_id\" = \"pg_table\".\"id\";", sql);
    }

    [Fact]
    public void FullJoin_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Select(x => x.Id)
            .FullJoin<PostgresJoinEntity, PostgresTestEntity>(
                j => j.ParentId,
                p => p.Id)
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\" FROM \"pg_table\" FULL JOIN \"pg_join_table\" ON \"pg_join_table\".\"parent_id\" = \"pg_table\".\"id\";", sql);
    }

    [Fact]
    public void CrossJoin_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Select(x => x.Id)
            .CrossJoin<PostgresJoinEntity>()
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\" FROM \"pg_table\" CROSS JOIN \"pg_join_table\";", sql);
    }

    [Fact]
    public void Join_Security_IdentifierQuoting_ReturnsCorrectSql()
    {
        // Custom registration for malicious entity
        var config = new PgSqlConfiguration(new List<IClassMapper>
        {
            new MaliciousJoinEntityMapper(),
            new PostgresTestEntityMapper()
        });

        var sql = new PostgresBuilder<PostgresTestEntity>(config)
            .InnerJoin<MaliciousJoinEntity, PostgresTestEntity>(
                j => j.InjectionColumn,
                p => p.Id)
            .ToDebugSql();

        // The column "Col\"-broken" should become "\"Col\"\"-broken\""
        Assert.Contains("\"pg_malicious_table\".\"Col\"\"-broken\"", sql);
    }

    private class MaliciousJoinEntity
    {
        public int Id { get; set; }
        public int InjectionColumn { get; set; }
    }

    private class MaliciousJoinEntityMapper : Mapper<MaliciousJoinEntity>
    {
        public MaliciousJoinEntityMapper()
        {
            Table("pg_malicious_table");
            Map(x => x.Id).Column("id");
            Map(x => x.InjectionColumn).Column("Col\"-broken");
            Build();
        }
    }
}
