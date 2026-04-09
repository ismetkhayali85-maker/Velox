#pragma warning disable CS0618

namespace Velox.Sql.Tests.Postgres;

public class SelectTests : TestBase
{
    [Fact]
    public void Select_All_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Select()
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\", \"pg_table\".\"description\" AS \"Description\" FROM \"pg_table\";", sql);
    }

    [Fact]
    public void Select_Specific_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Select(x => x.Description)
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"description\" AS \"Description\" FROM \"pg_table\";", sql);
    }

    [Fact]
    public void Select_Multiple_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Select(x => new { x.Id, x.Description })
            .ToDebugSql();

        Assert.Equal("SELECT \"pg_table\".\"id\" AS \"Id\", \"pg_table\".\"description\" AS \"Description\" FROM \"pg_table\";", sql);
    }

    [Fact]
    public void Select_Avg_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Avg(x => x.Id, "AverageId")
            .ToDebugSql();

        Assert.Equal("SELECT AVG(\"pg_table\".\"id\") AS \"AverageId\" FROM \"pg_table\";", sql);
    }

    [Fact]
    public void Select_Sum_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Sum(x => x.Id, "TotalId")
            .ToDebugSql();

        Assert.Equal("SELECT SUM(\"pg_table\".\"id\") AS \"TotalId\" FROM \"pg_table\";", sql);
    }

    [Fact]
    public void Select_Min_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Min(x => x.Id, "MinId")
            .ToDebugSql();

        Assert.Equal("SELECT MIN(\"pg_table\".\"id\") AS \"MinId\" FROM \"pg_table\";", sql);
    }

    [Fact]
    public void Select_Max_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Max(x => x.Id, "MaxId")
            .ToDebugSql();

        Assert.Equal("SELECT MAX(\"pg_table\".\"id\") AS \"MaxId\" FROM \"pg_table\";", sql);
    }

    [Fact]
    public void Select_Count_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Count(x => x.Id, "IdCount")
            .ToDebugSql();

        Assert.Equal("SELECT COUNT(\"pg_table\".\"id\") AS \"IdCount\" FROM \"pg_table\";", sql);
    }

    [Fact]
    public void Select_CountAll_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Count("TotalCount")
            .ToDebugSql();

        Assert.Equal("SELECT COUNT(*) FROM \"pg_table\";", sql); 
    }

    [Fact]
    public void Select_CountDistinct_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .CountDistinct(x => x.Description, "UniqueDesc")
            .ToDebugSql();

        Assert.Equal("SELECT COUNT(DISTINCT(\"pg_table\".\"description\")) AS \"UniqueDesc\" FROM \"pg_table\";", sql);
    }

    [Fact]
    public void Select_Now_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Select(s => s.Now("CurrentTimestamp"))
            .ToDebugSql();

        Assert.Equal("SELECT NOW() AS \"CurrentTimestamp\" FROM \"pg_table\";", sql);
    }

    [Fact]
    public void Select_StringFunctions_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Select(s => s
                .Lcase(new Core.PostgreSql.Column("pg_table", "description", "Lowered"))
                .Ucase(new Core.PostgreSql.Column("pg_table", "description", "Uppered"))
                .Len(new Core.PostgreSql.Column("pg_table", "id", "IdLength")))
            .ToDebugSql();

        Assert.Equal("SELECT LCASE(\"pg_table\".\"description\") AS \"Lowered\", UCASE(\"pg_table\".\"description\") AS \"Uppered\", LEN(\"pg_table\".\"id\") AS \"IdLength\" FROM \"pg_table\";", sql);
    }

    [Fact]
    public void Select_Round_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Select(s => s.Round(new Core.PostgreSql.Column("pg_table", "id"), 2))
            .ToDebugSql();

        Assert.Equal("SELECT ROUND(\"pg_table\".\"id\",2) FROM \"pg_table\";", sql);
    }

    [Fact]
    public void Select_Mid_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Select(s => s.Mid(new Core.PostgreSql.Column("pg_table", "description"), 1, 5))
            .ToDebugSql();

        Assert.Equal("SELECT MID(\"pg_table\".\"description\", 1, 5) FROM \"pg_table\";", sql);
    }

    [Fact]
    public void Select_GenericFunction_ReturnsCorrectSql()
    {
        var sql = VeloxRuntime.Postgres<PostgresTestEntity>()
            .Select(s => s.Function(new Core.PostgreSql.Column("pg_table", "id"), "MY_FUNC", "Alias"))
            .ToDebugSql();

        Assert.Equal("SELECT MY_FUNC(\"pg_table\".\"id\") AS \"Alias\" FROM \"pg_table\";", sql);
    }

    [Fact]
    public void Selection_Security_IdentifierQuoting_ReturnsCorrectSql()
    {
        // verifies that malicious column/table names are safely quoted.
        var maliciousName = "id\" FROM pg_table; DROP TABLE students; --";
        var table = new Core.PostgreSql.Table("public", "pg_table");
        var col = new Core.PostgreSql.Column(table, maliciousName);
        Assert.Equal("\"public\".\"pg_table\".\"id\"\" FROM pg_table; DROP TABLE students; --\"", col.Name);
    }
}
