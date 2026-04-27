using System.Globalization;

namespace Velox.Sql.Tests.Postgres;

public class ParameterizationTests : TestBase
{
    [Fact]
    public void Select_WithParameters_ReturnsCorrectSqlAndDictionary()
    {
        var builder = VeloxRuntime.Postgres<PostgresTestEntity>();
        builder.Select()
               .Where(x => x.Id == 10 && x.Description == "Test")
               .OrderByAsc<PostgresTestEntity>(x => x.Id);

        AssertQuery(builder,
            debug: "SELECT \"pg_table\".\"id\" AS \"Id\", \"pg_table\".\"description\" AS \"Description\" FROM \"pg_table\" WHERE \"pg_table\".\"id\" = 10 AND \"pg_table\".\"description\" = 'Test' ORDER BY \"pg_table\".\"id\" ASC;",
            sql:   "SELECT \"pg_table\".\"id\" AS \"Id\", \"pg_table\".\"description\" AS \"Description\" FROM \"pg_table\" WHERE \"pg_table\".\"id\" = @p0 AND \"pg_table\".\"description\" = @p1 ORDER BY \"pg_table\".\"id\" ASC;",
            expectedParams: new { p0 = 10, p1 = "Test" });
    }

    [Fact]
    public void Where_In_WithParameters_ReturnsCorrectSqlAndDictionary()
    {
        var ids = new object[] { 1, 2, 3 };
        var builder = VeloxRuntime.Postgres<PostgresTestEntity>();
        builder.Select().Where(w => w.In(x => x.Id, ids));

        AssertQuery(builder,
            debug: "SELECT \"pg_table\".\"id\" AS \"Id\", \"pg_table\".\"description\" AS \"Description\" FROM \"pg_table\" WHERE \"pg_table\".\"id\" IN (1,2,3);",
            sql:   "SELECT \"pg_table\".\"id\" AS \"Id\", \"pg_table\".\"description\" AS \"Description\" FROM \"pg_table\" WHERE \"pg_table\".\"id\" IN (@p0,@p1,@p2);",
            expectedParams: new { p0 = 1, p1 = 2, p2 = 3 });
    }

    [Fact]
    public void Update_WithParameters_ReturnsCorrectSqlAndDictionary()
    {
        var entity = new PostgresTestEntity { Id = 1, Description = "Updated" };
        var builder = VeloxRuntime.Postgres<PostgresTestEntity>();
        var updateBuilder = builder.Update(entity, x => x.Id == 1);

        AssertQuery(updateBuilder,
            debug: "UPDATE \"pg_table\" SET \"id\" = 1, \"description\" = 'Updated' WHERE \"pg_table\".\"id\" = 1;",
            sql:   "UPDATE \"pg_table\" SET \"id\" = @p0, \"description\" = @p1 WHERE \"pg_table\".\"id\" = @p2;",
            expectedParams: new { p0 = 1, p1 = "Updated", p2 = 1 });
    }

    [Fact]
    public void Contains_WithParameters_ReturnsCorrectSqlAndDictionary()
    {
        var builder = VeloxRuntime.Postgres<PostgresTestEntity>();
        builder.Select().Where(x => x.Description.Contains("abc"));

        AssertQuery(builder,
            debug: "SELECT \"pg_table\".\"id\" AS \"Id\", \"pg_table\".\"description\" AS \"Description\" FROM \"pg_table\" WHERE \"pg_table\".\"description\"::text ILIKE '%abc%';",
            sql:   "SELECT \"pg_table\".\"id\" AS \"Id\", \"pg_table\".\"description\" AS \"Description\" FROM \"pg_table\" WHERE \"pg_table\".\"description\"::text ILIKE @p0;",
            expectedParams: new { p0 = "%abc%" });
    }

    [Fact]
    public void Where_Enum_WithParameters_UsesUnderlyingInteger()
    {
        var builder = VeloxRuntime.Postgres<EnumPersistenceEntity>();
        builder.Select()
            .Where(x => x.Kind == PersistenceTestEnum.Beta);

        AssertQuery(builder,
            debug: "SELECT \"pg_kind_row\".\"id\" AS \"Id\", \"pg_kind_row\".\"kind\" AS \"Kind\" FROM \"pg_kind_row\" WHERE \"pg_kind_row\".\"kind\" = 2;",
            sql:   "SELECT \"pg_kind_row\".\"id\" AS \"Id\", \"pg_kind_row\".\"kind\" AS \"Kind\" FROM \"pg_kind_row\" WHERE \"pg_kind_row\".\"kind\" = @p0;",
            expectedParams: new { p0 = 2 });
    }

    [Fact]
    public void Where_DateTime_IsQuotedLiteral_WhileNonDateUsesParameters()
    {
        var from = new DateTime(2026, 4, 27, 5, 36, 5, DateTimeKind.Unspecified);
        var q = from.ToString(CultureInfo.InvariantCulture);

        var builder = VeloxRuntime.Postgres<DateTimeEntity>();
        builder.Select()
            .Where(x => x.Id == 1 && x.CreatedAt >= from);

        AssertQuery(builder,
            debug:
            "SELECT \"date_table\".\"id\" AS \"Id\", \"date_table\".\"created_at\" AS \"CreatedAt\" FROM \"date_table\" WHERE \"date_table\".\"id\" = 1 AND \"date_table\".\"created_at\" >= '" + q + "';",
            sql:
            "SELECT \"date_table\".\"id\" AS \"Id\", \"date_table\".\"created_at\" AS \"CreatedAt\" FROM \"date_table\" WHERE \"date_table\".\"id\" = @p0 AND \"date_table\".\"created_at\" >= '" + q + "';",
            expectedParams: new { p0 = 1 });
    }
}
