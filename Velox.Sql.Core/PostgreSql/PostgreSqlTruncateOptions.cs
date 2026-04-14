namespace Velox.Sql.Core.PostgreSql;

/// <summary>PostgreSQL <c>TRUNCATE</c> identity sequence behavior (see <c>RESTART IDENTITY</c> / <c>CONTINUE IDENTITY</c>).</summary>
public enum PostgreSqlTruncateIdentityOption
{
    /// <summary>Omit clause (server default: continue identity).</summary>
    Unspecified = 0,
    RestartIdentity = 1,
    ContinueIdentity = 2,
}

/// <summary>PostgreSQL <c>TRUNCATE</c> referential action (<c>CASCADE</c> / <c>RESTRICT</c>).</summary>
public enum PostgreSqlTruncateReferentialOption
{
    /// <summary>Omit clause (server default: restrict).</summary>
    Unspecified = 0,
    Cascade = 1,
    Restrict = 2,
}
