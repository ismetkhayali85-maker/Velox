namespace Velox.Sql.Registration;

/// <summary>
/// Target SQL engine(s) for a class mapper. Combine flags when one mapper class is registered for multiple engines.
/// </summary>
[System.Flags]
public enum SqlEngine
{
    None = 0,
    PostgreSQL = 1 << 0,
    ClickHouse = 1 << 1,
}
