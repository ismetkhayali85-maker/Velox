using System;
using System.Collections.Generic;
using Velox.Sql.Core.PostgreSql;

namespace Velox.Sql.Core.Impl;

public static class SqlHelper
{
    public const string As = " AS ";

    public const string And = " AND ";
    public const string Or = " OR ";

    public const string Comma = ", ";

    public const string Asc = " ASC";
    public const string Desc = " DESC";

    public const string Equal = " = ";
    private static readonly string GreaterThan = " > ";

    private static readonly string LessThan = " < ";
    private static readonly string GreaterThanOrEqual = " >= ";
    private static readonly string LessThanOrEqual = " <= ";
    private static readonly string NotEqual = " <> ";

    public static TEnum ConvertEnum<TEnum>(Enum source)
    {
        return (TEnum)Enum.Parse(typeof(TEnum), source.ToString(), true);
    }

    private static readonly Dictionary<Operators, string> ComparisionOperatorsStore =
        new Dictionary<Operators, string>
        {
            {Operators.Equal, Equal},
            {Operators.NotEqual, NotEqual},
            {Operators.GreaterThan, GreaterThan},
            {Operators.LessThan, LessThan},
            {Operators.GreaterThanOrEqual, GreaterThanOrEqual},
            {Operators.LessThanOrEqual, LessThanOrEqual}
        };

    private static readonly Dictionary<PostgreSqlTypes, string> PostgreSqlTypesDictionary =
        new Dictionary<PostgreSqlTypes, string>
        {
            {PostgreSqlTypes.Bigint, "bigint"},
            {PostgreSqlTypes.BigSerial, "bigserial"},
            {PostgreSqlTypes.Bit, "bit"},
            {PostgreSqlTypes.BitVarying, "bit varying"},
            {PostgreSqlTypes.Boolean, "bool"},
            {PostgreSqlTypes.Box, "box"},
            {PostgreSqlTypes.Bytea, "bytea"},
            {PostgreSqlTypes.Character, "character"},
            {PostgreSqlTypes.CharacterVarying, "character varying"},
            {PostgreSqlTypes.Cidr, "cidr"},
            {PostgreSqlTypes.Circle, "circle"},
            {PostgreSqlTypes.Date, "date"},
            {PostgreSqlTypes.DoublePrecision, "double precision"},
            {PostgreSqlTypes.Inet, "inet"},
            {PostgreSqlTypes.Integer, "integer"},
            {PostgreSqlTypes.Interval, "interval"},
            {PostgreSqlTypes.Json, "json"},
            {PostgreSqlTypes.Line, "line"},
            {PostgreSqlTypes.Lseg, "lseg"},
            {PostgreSqlTypes.Macaddr, "macaddr"},
            {PostgreSqlTypes.Money, "money"},
            {PostgreSqlTypes.Numeric, "numeric"},
            {PostgreSqlTypes.Path, "path"},
            {PostgreSqlTypes.Point, "point"},
            {PostgreSqlTypes.Polygon, "polygon"},
            {PostgreSqlTypes.Real, "real"},
            {PostgreSqlTypes.SmallInt, "smallint"},
            {PostgreSqlTypes.SmallSerial, "smallserial"},
            {PostgreSqlTypes.Serial, "serial"},
            {PostgreSqlTypes.Text, "text"},
            {PostgreSqlTypes.Time, "time"},
            {PostgreSqlTypes.Timestamp, "timestamp"},
            {PostgreSqlTypes.Tsquery, "tsquery"},
            {PostgreSqlTypes.Tsvector, "tsvector"},
            {PostgreSqlTypes.TxidSnapshot, "txid_snapshot"},
            {PostgreSqlTypes.Uuid, "uuid"},
            {PostgreSqlTypes.Xml, "xml"}
        };

    public static string ToString(Operators sqlWhereOperator)
    {
        return ComparisionOperatorsStore[sqlWhereOperator];
    }

    public static string ToString(PostgreSqlTypes type)
    {
        return PostgreSqlTypesDictionary[type];
    }

}