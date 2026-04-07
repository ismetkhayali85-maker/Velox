using System;

namespace Velox.Sql.Expressions;

internal sealed class MemberUnaryResult
{
    public Type Type { get; set; }
    public string Value { get; set; }
}