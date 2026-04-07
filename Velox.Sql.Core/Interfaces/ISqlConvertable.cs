using System;

namespace Velox.Sql.Core.Interfaces;

public interface ISqlConvertable<TTo>
{
    TTo CastTo<TFrom>(TFrom type) where TFrom : Enum;
}