using System;
using System.Reflection;

namespace Velox.Sql.Impl.Map;

public sealed class PropertyMap
{
    public PropertyMap(PropertyInfo propertyInfo)
    {
        PropertyInfo = propertyInfo;
        ColumnName = PropertyInfo.Name;
    }

    public string Name => PropertyInfo.Name;

    public string ColumnName { get; private set; }

    public bool Ignored { get; private set; }

    public bool IsReadOnly { get; private set; }
    public bool IsNullable { get; private set; }

    public PropertyInfo PropertyInfo { get; }

    public KeyType KeyType { get; set; }

    public PropertyMap Column(string columnName)
    {
        ColumnName = columnName;
        return this;
    }

    public PropertyMap Key(KeyType keyType)
    {
        if (Ignored || IsReadOnly)
            throw new ArgumentException($"'{Name}' is ignored and cannot be made a key field. ");

        KeyType = keyType;
        return this;
    }

    public PropertyMap Ignore()
    {
        Ignored = true;
        return this;
    }

    public PropertyMap Nullable()
    {
        IsNullable = true;
        return this;
    }

    public PropertyMap ReadOnly()
    {
        IsReadOnly = true;
        return this;
    }
}