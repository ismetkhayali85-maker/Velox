using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Numerics;
using System.Reflection;

namespace Velox.Sql.Impl.Map;

public interface IClassMapper
{
    string DatabaseName { get; }
    string SchemaName { get; }
    string TableName { get; }
    string FunctionName { get; }
    List<PropertyMap> Properties { get; }
    List<PropertyMap> FunctionParamsProperties { get; }
    Type EntityType { get; }
    Type FuncParamsType { get; }
    PropertyMap HasKey();
    string GetUserDefinedName(string columnName);
}

public interface IClassMapper<T> : IClassMapper where T : class
{
}

public interface IClassFuncMapper<TFuncParams, T> : IClassMapper where T : class where TFuncParams : class
{
}

public class Mapper<T> : IClassMapper<T> where T : class
{
    private PropertyMap _key;
    private Dictionary<string, string> _nameCache;

    public Mapper()
    {
        Properties = new List<PropertyMap>();
        FunctionParamsProperties = new List<PropertyMap>();

        PropertyTypeKeyTypeMapping = new Dictionary<Type, KeyType>
        {
            {typeof(byte), KeyType.Identity}, {typeof(byte?), KeyType.Identity},
            {typeof(sbyte), KeyType.Identity}, {typeof(sbyte?), KeyType.Identity},
            {typeof(short), KeyType.Identity}, {typeof(short?), KeyType.Identity},
            {typeof(ushort), KeyType.Identity}, {typeof(ushort?), KeyType.Identity},
            {typeof(int), KeyType.Identity}, {typeof(int?), KeyType.Identity},
            {typeof(uint), KeyType.Identity}, {typeof(uint?), KeyType.Identity},
            {typeof(long), KeyType.Identity}, {typeof(long?), KeyType.Identity},
            {typeof(ulong), KeyType.Identity}, {typeof(ulong?), KeyType.Identity},
            {typeof(BigInteger), KeyType.Identity}, {typeof(BigInteger?), KeyType.Identity},
            {typeof(Guid), KeyType.Guid}, {typeof(Guid?), KeyType.Guid}
        };

        Table(typeof(T).Name);
    }

    protected Dictionary<Type, KeyType> PropertyTypeKeyTypeMapping { get; }

    public string DatabaseName { get; protected set; }
    public string SchemaName { get; protected set; }
    public string TableName { get; protected set; }
    public string FunctionName { get; protected set; }
    public List<PropertyMap> Properties { get; }
    public List<PropertyMap> FunctionParamsProperties { get; }

    public Type EntityType => typeof(T);

    public Type FuncParamsType => default;

    public PropertyMap HasKey()
    {
        return _key;
    }

    public string GetUserDefinedName(string columnName)
    {
        if (_nameCache == null || _nameCache.Count != Properties.Count)
        {
            _nameCache = Properties.ToDictionary(x => x.PropertyInfo.Name, x => x.ColumnName);
        }

        if (_nameCache.TryGetValue(columnName, out var name))
            return name;

        return columnName;
    }

    public void Database(string databaseName)
    {
        DatabaseName = databaseName;
    }

    public void Schema(string schemaName)
    {
        SchemaName = schemaName;
    }

    public void Table(string tableName)
    {
        TableName = tableName;
    }

    public void Function(string functionName)
    {
        FunctionName = functionName;
    }

    protected PropertyMap Map(Expression<Func<T, object>> expression)
    {
        var propertyInfo = ReflectionHelper.GetProperty(expression) as PropertyInfo;
        return Map(propertyInfo);
    }

    protected PropertyMap Map(PropertyInfo propertyInfo)
    {
        var result = new PropertyMap(propertyInfo);
        Properties.Add(result);
        return result;
    }

    protected void Build()
    {
        AutoMap(null);
    }

    protected void AutoMap(Func<Type, PropertyInfo, bool> canMap)
    {
        var type = typeof(T);
        var hasDefinedKey = Properties.Any(p => p.KeyType != KeyType.NotAKey);
        PropertyMap keyMap = null;

        _key = Properties.SingleOrDefault(p => p.KeyType == KeyType.Identity);

        foreach (var propertyInfo in type.GetProperties().AsSpan())
        {
            if (Properties.Any(p =>
                p.Name.Equals(propertyInfo.Name, StringComparison.InvariantCultureIgnoreCase))) continue;

            if (canMap != null && !canMap(type, propertyInfo)) continue;

            var map = Map(propertyInfo);
            if (!hasDefinedKey)
            {
                if (string.Equals(map.PropertyInfo.Name, "id", StringComparison.InvariantCultureIgnoreCase))
                    keyMap = map;

                if (keyMap == null && map.PropertyInfo.Name.EndsWith("id", true, CultureInfo.InvariantCulture))
                    keyMap = map;
            }
        }

        if (keyMap != null)
            keyMap.Key(PropertyTypeKeyTypeMapping.ContainsKey(keyMap.PropertyInfo.PropertyType)
                ? PropertyTypeKeyTypeMapping[keyMap.PropertyInfo.PropertyType]
                : KeyType.Assigned);
    }
}

public sealed class FunctionMapper<TFuncParams, T> : IClassFuncMapper<TFuncParams, T> where T : class where TFuncParams : class
{
    private PropertyMap _key;
    private Dictionary<string, string> _nameCache;

    public FunctionMapper()
    {
        Properties = new List<PropertyMap>();
        FunctionParamsProperties = new List<PropertyMap>();

        PropertyTypeKeyTypeMapping = new Dictionary<Type, KeyType>
        {
            {typeof(byte), KeyType.Identity}, {typeof(byte?), KeyType.Identity},
            {typeof(sbyte), KeyType.Identity}, {typeof(sbyte?), KeyType.Identity},
            {typeof(short), KeyType.Identity}, {typeof(short?), KeyType.Identity},
            {typeof(ushort), KeyType.Identity}, {typeof(ushort?), KeyType.Identity},
            {typeof(int), KeyType.Identity}, {typeof(int?), KeyType.Identity},
            {typeof(uint), KeyType.Identity}, {typeof(uint?), KeyType.Identity},
            {typeof(long), KeyType.Identity}, {typeof(long?), KeyType.Identity},
            {typeof(ulong), KeyType.Identity}, {typeof(ulong?), KeyType.Identity},
            {typeof(BigInteger), KeyType.Identity}, {typeof(BigInteger?), KeyType.Identity},
            {typeof(Guid), KeyType.Guid}, {typeof(Guid?), KeyType.Guid}
        };

        Function(typeof(T).Name);
    }

    public Dictionary<Type, KeyType> PropertyTypeKeyTypeMapping { get; }

    public string DatabaseName { get; set; }
    public string SchemaName { get; set; }
    public string TableName { get; set; }
    public string FunctionName { get; set; }
    public List<PropertyMap> Properties { get; }
    public List<PropertyMap> FunctionParamsProperties { get; }

    public Type EntityType => typeof(T);

    public Type FuncParamsType => typeof(TFuncParams);

    public PropertyMap HasKey()
    {
        return _key;
    }

    public string GetUserDefinedName(string columnName)
    {
        if (_nameCache == null || _nameCache.Count != Properties.Count)
        {
            _nameCache = Properties.ToDictionary(x => x.PropertyInfo.Name, x => x.ColumnName);
        }

        if (_nameCache.TryGetValue(columnName, out var name))
            return name;

        return columnName;
    }

    public void Table(string tableName)
    {
        TableName = tableName;
    }

    public void Schema(string schemaName)
    {
        SchemaName = schemaName;
    }

    public void Function(string functionName)
    {
        TableName = FunctionName = functionName;
    }

    public PropertyMap FunctionParamsMap(Expression<Func<TFuncParams, object>> expression)
    {
        var propertyInfo = ReflectionHelper.GetProperty(expression) as PropertyInfo;
        return FunctionParamsMap(propertyInfo);
    }

    public PropertyMap FunctionParamsMap(PropertyInfo propertyInfo)
    {
        var result = new PropertyMap(propertyInfo);
        FunctionParamsProperties.Add(result);
        return result;
    }

    public PropertyMap Map(Expression<Func<T, object>> expression)
    {
        var propertyInfo = ReflectionHelper.GetProperty(expression) as PropertyInfo;
        return Map(propertyInfo);
    }

    public PropertyMap Map(PropertyInfo propertyInfo)
    {
        var result = new PropertyMap(propertyInfo);
        Properties.Add(result);
        return result;
    }

    public void Build()
    {
        AutoFunctionMap(null);
        AutoMap(null);
    }

    public void AutoMap(Func<Type, PropertyInfo, bool> canMap)
    {
        var type = typeof(T);
        var hasDefinedKey = Properties.Any(p => p.KeyType != KeyType.NotAKey);
        PropertyMap keyMap = null;

        _key = Properties.SingleOrDefault(p => p.KeyType == KeyType.Identity);

        foreach (var propertyInfo in type.GetProperties().AsSpan())
        {
            if (Properties.Any(p =>
                p.Name.Equals(propertyInfo.Name, StringComparison.InvariantCultureIgnoreCase))) continue;

            if (canMap != null && !canMap(type, propertyInfo)) continue;

            var map = Map(propertyInfo);
            if (!hasDefinedKey)
            {
                if (string.Equals(map.PropertyInfo.Name, "id", StringComparison.InvariantCultureIgnoreCase))
                    keyMap = map;

                if (keyMap == null && map.PropertyInfo.Name.EndsWith("id", true, CultureInfo.InvariantCulture))
                    keyMap = map;
            }
        }

        if (keyMap != null)
            keyMap.Key(PropertyTypeKeyTypeMapping.ContainsKey(keyMap.PropertyInfo.PropertyType)
                ? PropertyTypeKeyTypeMapping[keyMap.PropertyInfo.PropertyType]
                : KeyType.Assigned);
    }

    public void AutoFunctionMap(Func<Type, PropertyInfo, bool> canMap)
    {
        var type = typeof(TFuncParams);
        var hasDefinedKey = FunctionParamsProperties.Any(p => p.KeyType != KeyType.NotAKey);
        PropertyMap keyMap = null;

        _key = FunctionParamsProperties.SingleOrDefault(p => p.KeyType == KeyType.Identity);

        foreach (var propertyInfo in type.GetProperties().AsSpan())
        {
            if (FunctionParamsProperties.Any(p =>
                p.Name.Equals(propertyInfo.Name, StringComparison.InvariantCultureIgnoreCase))) continue;

            if (canMap != null && !canMap(type, propertyInfo)) continue;

            var map = Map(propertyInfo);
            if (!hasDefinedKey)
            {
                if (string.Equals(map.PropertyInfo.Name, "id", StringComparison.InvariantCultureIgnoreCase))
                    keyMap = map;

                if (keyMap == null && map.PropertyInfo.Name.EndsWith("id", true, CultureInfo.InvariantCulture))
                    keyMap = map;
            }
        }

        if (keyMap != null)
            keyMap.Key(PropertyTypeKeyTypeMapping.ContainsKey(keyMap.PropertyInfo.PropertyType)
                ? PropertyTypeKeyTypeMapping[keyMap.PropertyInfo.PropertyType]
                : KeyType.Assigned);
    }
}