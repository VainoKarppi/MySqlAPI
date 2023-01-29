using System.Data;
using System.Reflection;
using System.Text.Json;
using MySql.Data.MySqlClient;


// WORKING FILES!!!

public partial class Program {


    public static void InitMetaData() {
        //TODO CALL AUTOMATICALLY FROM ANY METHOD IF NOT INITIALIZED
        string[] listOfTables = GetTables();
        Console.Write($"Found {listOfTables.Count()} tables!:\n");
        foreach (var item in listOfTables) Console.WriteLine("\t" + item);
        Console.WriteLine();


        foreach (string tableName in listOfTables) {
            if (SqlMetaData.TableInfoData != null && SqlMetaData.TableInfoData.ContainsKey(tableName.ToLower())) continue; // Update missings only
            MySqlCommand myCommand = new MySqlCommand($"SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME =N'{tableName}'", Connection);
            MySqlDataReader reader = myCommand.ExecuteReader();

            var tableInfo = new SqlMetaData.TableInfo();
            List<SqlMetaData.ColumnInfo> columnInfoAll = new List<SqlMetaData.ColumnInfo>();
            bool firstRow = true;
            while (reader.Read()) {       

                var columnData = new SqlMetaData.ColumnInfo() {
                    ColumnName = reader["COLUMN_NAME"].ToString(),
                    Nullable = reader.GetString("IS_NULLABLE").ToLower() == "yes",
                    Type = GetDataType(reader.GetString("DATA_TYPE")),
                    Signed = !(reader.GetString("COLUMN_TYPE").ToLower().Contains("unsigned"))
                };

                string? name = columnData.ColumnName?.ToLower();
                
                if (firstRow && name != null && (name == "id" || name == "uid" || name == "uuid" || name == "guid")) {
                    tableInfo.UsesGuid = true;
                    tableInfo.GuidColumnName = columnData.ColumnName!;
                    firstRow = false;
                    continue;
                }

                firstRow = false;
                columnInfoAll.Add(columnData);
            }
            reader.Close();
            if (columnInfoAll.Count == 0) return;

            tableInfo.ColumnInfo = columnInfoAll.ToArray();

            if (SqlMetaData.TableInfoData == null) SqlMetaData.TableInfoData = new Dictionary<string, SqlMetaData.TableInfo>() { };
            SqlMetaData.TableInfoData.Add(tableName.ToLower(), tableInfo);
        }
    }



    //!! -----------------!!//
    //!! DATABASE METHODS !!//
    //!! -----------------!!//
    public static bool DatabaseExists() {
        try {
            MySqlCommand myCommand = Connection.CreateCommand();
            myCommand.CommandText = $"SHOW TABLE STATUS FROM `{DatabaseName}`;";
            var asd = myCommand.ExecuteScalar();
            return true;
        } catch (Exception ex) {
            if (ex == null) return false;
            return false;
        }
    }
    public static void CreateDatabase() {
        MySqlCommand myCommand = Connection.CreateCommand();
        myCommand.CommandText = $"CREATE DATABASE {Connection.Database};";
        myCommand.ExecuteScalar();
    }
    public static void DeleteDatabase() {
        MySqlCommand myCommand = Connection.CreateCommand();
        myCommand.CommandText = $"DROP DATABASE {Connection.Database};";
        myCommand.ExecuteScalar();
    }











    //!! --------------!!//
    //!! TABLE METHODS !!//
    //!! --------------!!//

    public static void CreateTable(string tableName) {
        Console.WriteLine($"Trying to create a new table... {tableName}");
        SqlMetaData.TableInfo? tableInfo = GetTableInfo(tableName)!;
        if (tableInfo == null) throw new Exception($"Table ({tableName}) Not Found!");
        string idName = tableInfo.GuidColumnName!;
        MySqlCommand myCommand = new MySqlCommand(@$"
            CREATE TABLE `{tableName}` (
                {idName} BINARY(16) NOT NULL, 
                PRIMARY KEY (`{idName}`) USING BTREE
            ) COLLATE='utf8_general_ci' ENGINE=InnoDB;", Connection);
        myCommand.ExecuteScalar();
        Console.WriteLine($"Table created!");

        InitMetaData(); // TODO DO ONLY INTERNALLY
    }
    public static void CreateTable<T>(string? tableName = null) {
        // TODO update metadata
        Console.WriteLine($"Trying to create a new table... {tableName}");
        List<PropertyInfo> propertyInfos = typeof(T).GetProperties().ToList();
        
        if (tableName == null) {
            tableName = (string)typeof(T).GetField("TableName")?.GetValue(null)!;
            if (tableName == null) tableName = typeof(T).ToString();
        }

        SqlMetaData.TableInfo? tableInfo = GetTableInfo(tableName)!;
        if (tableInfo == null) throw new Exception($"Table ({tableName}) Not Found!");
        string idName = tableInfo.GuidColumnName!;

        int removed = propertyInfos.RemoveAll(x => x.Name.ToLower() == idName.ToLower());
        
        string command = @$"CREATE TABLE `{tableName}` (";
        if (removed > 0) command += $"`{idName}` BINARY(16) NOT NULL, ";

        int i = 0;
        foreach (PropertyInfo info in propertyInfos) {
            if (info.CanWrite && info.CanRead) {
                string type = GetSqlDataType(info.PropertyType);
                // TODO if null not allowed get default

                bool unsigned = type.Contains("!"); type = type.Replace("!","");

                command += $"`{info.Name}` {type}";
                if (unsigned) command += " UNSIGNED"; // `Unsigned` INT(10) UNSIGNED
                command += " NULL DEFAULT NULL";
                // not nullable                 = `Key` INT NOT NULL,
                // unsigned                     = `Key` INT UNSIGNED NULL,
                // unsigned AND not nullable    = `Key` INT UNSIGNED NOT NULL,
                if (type == "TINYINT(1)") command += $", CHECK ({info.Name}<=1)"; // Check if boolean and allow 1 and 0 only
            }
            i++;
            if (i != propertyInfos.Count()) command += ", ";
        }
        command += $"PRIMARY KEY (`{idName}`) USING BTREE) COLLATE='utf8_general_ci' ENGINE=InnoDB;";

        MySqlCommand myCommand = new MySqlCommand(command, Connection);
        myCommand.ExecuteScalar();
        Console.WriteLine($"Table created!");

        InitMetaData(); // TODO DO ONLY INTERNALLY
    }
    public static void DeleteTable(string tableName) {
        Console.WriteLine($"Trying to delete a table... {tableName}");
        MySqlCommand myCommand = new MySqlCommand($"DROP TABLE {tableName};", Connection);
        myCommand.ExecuteScalar();
        Console.WriteLine($"Table deleted!");
    }
    public static bool TableExists(string tableName) {
        try {
            MySqlCommand myCommand = new MySqlCommand($"SELECT 1 FROM {tableName} WHERE 1=2;", Connection);
            myCommand.ExecuteScalar();
            return true;
        } catch (Exception ex) {
            if (ex == null) return false;
            return false;
        }
    }










    //!! -----------------!!//
    //!! METADATA METHODS !!//
    //!! -----------------!!//
    public static SqlMetaData.TableInfo? GetTableInfo(string tableName) {
        return SqlMetaData.TableInfoData?[tableName.ToLower()]!;
    }
    public static SqlMetaData.ColumnInfo? GetColumnInfo(string tableName, string columnName) {
        return SqlMetaData.TableInfoData?[tableName].ColumnInfo?.Single(x => x.ColumnName?.ToLower() == columnName.ToLower());
    }

    public static string[] GetTables() {
        MySqlCommand myCommand = new MySqlCommand($"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA='{Connection.Database}' ", Connection);
        MySqlDataReader reader = myCommand.ExecuteReader();
        List<string> listOfTables = new List<string>();
        while (reader.Read()) {
            string? tableName = reader["TABLE_NAME"].ToString();
            if (tableName != null) listOfTables.Add(tableName);
        }
        reader.Close();
        return listOfTables.ToArray();
    }



    public static Type GetDataType(string typeText) {
        typeText = typeText.ToLower();
        if (typeText.Contains("text")) return typeof(string);
        if (typeText.Contains("int")) return typeof(Int32);
        if (typeText.Contains("datetime") || typeText == "year" || typeText.Contains("time")) return typeof(DateTime);
        if (typeText == "json") return typeof(string);
        if (typeText == "float") return typeof(float);
        if (typeText == "double") return typeof(double);
        if (typeText == "decimal") return typeof(decimal);
        if (typeText == "char") return typeof(char);
        if (typeText == "varchar") return typeof(char[]);
        if (typeText == "binary") return typeof(byte[]);
        throw new NotImplementedException($"{typeText} not yet implemented in ParseDataType()");
    }

    private static string GetSqlDataType(Type? type) {

        if (Nullable.GetUnderlyingType(type!) != null) type = Nullable.GetUnderlyingType(type!);

        if (type == typeof(byte)) return "TINYINT";
        if (type == typeof(sbyte)) return "TINYINT!";
        if (type == typeof(short)) return "SMALLINT";
        if (type == typeof(ushort)) return "SMALLINT!";
        if (type == typeof(int)) return "INT";
        if (type == typeof(uint)) return "INT!";
        if (type == typeof(long)) return "BIGINT";
        if (type == typeof(ulong)) return "BIGINT!";
        if (type == typeof(double)) return "DOUBLE";
        if (type == typeof(float)) return "FLOAT";
        if (type == typeof(decimal)) return "DECIMAL";
        if (type == typeof(string)) return "LONGTEXT";
        if (type == typeof(DateTime)) return "DATETIME";
        if (type == typeof(bool)) return "TINYINT(1)";
        if (type! != null && type.IsArray) throw new NotImplementedException(); // char[] AND ???
        throw new NotImplementedException($"Type: ({type}), Not yet implemented!");
    }
}







public class SqlMetaData {
    public static Dictionary<string, SqlMetaData.TableInfo>? TableInfoData { get; set; }

    public class TableInfo {
        public string? GuidColumnName { get; set; }
        public bool UsesGuid { get; set; } = false;
        public SqlMetaData.ColumnInfo[]? ColumnInfo { get; set; }
    }
    public class ColumnInfo {
        public string? ColumnName { get; set; }
        public uint? ColumnCount { get; set; }
        public bool? Nullable { get; set; }
        public Type? Type { get; set; }
        public bool Signed { get; set; }
    }

}


public abstract class MySqlObjectClass {
    public Guid Id { get; internal set; }
    public static string? TableName { get; internal set; }
    public MySqlObjectClass() {
        TableName = this.GetType().UnderlyingSystemType.Name;
    }
}