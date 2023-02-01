using System.Data;
using System.Reflection;
using System.Text.Json;
using MySql.Data.MySqlClient;


// WORKING FILES!!!
namespace MySqlDatabaseAPI;

public static class Database {
    public static MySqlConnection Connection = new MySql.Data.MySqlClient.MySqlConnection();
    public static string? DatabaseName;

    /// <summary>
    /// Connects to database and initalizes the metadatas
    /// </summary>
    /// <param name="ip"></param>
    /// <param name="databaseName"></param>
    /// <param name="username"></param>
    /// <param name="password"></param>
    /// <param name="createNewIfNotFound"></param>
    public static void ConnectToDatabase(string ip, string databaseName, string username, string? password, bool createNewIfNotFound = false) {
        Console.Write("Connecting to Database... ");
        Connection.ConnectionString = @$"server={ip};uid={username}";
        if (password != null) Connection.ConnectionString += $";pwd={password}";
        Console.Write($"({Connection.DataSource})...");
        Connection.Open();
        Console.Write("\t*SUCCESS*\n");

        bool dbFound = DatabaseExists(databaseName);
        if (!dbFound && createNewIfNotFound) {            
            Console.WriteLine("Database not found, Creating new...");
            CreateDatabase(databaseName); // Create new database if one doesent already existDataColumn
            dbFound = true;
            Console.WriteLine($"Database: {databaseName} created!");
        }
        if (dbFound) {
            Connection.ChangeDatabase(databaseName); // Select Database
            InitMetaData();
        }
            
    }

    /// <summary>
    /// Disconnect from the database and dispose socket.
    /// </summary>
    /// <exception cref="Exception"></exception>
    public static void DisconnectFromDatabase() {
        if (Connection.State == 0) throw new Exception("Connection is already closed!");
        Connection.Close();
        Connection.Dispose();
        Connection = new MySql.Data.MySqlClient.MySqlConnection();
        
    }








    //!! -----------------------------------------------------------------------------------------------------------------------!!//
    //!! ----------------------------------------------- DATABASE METHODS ------------------------------------------------------!!//
    //!! -----------------------------------------------------------------------------------------------------------------------!!//

    /// <summary>
    /// Check if database exists with this name.
    /// </summary>
    /// <param name="databaseName"></param>
    /// <returns>TRUE or FALSE</returns>
    public static bool DatabaseExists(string databaseName) {
        try {
            MySqlCommand myCommand = Connection.CreateCommand();
            myCommand.CommandText = $"SHOW TABLE STATUS FROM `{databaseName}`;";
            var asd = myCommand.ExecuteScalar();
            return true;
        } catch (Exception ex) {
            if (ex == null) return false;
            return false;
        }
    }

    /// <summary>
    /// Creates a new database with given name.
    /// </summary>
    /// <param name="databaseName"></param>
    public static void CreateDatabase(string databaseName) {
        MySqlCommand myCommand = Connection.CreateCommand();
        myCommand.CommandText = $"CREATE DATABASE {databaseName};";
        myCommand.ExecuteScalar();
    }

    /// <summary>
    /// Deletes the database with its given name
    /// </summary>
    /// <param name="databaseName"></param>
    public static void DeleteDatabase(string databaseName) {
        MySqlCommand myCommand = Connection.CreateCommand();
        myCommand.CommandText = $"DROP DATABASE {databaseName};";
        myCommand.ExecuteScalar();
    }









    //!! -----------------------------------------------------------------------------------------------------------------------!!//
    //!! ------------------------------------------------ TABLE METHODS --------------------------------------------------------!!//
    //!! -----------------------------------------------------------------------------------------------------------------------!!//

    /// <summary>
    /// Create empty table with only GUID column available.
    /// </summary>
    /// <param name="tableName"></param>
    // TODO Add ALTERDATA method
    public static void CreateTable(string tableName) {
        Console.WriteLine($"Trying to create a new table... {tableName}");
        //SqlMetaData.TableInfo? tableInfo = GetTableInfo(tableName)!;
        //if (tableInfo == null) throw new Exception($"Table ({tableName}) Not Found!");
        string idName = "Id";
        MySqlCommand myCommand = new MySqlCommand(@$"
            CREATE TABLE `{tableName}` (
                {idName} BINARY(16) NOT NULL, 
                PRIMARY KEY (`{idName}`) USING BTREE
            ) COLLATE='utf8_general_ci' ENGINE=InnoDB;", Connection);
        myCommand.ExecuteScalar();
        Console.WriteLine($"Table created!");

        InitMetaData(); // TODO DO ONLY INTERNALLY
    }

    /// <summary>
    /// Used to create table using object as a reference. Only SET-GET values are saved!
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="tableName"></param>
    public static void CreateTable<T>(string? tableName = null) {
        // TODO update metadata
        List<PropertyInfo> propertyInfos = typeof(T).GetProperties().ToList();
        
        if (tableName == null) {
            tableName = (string)typeof(T).GetField("TableName")?.GetValue(null)!;
            if (tableName == null) tableName = typeof(T).ToString();
        }
        Console.WriteLine($"Trying to create a new table... {tableName}");

        //SqlMetaData.TableInfo? tableInfo = GetTableInfo(tableName)!;
        //if (tableInfo == null) throw new Exception($"Table ({tableName}) Not Found!");
        string idName = "Id";

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
                if (type == "TINYINT(1)") command += $", CHECK ({info.Name}=1 OR {info.Name}=0)"; // Check if boolean and allow 1 and 0 only
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

    /// <summary>
    /// Used to delete table from database. Doesent matter if the table is not empty.
    /// </summary>
    /// <param name="tableName"></param>
    public static void DeleteTable(string tableName) {
        Console.WriteLine($"Trying to delete a table... {tableName}");
        MySqlCommand myCommand = new MySqlCommand($"DROP TABLE {tableName};", Connection);
        myCommand.ExecuteScalar();
        Console.WriteLine($"Table deleted!");
    }
    
    /// <summary>
    /// Returns true if table with that name is found. False if not. 
    /// </summary>
    /// <param name="tableName"></param>
    /// <returns></returns>
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








    //!! -----------------------------------------------------------------------------------------------------------------------!!//
    //!! ---------------------------------------------------- COLUMN METHODS -------------------------------------------------- !!//
    //!! -----------------------------------------------------------------------------------------------------------------------!!//

    /// <summary>
    /// Used to insert / update object's data in the database.
    /// </summary>
    /// <param name="data"></param>
    /// <exception cref="Exception"></exception>
    public static void SetData(object data) { // ALSO USED TO UPDATE DATA
        // TODO Update only the values that have changed!
        Type type = data.GetType();

        //--- Get Table Name Info
        string tableName = (string)type.GetField("TableName")?.GetValue(null)!;
        if (tableName == null) tableName = type.ToString();
        SqlMetaData.TableInfo? tableInfo = GetTableInfo(tableName)!;
        if (tableInfo == null) throw new Exception($"Table ({tableName}) Not Found!");
        string idName = tableInfo.GuidColumnName!;
        

        //--- Get uid and check if UID exists already
        Guid? uid = (Guid)(data.GetType().GetProperties().Single(x => x.Name.ToLower() == idName.ToLower()).GetValue(data))!;
        bool update = (uid != null && TableHasGuid((Guid)uid,tableName));

        //--- Get Columns
        SqlMetaData.ColumnInfo[]? columnInfo = tableInfo.ColumnInfo!;
        if (columnInfo == null || columnInfo.Count() == 0) throw new Exception($"Not metadata found for columnInfo: ({tableName})");
        string[]? columns = columnInfo.Select(x => x.ColumnName).ToArray()!;
        if (columns == null) throw new Exception($"No columns found for table: {tableName}");
        columns = (new string[] { idName }).ToArray().Union(columns).ToArray();

        //--- Parse so that it supports serilization
        string values = String.Join(",@",columns);
        values = "@" + values;

        string command = "";
        if (update) {
            command = $"UPDATE {tableName} SET ";
            for (int i = 0; i != columns.Count(); i++) {
                command += $"{columns[i]}=@{columns[i]}";
                if (i != columns.Count()-1) command += ",";
            }
            command += $" WHERE HEX({idName})='{Convert.ToHexString(((Guid)uid!)!.ToByteArray())}';";
        } else {
            command = $"INSERT INTO {tableName} ({String.Join(",", columns)}) VALUES ({values})";
        }
        MySqlCommand myCommand = new MySqlCommand(command, Connection);

        foreach (PropertyInfo prop in type.GetProperties()) {
            if (!prop.CanWrite || !prop.CanRead) continue;
            object? propValue = prop.GetValue(data);
            if (propValue == null) throw new Exception("ERROR");
            if (prop.Name .ToLower()== idName.ToLower()) propValue = ((Guid)(propValue as Guid?)!).ToByteArray();
            myCommand.Parameters.AddWithValue(("@"+prop.Name),propValue);
        }

        Console.WriteLine($"{command[0..6]} DATA: ({((Guid)uid!).ToString("N")}) - " + JsonSerializer.Serialize(data,new JsonSerializerOptions{IgnoreReadOnlyProperties = true}) + "\n");

        int effect = myCommand.ExecuteNonQuery();
        if (effect != 1) throw new Exception("Something went wrong when trying to insert data!");
    }

    /// <summary>
    /// Can be used to remove data from Table with guid. Only works if the key is stored in the database
    /// </summary>
    /// <param name="tableName"></param>
    /// <param name="uid"></param>
    /// <exception cref="Exception"></exception>
    public static void DeleteData(string tableName, Guid uid) {
        Console.WriteLine("DELETING DATA FOR ID: " + uid);
        string? idName = GetTableInfo(tableName)?.GuidColumnName;
        if (idName == null) throw new Exception("Table Not Found!");

        string hexId = Convert.ToHexString(uid.ToByteArray());
        MySqlCommand myCommand = new MySqlCommand($"DELETE FROM {tableName} WHERE HEX({idName})='{hexId}';", Connection);
        int result = myCommand.ExecuteNonQuery();
        if (result != 1) throw new Exception($"Unable to remove MySQL row WHERE {idName}='{uid.ToByteArray()}'");
        Console.WriteLine("Object deleted succesfully!");
    }


    /// <summary>
    /// Can be used to delete data from table using object that has ID value in it, and that key is stored in the database.
    /// </summary>
    /// <param name="data"></param>
    /// <exception cref="Exception"></exception>
    public static void DeleteData(object data) {
        Type type = data.GetType();
        //--- Get Table Name Info
        string tableName = (string)type.GetField("TableName")?.GetValue(null)!;
        if (tableName == null) tableName = type.ToString();
        SqlMetaData.TableInfo? tableInfo = GetTableInfo(tableName)!;
        if (tableInfo == null) throw new Exception($"Table ({tableName}) Not Found!");
        string idName = tableInfo.GuidColumnName!;

        Guid? uid = (Guid)(data.GetType().GetProperties().Single(x => x.Name.ToLower() == idName.ToLower()).GetValue(data))!;
        if (uid == null) throw new Exception($"Unable to find object ID! {tableName}");
        DeleteData(tableName,(Guid)uid);
    }

    // TODO create whole new object instead
    // public class GetObjectData<T> where T : class, new()
    /// <summary>
    /// Used to get data from column using GUID.
    /// </summary>
    /// <param name="tableName"></param>
    /// <param name="uid"></param>
    /// <returns>Returns a List of values in order from left column to right</returns>
    /// <exception cref="Exception"></exception>
    /// <exception cref="TargetException"></exception>
    public static List<dynamic?>? GetColumnData(string tableName, Guid uid) {
        string? idName = GetTableInfo(tableName)?.GuidColumnName;
        if (idName == null) throw new Exception("Table Not Found!");

        string hexId = Convert.ToHexString(uid.ToByteArray());
        MySqlCommand myCommand = new MySqlCommand($"SELECT * FROM {tableName} WHERE HEX({idName})='{hexId}';", Connection);
        MySqlDataReader reader = myCommand.ExecuteReader();

        List<dynamic?> data = new List<dynamic?>();

        // TODO create new object and add values where columnName is data name...
        // TODO use only Connection.GetSchema()
        bool dataFound = false;
        while (reader.Read()) {
            dataFound = true;
            var dtSchema = reader.GetSchemaTable();
            foreach (DataRow row in dtSchema.Rows) {
                string? columnName = row["ColumnName"]?.ToString()!;
                if (columnName.ToLower() == idName.ToLower()) continue;

                data.Add(reader.GetValue(columnName));
            }
        }
        reader.Close();
        if (!dataFound) throw new TargetException($"NO DATA FOUND FROM TABLE: {tableName}, USING GUID: {uid.ToString("N")}");
        
        return data;
    }


    public static void RestoreData(this object dataNew, Guid uid) {
        Type type = dataNew.GetType();

        string tableName = (string)type.GetField("TableName")?.GetValue(dataNew)!;
        if (tableName == null) tableName = type.ToString();

        string? idName = GetTableInfo(tableName)?.GuidColumnName;
        if (idName == null) throw new Exception("Table Not Found!");

        string hexId = Convert.ToHexString(uid.ToByteArray());
        MySqlCommand myCommand = new MySqlCommand($"SELECT * FROM {tableName} WHERE HEX({idName})='{hexId}';", Connection);
        MySqlDataReader reader = myCommand.ExecuteReader();

        bool idSet = false;
        while (reader.Read()) {
            var dtSchema = reader.GetSchemaTable();

            foreach (DataRow row in dtSchema.Rows) {
                string? columnName = row["ColumnName"]?.ToString()!;
                PropertyInfo? property = type.GetProperty(columnName);
                if (columnName.ToLower() == idName.ToLower()) {
                    if (property == null) throw new Exception($"UNABLE TO RESTORE UID FOR OBJECT: {uid}");
                    property.SetValue(dataNew, uid, null);
                    idSet = true;
                    continue;
                }
                if (property == null) continue;

                var data = reader.GetValue(columnName);
                property.SetValue(dataNew, data, null);
            }
        }
        reader.Close();

        if (!idSet) throw new Exception($"UNABLE TO RESTORE UID FOR OBJECT: {uid}");
    }








    //!! -----------------!!//
    //!! METADATA METHODS !!//
    //!! -----------------!!//

    /// <summary>
    /// Can be used to detect if the guid is found from the table as ID
    /// </summary>
    /// <param name="uid"></param>
    /// <param name="tableName"></param>
    /// <returns></returns>
    /// <exception cref="Exception"></exception>
    public static bool TableHasGuid(Guid uid, string tableName) {
        SqlMetaData.TableInfo? tableInfo = GetTableInfo(tableName)!;
        if (tableInfo == null) throw new Exception($"Table ({tableName}) Not Found!");
        string idName = tableInfo.GuidColumnName!;

        string hexId = Convert.ToHexString(uid.ToByteArray());
        MySqlCommand myCommand = new MySqlCommand(@$"SELECT {idName} FROM {tableName} WHERE HEX({idName})='{hexId}';", Connection);
        var found = myCommand.ExecuteScalar();
        return (found != null);
    }

    /// <summary>
    /// Returns TableInfo class that contains the metadata for the wanted table
    /// </summary>
    /// <param name="tableName"></param>
    /// <returns></returns>
    public static SqlMetaData.TableInfo? GetTableInfo(string tableName) {
        return SqlMetaData.TableInfoData?[tableName.ToLower()]!;
    }

    /// <summary>
    /// Returns array of string of tables in the currently selected database.
    /// </summary>
    /// <returns>database table names</returns>
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

    
    private static void InitMetaData() {
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


    private static Type GetDataType(string typeText) {
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
        // Returns System.Object instead of NULLABLE`1(System.Object) if nulled
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










//!! ---------------------!!//
//!! CLASSES FOR METADATA !!//
//!! ---------------------!!//


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
        public bool? Nullable { get; set; } //TODO Always true for now when generated automatically
        public Type? Type { get; set; }
        public bool Signed { get; set; }
    }

}


/// <summary>
/// Used to create new object classes that can be registered to Database.
/// </summary>
public abstract class MySqlBaseObjectClass {
    internal int LastHashCode;
    public Guid Id { get; init; } = Guid.NewGuid();
    public static string? TableName { get; internal set; }
    public MySqlBaseObjectClass() {
        TableName = this.GetType().UnderlyingSystemType.Name;
        LastHashCode = this.GetHashCode();
    }

    // To check if data has been altered in object
    /// <summary>
    /// Get the hash code from all properties and combine to get total hash of all values.
    /// </summary>
    /// <returns></returns>
    public override int GetHashCode() {
        int hashFinal = base.GetHashCode();
        foreach(PropertyInfo info in this.GetType().GetProperties()) {
            if (!info.CanRead || !info.CanWrite) continue;
            int? tempHash = info.GetValue(this)?.GetHashCode();
            if (tempHash != null) hashFinal += (int)tempHash;
        }
        return hashFinal;
    }
}