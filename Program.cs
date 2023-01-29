using System.Data;
using System.Reflection;
using System.Text.Json;
using MySql.Data.MySqlClient;

// TODO add scheduler for execute command so no new scoket cant be running while another is executing


public partial class Program {
    public static MySqlConnection Connection = new MySql.Data.MySqlClient.MySqlConnection();
    public static string DatabaseName = "test";
    public static async Task Main(string[] args) {
        Console.Write("Connecting to Database...");
        try {
            Connection.ConnectionString = @"server=192.168.1.4;uid=test;pwd=test";
            await Connection.OpenAsync();
            Console.Write("\t*SUCCESS*\n");

            if (!DatabaseExists()) {
                Console.WriteLine("Database not found, Creating new...");
                CreateDatabase(); // Create new database if one doesent already existDataColumn
            }
            Connection.ChangeDatabase(DatabaseName); // Select Database
            
            if (!TableExists("test")) CreateTable("test");
            InitMetaData();


            SqlMetaData.TableInfo? info = GetTableInfo("test")!;
            Console.WriteLine("UsesGuid: " + info?.UsesGuid!);

            foreach (SqlMetaData.ColumnInfo? dataInfo in info?.ColumnInfo!) {
                Console.Write(dataInfo.ColumnName);
                Console.WriteLine(": " + dataInfo.Type);
            }
            Console.WriteLine();

            if (!TableExists("students")) CreateTable<Student>();

            Guid uid = new Guid("0a69f3dc-1256-435b-8919-5b42cc73f573"); // IN DATABASE!

            
            Student testData = new Student();
            testData.BirthDate = new DateTime(1999,04,26);
            testData.Name = "Vaino";
            testData.Money = 27500f;
            testData.Alive = false;
            Guid tmp = testData.Id;
            InsertData(testData);
            

            Student restoredData = new Student(tmp);
            Console.WriteLine($"[RESTORED] Id:{restoredData.Id}, Name:{restoredData.Name}, BirthDate:{restoredData.BirthDate}, Money:{restoredData.Money}, Alive:{restoredData.Alive}");
            Console.WriteLine("AGE: " + restoredData.Age);
            Console.WriteLine();

            var rnd = new Random();
            restoredData.Name = "MAURI " + rnd.Next().ToString();
            restoredData.Alive = true;
            InsertData(restoredData);

            Console.WriteLine();
            Console.WriteLine("PRESS ENTER TO DELETE ROW");
            Console.ReadLine(); // Wait for delete...
            DeleteData(restoredData);
        } catch (MySqlException ex) {
            Console.WriteLine(ex);
        }
    }




    // TODO create whole new object
    public static List<dynamic?>? GetColumnData(string tableName, Guid uid) {
        string? idName = GetTableInfo(tableName)?.GuidColumnName;
        if (idName == null) throw new Exception("Table Not Found!");

        string hexId = Convert.ToHexString(uid.ToByteArray());
        MySqlCommand myCommand = new MySqlCommand($"SELECT * FROM {tableName} WHERE HEX({idName})='{hexId}';", Connection);
        MySqlDataReader reader = myCommand.ExecuteReader();

        List<dynamic?> data = new List<dynamic?>();

        // TODO create new object and add values where columnName is data name...
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


    public static void InsertData(object data) {
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

        Console.WriteLine($"{command[0..6]} DATA: \t" + JsonSerializer.Serialize(data) + "\n");

        int effect = myCommand.ExecuteNonQuery();
        if (effect != 1) throw new Exception("Something went wrong when trying to insert data!");
    }
    
    
    public static bool TableHasGuid(Guid uid, string tableName) {
        SqlMetaData.TableInfo? tableInfo = GetTableInfo(tableName)!;
        if (tableInfo == null) throw new Exception($"Table ({tableName}) Not Found!");
        string idName = tableInfo.GuidColumnName!;

        string hexId = Convert.ToHexString(uid.ToByteArray());
        MySqlCommand myCommand = new MySqlCommand(@$"SELECT {idName} FROM {tableName} WHERE HEX({idName})='{hexId}';", Connection);
        var found = myCommand.ExecuteScalar();
        return (found != null);
    }

    
    // TODO
    // TODO
    public static void DeleteData(string tableName, Guid uid) {
        Console.WriteLine("DELETING DATA FOR ID: " + uid);
        string? idName = GetTableInfo(tableName)?.GuidColumnName;
        if (idName == null) throw new Exception("Table Not Found!");

        string hexId = Convert.ToHexString(uid.ToByteArray());
        MySqlCommand myCommand = new MySqlCommand($"DELETE FROM {tableName} WHERE HEX({idName})='{hexId}';", Connection);
        int result = myCommand.ExecuteNonQuery();
        if (result != 1) throw new Exception($"Unable to remove MySQL row WHERE {idName}='{uid.ToByteArray()}'");
    }
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
}





public class Student : MySqlObjectClass
{
    public new static string TableName = "students"; // Override
    public string? Name { get; set; }
    public DateTime? BirthDate { get; set; }
    public float Money { get; set; }
    public bool Alive { get; set; } = false;

    public int Age { get {return DateTime.Now.Year - this.BirthDate!.Value.Year;} }

    public Student() {
        this.Id = Guid.NewGuid();
    }
    public Student(Guid id) {
        // Restore from DB
        List<dynamic>? data = Program.GetColumnData(TableName, id)!;
        this.Id = id;
        this.Name = data[0];
        this.BirthDate = data[1];
        this.Money = data[2];
    }
}
 
// GET TABLES LIST: show tables like '%'

// COLLUMS: SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME =N'test';

