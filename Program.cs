using System.Data;
using System.Reflection;
using System.Text.Json;
using MySql.Data.MySqlClient;

using static MySqlDatabaseAPI.Database;

// TODO add scheduler for execute command so no new scoket cant be running while another is executing


public class Program {
    public static void Main(string[] args) {
        try {
            ConnectToDatabase("192.168.1.4","test","test","test",true);

            if (!TableExists(Student.TableName)) CreateTable<Student>();

            Guid temp = new Guid("0a69f3dc-1256-435b-8919-5b42cc73f573"); // TEMP UID FOR DEVELOP THAT IS ALWAYS IN DATABASE!
           
            Student testData = new Student();
            Guid uid = testData.Id;

            testData.BirthDate = new DateTime(1999,04,26);
            testData.Name = "Lauri";
            testData.Money = 27500f;
            testData.Alive = true;

            SetData(testData);

            Student restoredData = new Student(uid);
            Console.WriteLine($"[RESTORED DATA] Id:{restoredData.Id}, Name:{restoredData.Name}, BirthDate:{restoredData.BirthDate}, Money:{restoredData.Money}, Alive:{restoredData.Alive}");
            Console.WriteLine("AGE: " + restoredData.Age);
            Console.WriteLine("OLD HASH: " + restoredData.GetHashCode());
            
            restoredData.Money = 15500f;
            restoredData.Alive = false;
            SetData(restoredData);

            Console.WriteLine("NEW HASH: " + restoredData.GetHashCode());

            Console.WriteLine();
            Console.WriteLine(@"TYPE ""Y"" TO DELETE THIS ROW");
            
            string? del = Console.ReadLine(); // Wait for delete...
            if (del!.Equals("y",StringComparison.InvariantCultureIgnoreCase)) DeleteData(restoredData);

        } catch (MySqlException ex) {
            Console.WriteLine("ERROR: " + ex.Message + "\n");
            Console.WriteLine(ex);
        }
    }
}





public class Student : MySqlDatabaseAPI.MySqlBaseObjectClass
{
    // MySqlObjectClass always has the following properties:
    //  Id = When new object is generated it gets random guid inserted into it
    //  TableName = If not overwritten, class name will be used.
    //  LastHashCode = Can be used to detect if data in class properties has been altered when comparing to myObject.GetHashCode()
    public new static string TableName = "students"; // Override over (Student)
    public string? Name { get; set; }
    public DateTime? BirthDate { get; set; }
    public float Money { get; set; }
    public bool Alive { get; set; } = false;

    // Methods that have both get and set can be only saved to DB!
    public int Age { get {return DateTime.Now.Year - this.BirthDate!.Value.Year;} }

    public Student() {}
    public Student(Guid id) {
        //TODO Restore from DB automatically
        List<dynamic>? data = GetColumnData(TableName, id)!;
        this.Id = id;
        this.Name = data[0];
        this.BirthDate = data[1];
        this.Money = data[2];
    }
}
 
// GET TABLES LIST: show tables like '%'

// COLLUMS: SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME =N'test';

