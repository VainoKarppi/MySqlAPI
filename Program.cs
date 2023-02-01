using System.Data;
using System.Reflection;
using System.Text.Json;
using MySql.Data.MySqlClient;

using MySqlDatabaseAPI;
using static MySqlDatabaseAPI.Database;


// TODO add scheduler for execute command so no new scoket cant be running while another is executing


public class Program {
    public static void Main(string[] args) {
        try {
            //ConnectToDatabase("192.168.1.4","test","test","test",true);
            Console.WriteLine("WARNING: Cannot connect from XAMK network!");
            ConnectToDatabase("karppi2.asuscomm.com","testdatabase","test","test",true); // Open database for develop (Might remove later)
            
            if (!TableExists(Student.TableName)) CreateTable<Student>();
           
            Student testData = new Student();
            Guid uid = testData.Id;

            testData.BirthDate = new DateTime(1999,04,26);
            testData.Name = "Firstname Lastname";
            testData.Money = 27500.60f;
            testData.Alive = true;

            SetData(testData);

            Student restoredData = new Student(uid);
            Console.WriteLine($"[RESTORED DATA] Id:{restoredData.Id}, Name:{restoredData.Name}, BirthDate:{restoredData.BirthDate}, Money:{restoredData.Money}, Alive:{restoredData.Alive}");
            Console.WriteLine("AGE: " + restoredData.Age + "\nOLD HASH: " + restoredData.GetHashCode());
            
            restoredData.Money = 15500.35f;
            restoredData.Alive = false;
            SetData(restoredData);

            Console.WriteLine("NEW HASH: " + restoredData.GetHashCode());

            Console.WriteLine();
            Console.WriteLine(@"TYPE ""Y"" TO DELETE THIS ROW");

            string? del = Console.ReadLine(); // Wait for delete...
            if (del!.Equals("y",StringComparison.InvariantCultureIgnoreCase)) DeleteData(restoredData);

        } catch (MySqlException ex) {
            if (ex is MySqlException)
            Console.WriteLine("\nERROR: " + ex.Message + "\n\n");
            Console.WriteLine(ex);
        }
    }
}





public class Student : MySqlBaseObjectClass
{
    // MySqlObjectClass always has the following properties:
    //  - Id = When new object is generated it gets random guid inserted into it
    //  - TableName = If not overwritten, class name will be used.
    //  - LastHashCode = Can be used to detect if data in class properties has been altered when comparing to myObject.GetHashCode()
    public new static string TableName = "students"; // Override over (Student) name for table

    //--- These Values Are Saved To DB!
    public string? Name { get; set; }
    public DateTime? BirthDate { get; set; }
    public float Money { get; set; }
    public bool Alive { get; set; } = false;



    // Methods that have both get and set can be only saved to DB!
    public int Age { get { return DateTime.Now.Year - this.BirthDate!.Value.Year; } }

    public Student() {}
    public Student(Guid id) {
        Database.RestoreData(this,id); // CALL FOR RESTORE DATA AUTOMATICALLY
    }
}


