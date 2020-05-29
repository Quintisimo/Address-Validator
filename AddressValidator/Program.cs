using System;
using System.Data.SqlClient;
using System.IO;
using System.Text.RegularExpressions;

namespace AddressValidator
{
    class Program
    {
        const string FILE = @"D:\Work Experience\AddressValidator\20200515_AddressExamples.txt";
        static void Main(string[] args)
        {
            string[] lines = File.ReadAllLines(FILE);
            SqlConnection db = Database.Connect();
            foreach (string line in lines)
            {
                string[] cols = line.Split('\t');
                bool validPostcode = Database.CheckValid(Database.FieldName.Postcode, cols[3], db);
                bool validState = Database.CheckValid(Database.FieldName.State, cols[2], db);
                bool locality = Database.CheckValid(Database.FieldName.Locality, cols[1], db);
                bool validStreet = HouseNumber(cols[0], db);
            }
            db.Close();
            Console.ReadLine();
        }

        static bool HouseNumber(string streetCombined, SqlConnection db)
        {
            Match numberCheck = Regex.Match(streetCombined, @"(.*\d[a-z]?)");

            if (numberCheck.Success)
            {
                string streetName = streetCombined.Substring(numberCheck.Index + numberCheck.Length);
                bool validState = Database.CheckValid(Database.FieldName.StreetName, streetName, db);
                Console.WriteLine($"{streetName} {validState}");
                return validState;
            }
            return false;
        }
    }
}
