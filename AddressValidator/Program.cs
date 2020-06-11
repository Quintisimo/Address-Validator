using System;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace AddressValidator
{
    class Program
    {
        const string FILE = @"D:\Work Experience\AddressValidator\20200515_AddressExamples.txt";
        static void Main(string[] args)
        {
            string[] lines = File.ReadAllLines(FILE).Skip(1).ToArray();
            SqlConnection db = Database.Connect();
            int foundNumber = 0;
            foreach (string line in lines)
            {
                string[] cols = line.Split('\t');
                bool validPostcode = Database.CheckValid(Database.FieldName.Postcode, cols[3], db);
                bool validState = Database.CheckValid(Database.FieldName.State, cols[2], db);
                bool validLocality = Database.CheckValid(Database.FieldName.Locality, cols[1], db);
                string streetName = StreetName(cols[0].Replace("\"", string.Empty));
                bool validStreet = Database.CheckValid(Database.FieldName.StreetName, streetName, db);

                if (validPostcode && validState && validLocality && validStreet)
                {
                    Console.WriteLine(line);
                    string streetLocalityId = Database.GetStreetLocalityId(cols[2], cols[1], streetName, db);

                    if (streetLocalityId != null)
                    {
                        Console.WriteLine(streetLocalityId);
                        foundNumber++;
                    }
                    else
                    {
                        Console.WriteLine("Not Found");
                    }
                }
                else
                {
                    Console.WriteLine(line);
                    Console.WriteLine("Not Found");
                }
                Console.WriteLine("");
            }
            db.Close();
            Console.ReadLine();
        }

        /// <summary>
        /// Extract street name from address
        /// </summary>
        /// <param name="streetCombined">street name with house/unit number</param>
        /// <returns>street name if found otherwise null</returns>
        static string StreetName(string streetCombined)
        {
            Match numberCheck = Regex.Match(streetCombined, @"\d+");
            Match streetNumber = Regex.Match(streetCombined, @"(.*)\d+[A-z]?");
            Match postbpox = Regex.Match(streetCombined, @"P\.*O\.* BOX", RegexOptions.IgnoreCase);


            if (!numberCheck.Success)
            {
                return streetCombined;
            }

            if (streetNumber.Success && !postbpox.Success)
            {
                return streetCombined.Substring(streetNumber.Index + streetNumber.Length).Trim();
            }
            return null;
        }
    }
}
