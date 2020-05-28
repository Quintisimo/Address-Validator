using System;
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
            foreach (string line in lines)
            {
                string[] cols = line.Split('\t');
                HouseNumber(cols[0]);
                Console.WriteLine($"Suburb: {cols[1]}");
                Console.WriteLine($"State: {cols[2]}");
                Console.WriteLine($"PostCode: {cols[3]}");
                Console.WriteLine($"Country: {cols[4]}");
            }
            Console.ReadLine();
        }

        static void HouseNumber(string streetCombined)
        {
            Match numberCheck = Regex.Match(streetCombined, @"(.*\d[a-z]?)");

            if (numberCheck.Success)
            {
                Console.WriteLine($"House Number: {numberCheck.Value}");
                Console.WriteLine($"Street Name: {streetCombined.Substring(numberCheck.Index + numberCheck.Length)}");
            }
            else
            {
                Console.WriteLine("");
            }
        }
    }
}
