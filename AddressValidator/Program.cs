using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection.Emit;
using System.Text.RegularExpressions;

namespace AddressValidator
{
    class Program
    {
        //const string FILE = @"D:\Work Experience\AddressValidator\20200515_AddressExamples.txt";
        const string FILE = @"D:\Work Experience\AddressValidator\Addresses for Test.csv";
        static void Main(string[] args)
        {
            //if (args.Length > 0) ProcessFile();
            //else 
            ProcessDB();
        }

        private static void ProcessDB()
        {
            var addresses = Database.GetAddresses();
            DiskLog.CreateFile();
            System.Diagnostics.Stopwatch watch = System.Diagnostics.Stopwatch.StartNew();
            Database.GetAddressIds(addresses);
            //Database.UpdateAddressList(addresses);
            Console.WriteLine($"Execution Time: {watch.ElapsedMilliseconds} ms");
            Console.ReadLine();
        }


        //private static void ProcessFile()
        //{
        //    string[] lines = File.ReadAllLines(FILE).Skip(2).ToArray();
        //    DiskLog.CreateFile();
        //    System.Diagnostics.Stopwatch watch = System.Diagnostics.Stopwatch.StartNew();
        //    foreach (string line in lines)
        //    {
        //        string[] cols = line.Split(',');
        //        string state = RemoveSpaces(cols[4]);
        //        string locality = RemoveSpaces(cols[3]);
        //        (string streetName, string streetNumber) = StreetName(cols[1]);

        //        if (!string.IsNullOrEmpty(state) && !string.IsNullOrEmpty(locality) && !string.IsNullOrEmpty(streetName) && !string.IsNullOrEmpty(streetNumber))
        //        {
        //            System.Diagnostics.Stopwatch each = System.Diagnostics.Stopwatch.StartNew();
        //            //List<string> addressIds = Database.GetAddressId(state, locality, streetName, streetNumber);
        //            each.Stop();

        //            if (addressIds != null && addressIds.Count > 0) Console.WriteLine($"{addressIds.Count} Matches - {each.ElapsedMilliseconds} ms");
        //            else
        //            {
        //                Console.WriteLine($"{line} - {each.ElapsedMilliseconds} ms");
        //                DiskLog.WriteLog(line);
        //            }
        //        }
        //    }
        //    watch.Stop();
        //    Console.WriteLine($"Execution Time: {watch.ElapsedMilliseconds} ms");
        //    Console.ReadLine();
        //}
    }
}
