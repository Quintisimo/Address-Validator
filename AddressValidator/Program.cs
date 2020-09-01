﻿using System;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace AddressValidator
{
    class Program
    {
        //const string FILE = @"D:\Work Experience\AddressValidator\20200515_AddressExamples.txt";
        const string FILE = @"D:\Work Experience\AddressValidator\NotFound.txt";
        const string CSV_FILE = @"D:\Work Experience\AddressValidator\NotFoundCSV.txt"; 
        static void Main()
        {
            //string[] lines = File.ReadAllLines(FILE).Skip(1).ToArray();
            //SqlConnection db = Database.Connect();
            //DiskLog.CreateFile();
            //foreach (string line in lines)
            //{
            //    string[] cols = line.Split('\t');
            //    string state = RemoveSpaces(cols[2]);
            //    string locality = RemoveSpaces(cols[1]);
            //    (string streetName, string streetNumber) = StreetName(cols[0]);

            //    if (state != null && locality != null && streetName != null)
            //    {
            //        string addressId = Database.GetAddressId(state, locality, streetName, streetNumber, db);
            //        if (addressId != null) Console.WriteLine(addressId);
            //        else
            //        {
            //            Console.WriteLine(line);
            //            DiskLog.WriteLog(line);
            //        }
            //    }
            //}
            //db.Close();
            CSVFile();
            Console.WriteLine("DONE");
        }

        static void CSVFile()
        {
            string[] lines = File.ReadAllLines(CSV_FILE).Skip(1).ToArray();
            DiskLog.CreateFile();
            foreach (string line in lines)
            {
                string[] cols = line.Split(',');
                string state = RemoveSpaces(cols[4]);
                string locality = RemoveSpaces(cols[3]);
                (string streetName, string streetNumber) = StreetName(cols[1]);

                if (!string.IsNullOrEmpty(state) && !string.IsNullOrEmpty(locality) && !string.IsNullOrEmpty(streetName) && !string.IsNullOrEmpty(streetNumber))
                {
                    string addressId = Database.GetAddressId(state, locality, streetName, streetNumber);
                    if (addressId != null) Console.WriteLine(addressId);
                    else
                    {
                        Console.WriteLine(line);
                        DiskLog.WriteLog(line);
                    }
                }
            }
        }


        /// <summary>
        /// Extract street name from address
        /// </summary>
        /// <param name="streetCombined">street name with house/unit number</param>
        /// <returns>street name if found otherwise null</returns>
        private static (string, string) StreetName(string streetCombined)
        {
            Match numberCheck = Regex.Match(streetCombined, @"\d+");
            Match streetNumber = Regex.Match(streetCombined, @"(.*)\d+[A-z]?");
            Match postbpox = Regex.Match(streetCombined, @"P\.*O\.* BOX", RegexOptions.IgnoreCase);

            if (!numberCheck.Success) return (streetCombined, streetNumber.Value);
            if (streetNumber.Success && !postbpox.Success) return (RemoveSpaces(streetCombined.Substring(streetNumber.Index + streetNumber.Length).Trim()), streetNumber.Value);
            return (null, streetNumber.Value);
        }

        /// <summary>
        /// Remove muiltiple spaces in words and replace with a single space
        /// </summary>
        /// <param name="str">string</param>
        /// <returns>formated string</returns>
        private static string RemoveSpaces(string str) => Regex.Replace(str, @"\s+|/", @" ").Trim();
    }
}
