using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace AddressValidator
{
    class Program
    {
        //const string FILE = @"D:\Work Experience\AddressValidator\20200515_AddressExamples.txt";
        const string FILE = @"D:\Work Experience\AddressValidator\NotFound.txt";
        static void Main()
        {
            string[] lines = File.ReadAllLines(FILE).Skip(1).ToArray();
            DiskLog.CreateFile();
            System.Diagnostics.Stopwatch watch = System.Diagnostics.Stopwatch.StartNew();
            foreach (string line in lines)
            {
                string[] cols = line.Split(',');
                string state = RemoveSpaces(cols[4]);
                string locality = RemoveSpaces(cols[3]);
                (string streetName, string streetNumber) = StreetName(cols[1]);

                if (!string.IsNullOrEmpty(state) && !string.IsNullOrEmpty(locality) && !string.IsNullOrEmpty(streetName) && !string.IsNullOrEmpty(streetNumber))
                {
                    System.Diagnostics.Stopwatch each = System.Diagnostics.Stopwatch.StartNew();
                    List<string> addressIds = Database.GetAddressId(state, locality, streetName, streetNumber);
                    each.Stop();

                    if (addressIds != null && addressIds.Count > 0) Console.WriteLine($"{addressIds.Count} Matches - {each.ElapsedMilliseconds} ms");
                    else
                    {
                        Console.WriteLine($"{line} - {each.ElapsedMilliseconds} ms");
                        DiskLog.WriteLog(line);
                    }
                }
            }
            watch.Stop();
            Console.WriteLine($"Execution Time: {watch.ElapsedMilliseconds} ms");
            Console.ReadLine();
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
