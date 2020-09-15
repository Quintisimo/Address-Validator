using System;
using System.Threading.Tasks;

namespace AddressValidator
{
    class Program
    {
        static void Main(string[] args)
        {
            ProcessDB();
        }

        private static void ProcessDB()
        {
            var t = Task.Run(async () => await Database.GetDBData());
            t.Wait();
            int i = 0;
            while (i < 15)
            {
                i++;
                var addresses = Database.GetAddresses();
                DiskLog.CreateFile();
                System.Diagnostics.Stopwatch watch = System.Diagnostics.Stopwatch.StartNew();
                var t2 = Task.Run(async () => await Database.GetAddressIds(addresses));
                t2.Wait();
                Console.WriteLine($"Execution Time: {watch.ElapsedMilliseconds} ms");
                Database.UpdateAddressList(addresses);
                Console.WriteLine($"Total Save + Execution Time: {watch.ElapsedMilliseconds} ms");
            }
            Console.ReadLine();
        }
    }
}
