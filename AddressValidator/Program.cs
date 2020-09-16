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
            int count = int.MaxValue;
            while (count > 0)
            {
                var addresses = Database.GetAddresses();
                count = addresses.Count;
                DiskLog.CreateFile();
                System.Diagnostics.Stopwatch watch = System.Diagnostics.Stopwatch.StartNew();
                var t2 = Task.Run(async () => 
                {
                    await Database.GetAddressIds(addresses);
                    Console.WriteLine($"Execution Time: {watch.ElapsedMilliseconds} ms");
                    await Database.UpdateAddressListAsync(addresses); 
                });
                t2.Wait();
                Console.WriteLine($"Execution Time: {watch.ElapsedMilliseconds} ms");
                //count = 0;
            }
            Console.WriteLine($"Process complete, press enter key to exit.");
            Console.ReadLine();
        }
    }
}
