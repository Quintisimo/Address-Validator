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
                try
                {
                    var addresses = Database.GetAddresses(ref count);
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
                }
                catch (Exception e)
                {
                    PrintError(e, ref count);
                }
            }
            Console.WriteLine($"Process complete, press enter key to exit.");
            Console.ReadLine();
        }

        private static void PrintError(Exception e, ref int count)
        {
            Console.WriteLine(e.Message);
            Console.Write("Restart [R] or Exit [E]: ");
            var input = Console.ReadLine().ToLower()[0];
            Console.WriteLine("");

            if (input == 'r')
            {
                count = 0;
            }
            else if (input == 'e')
            {
                Environment.Exit(1);
            }
            else
            {
                Console.WriteLine("\nPlease enter a valid option");
                PrintError(e, ref count);
            }

        }
    }
}
