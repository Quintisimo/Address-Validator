using System;
using System.Data.SqlTypes;

namespace Test
{
    class Program
    {
        static void Main(string[] args)
        {
            var source = new SqlString("JEANHULLEY");
            var target = new SqlString("Jeanhuller");
            Console.WriteLine(MathFunctions.Distance(source, target));
            Console.ReadLine();
        }
    }
}
