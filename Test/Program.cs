﻿using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test
{
    class Program
    {
        static void Main(string[] args)
        {
            var source = new SqlString("hell");
            var target = new SqlString("worldssssssss");
            Console.WriteLine(MathFunctions.Distance(source, target));
            Console.ReadLine();
        }
    }
}
