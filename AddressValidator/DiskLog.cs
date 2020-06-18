using System.IO;

namespace AddressValidator
{
    class DiskLog
    {
        private const string FILENAME = @"D:\Work Experience\AddressValidator\Missed.txt";

        /// <summary>
        /// Write log to file
        /// </summary>
        /// <param name="log">log row</param>
        public static void WriteLog(string log)
        {

            if (!File.Exists(FILENAME))
            {
                File.Create(FILENAME);
            }

            using (StreamWriter fsAppend = File.AppendText(FILENAME))
            {
                fsAppend.WriteLine(log);
            }
        }
    }
}
