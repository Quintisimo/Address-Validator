using System.IO;
using System.Text;

namespace AddressValidator
{
    class DiskLog
    {
        private const string FILENAME = @"D:\Work Experience\AddressValidator\Missed.txt";

        /// <summary>
        /// Create log file
        /// </summary>
        public static void CreateFile()
        {
            if (!File.Exists(FILENAME))
            {
                File.Delete(FILENAME);
            }

            using (FileStream fs = File.Create(FILENAME))
            {
                byte[] info = new UTF8Encoding(true).GetBytes("Cannot Find\n");
                fs.Write(info, 0, info.Length);
            }
        }

        /// Write log to file
        /// </summary>
        /// <param name="log">log row</param>
        public static void WriteLog(string log)
        {
            using (StreamWriter fsAppend = File.AppendText(FILENAME))
            {
                fsAppend.WriteLine(log);
            }
        }
    }
}
