using System.IO;
using System.Text;

namespace AddressValidator
{
    class DiskLog
    {
        private const string FILENAME = @"D:\Work Experience\AddressValidator\Missed.txt";

        /// Write log to file
        /// </summary>
        /// <param name="log">log row</param>
        public static void WriteLog(string log)
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

            using (StreamWriter fsAppend = File.AppendText(FILENAME))
            {
                fsAppend.WriteLine(log);
            }
        }
    }
}
