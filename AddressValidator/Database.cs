using System;
using System.Data.SqlClient;
using System.Configuration;
using System.Data;

namespace SyslogReceiver
{
    class Database
    {
        /// <summary>
        /// Connect to database
        /// </summary>
        /// <returns>database object</returns>
        private static SqlConnection Connect()
        {
            ConnectionStringSettings settings = ConfigurationManager.ConnectionStrings["DB Connection"];
            SqlConnection db = new SqlConnection(settings.ConnectionString);
            try
            {
                db.Open();

            }
            catch (Exception e)
            {
                Console.WriteLine($"Connection Error: {e.Message}");
            }
            return db;

        }
    }
}
