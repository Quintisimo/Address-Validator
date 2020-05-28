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

        public static Boolean checkState(string state)
        {
            SqlConnection db = Connect();
            SqlCommand stateQuery = new SqlCommand(@"SELECT count(state_pid) FROM [PSMA_G-NAF].[dbo].[STATE] WHERE state_abbreviation = @state", db);
            stateQuery.Parameters.Add(new SqlParameter("@state", SqlDbType.NVarChar) { Value = state });

            try
            {
                int count = (int)stateQuery.ExecuteScalar();

                if (count > 0)
                {
                    return true;
                }
            }
            catch (Exception)
            {
                Console.WriteLine("Error checking state");
            }
            return false;
        }
    }
}
