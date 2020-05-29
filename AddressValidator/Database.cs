using System;
using System.Data.SqlClient;
using System.Configuration;
using System.Data;
using System.Linq;

namespace AddressValidator
{
    class Database
    {
        public enum FieldName
        {
            Postcode,
            State,
            Locality,
            StreetName
        }

        /// <summary>
        /// Connect to database
        /// </summary>
        /// <returns>database object</returns>
        public static SqlConnection Connect()
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

        public static bool CheckValid(FieldName field, string value, SqlConnection db)
        {
            if (field == FieldName.Postcode)
            {
                string queryStatement = @"SELECT count(address_detail_pid) FROM [PSMA_G-NAF].[dbo].[ADDRESS_DETAIL] WHERE postcode = @postcode";
                SqlCommand query = new SqlCommand(queryStatement, db);
                query.Parameters.Add(new SqlParameter("@postcode", SqlDbType.Int) { Value = value });
                return checkRows(query);
            }
            else if (field == FieldName.State)
            {
                string queryStatement = @"SELECT count(state_pid) FROM [PSMA_G-NAF].[dbo].[STATE] WHERE DIFFERENCE(state_abbreviation, @state) > 2";
                SqlCommand query = new SqlCommand(queryStatement, db);
                query.Parameters.Add(new SqlParameter("@state", SqlDbType.NVarChar) { Value = value });
                return checkRows(query);
            }
            else if (field == FieldName.Locality)
            {
                string queryStatement = @"SELECT count(locality_pid) FROM [PSMA_G-NAF].[dbo].[LOCALITY] WHERE DIFFERENCE(locality_name, @locality) = 4";
                SqlCommand query = new SqlCommand(queryStatement, db);
                query.Parameters.Add(new SqlParameter("@locality", SqlDbType.NVarChar) { Value = value });
                return checkRows(query);
            }
            else if (field == FieldName.StreetName)
            {
                string streetType = value.Split(' ').Last();
                string queryStatement = @"SELECT count(street_locality_pid) FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY] WHERE DIFFERENCE(street_name, @name) = 4 AND street_type_code LIKE '%@type%'";
                SqlCommand query = new SqlCommand(queryStatement, db);
                query.Parameters.Add(new SqlParameter("@name", SqlDbType.NVarChar) { Value = value });
                query.Parameters.Add(new SqlParameter("@type", SqlDbType.NVarChar) { Value = streetType });
                return checkRows(query);
            }
            return false;
        }

        private static bool checkRows(SqlCommand query)
        {
            try
            {
                int count = (int)query.ExecuteScalar();
                Console.WriteLine(count);

                if (count > 0)
                {
                    return true;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            return false;
        }
    }
}
