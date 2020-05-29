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
        private static bool checkRows(SqlCommand query)
        {
            try
            {
                int count = (int)query.ExecuteScalar();

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
                string queryStatement = @"SELECT count(street_locality_pid) FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY] WHERE DIFFERENCE(street_name, @name) = 4 AND street_type_code LIKE '%' + @type + '%'";
                SqlCommand query = new SqlCommand(queryStatement, db);
                query.Parameters.Add(new SqlParameter("@name", SqlDbType.NVarChar) { Value = value });
                query.Parameters.Add(new SqlParameter("@type", SqlDbType.NVarChar) { Value = streetType });
                return checkRows(query);
            }
            return false;
        }

        public static string GetStreetLocalityId(string state, string locality, string streetName, SqlConnection db)
        {
            string stateIdStatement = @"SELECT TOP(1) state_pid FROM [PSMA_G-NAF].[dbo].[STATE] 
                                      WHERE DIFFERENCE(state_abbreviation, @state) > 2 
                                      ORDER BY DIFFERENCE(state_abbreviation, @state) DESC";
            SqlCommand stateIdQuery = new SqlCommand(stateIdStatement, db);
            stateIdQuery.Parameters.Add(new SqlParameter("@state", SqlDbType.NVarChar) { Value = state });
            int stateId = Convert.ToInt32(stateIdQuery.ExecuteScalar());

            if (stateId != 0)
            {
                string localityIdStatement = @"SELECT TOP(1) locality_pid FROM [PSMA_G-NAF].[dbo].[LOCALITY] 
                                             WHERE DIFFERENCE(locality_name, @locality) = 4 and state_pid = @stateId 
                                             ORDER BY DIFFERENCE(locality_name, @locality) DESC";
                SqlCommand localityIdQuery = new SqlCommand(localityIdStatement, db);
                localityIdQuery.Parameters.Add(new SqlParameter("@locality", SqlDbType.NVarChar) { Value = locality });
                localityIdQuery.Parameters.Add(new SqlParameter("@stateId", SqlDbType.Int) { Value = stateId });
                string localityId = (string)localityIdQuery.ExecuteScalar();

                if (localityId != null)
                {
                    string streetType = streetName.Split(' ').Last();
                    string streetLocalityIdStatement = @"SELECT TOP(1) street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY] 
                                              WHERE DIFFERENCE(street_name, @name) = 4 AND locality_pid = @localityId AND street_type_code LIKE '%' + @type + '%' 
                                              ORDER BY DIFFERENCE(street_name, @name) DESC";
                    SqlCommand streetLocalityIdQuery = new SqlCommand(streetLocalityIdStatement, db);
                    streetLocalityIdQuery.Parameters.Add(new SqlParameter("@name", SqlDbType.NVarChar) { Value = streetName });
                    streetLocalityIdQuery.Parameters.Add(new SqlParameter("@localityId", SqlDbType.NVarChar) { Value = localityId });
                    streetLocalityIdQuery.Parameters.Add(new SqlParameter("@type", SqlDbType.NVarChar) { Value = streetType });
                    string streetLocalityId = (string)streetLocalityIdQuery.ExecuteScalar();
                    return streetLocalityId;
                }
            }
            return null;
        }
    }
}
