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

        /// <summary>
        /// Check if db row exists
        /// </summary>
        /// <param name="query">sql query</param>
        /// <returns>true if row exists otherwise false</returns>
        private static bool CheckRows(SqlCommand query)
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

        /// <summary>
        /// Check if address value is in db
        /// </summary>
        /// <param name="field">address field</param>
        /// <param name="value">value of address field</param>
        /// <param name="db">database connection</param>
        /// <returns>true if found otherwise false</returns>
        public static bool CheckValid(FieldName field, string value, SqlConnection db)
        {
            if (field == FieldName.Postcode)
            {
                string queryStatement = @"SELECT count(address_detail_pid) FROM [PSMA_G-NAF].[dbo].[ADDRESS_DETAIL] WHERE postcode = @postcode";
                SqlCommand query = new SqlCommand(queryStatement, db);
                query.Parameters.Add(new SqlParameter("@postcode", SqlDbType.Int) { Value = value });
                return CheckRows(query);
            }
            else if (field == FieldName.State)
            {
                string queryStatement = @"SELECT count(state_pid) FROM [PSMA_G-NAF].[dbo].[STATE] WHERE DIFFERENCE(state_abbreviation, @state) > 2";
                SqlCommand query = new SqlCommand(queryStatement, db);
                query.Parameters.Add(new SqlParameter("@state", SqlDbType.NVarChar) { Value = value });
                return CheckRows(query);
            }
            else if (field == FieldName.Locality)
            {
                string queryStatement = @"SELECT count(locality_pid) FROM [PSMA_G-NAF].[dbo].[LOCALITY] WHERE DIFFERENCE(locality_name, @locality) = 4";
                SqlCommand query = new SqlCommand(queryStatement, db);
                query.Parameters.Add(new SqlParameter("@locality", SqlDbType.NVarChar) { Value = value });
                return CheckRows(query);
            }
            else if (field == FieldName.StreetName)
            {
                if (value != null)
                {
                    string streetType = value.Split(' ').Last();
                    string queryStatement = @"SELECT count(street_locality_pid) FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY] 
                                            WHERE DIFFERENCE(street_name, @name) = 4 AND street_type_code LIKE '%' + @type + '%'";
                    SqlCommand query = new SqlCommand(queryStatement, db);
                    query.Parameters.Add(new SqlParameter("@name", SqlDbType.NVarChar) { Value = value });
                    query.Parameters.Add(new SqlParameter("@type", SqlDbType.NVarChar) { Value = streetType });
                    return CheckRows(query);
                }
                return false;
            }
            return false;
        }

        private static object GetValue(string sqlCommand, SqlParameter[] parameters, SqlConnection db)
        {
            SqlCommand command = new SqlCommand(sqlCommand, db);
            command.Parameters.AddRange(parameters);
            object value = command.ExecuteScalar();
            command.Parameters.Clear();
            return value;
        }


        /// <summary>
        /// Get street locality id of address in Australia
        /// </summary>
        /// <param name="state">state</param>
        /// <param name="locality">suburb</param>
        /// <param name="street">street name</param>
        /// <param name="db">database connection</param>
        /// <returns>street locality id if found otherwise null</returns>
        public static string GetStreetLocalityId(string state, string locality, string street, SqlConnection db)
        {
            string stateIdExact = @"SELECT TOP(1) state_pid FROM [PSMA_G-NAF].[dbo].[STATE]
                                  WHERE state_abbreviation = @state";

            string stateIdDifference = @"SELECT TOP(1) state_pid FROM [PSMA_G-NAF].[dbo].[STATE] 
                                       WHERE DIFFERENCE(state_abbreviation, @state) > 2 
                                       ORDER BY [PSMA_G-NAF].[dbo].[Distance](state_abbreviation, @state)";

            SqlParameter[] stateIdParams = new SqlParameter[] { new SqlParameter("@state", SqlDbType.NVarChar) { Value = state } };

            int stateId = Convert.ToInt32(GetValue(stateIdExact, stateIdParams, db));

            if (stateId == 0)
            {
                stateId = Convert.ToInt32(GetValue(stateIdDifference, stateIdParams, db));
            }

            if (stateId != 0)
            {
                string localityIdExact = @"SELECT TOP(1) locality_pid FROM [PSMA_G-NAF].[dbo].[LOCALITY]
                                         WHERE locality_name = @locality and state_pid = @stateId";

                SqlParameter[] localityIdParams = new SqlParameter[]
                {
                    new SqlParameter("@locality", SqlDbType.NVarChar) { Value = locality },
                    new SqlParameter("@stateId", SqlDbType.Int) { Value = stateId },
                };

                string localityId = (string)GetValue(localityIdExact, localityIdParams, db);

                // Locality cannot be found so try vage search
                if (localityId == null)
                {
                    string localityIdDifference = @"SELECT TOP(1) locality_pid FROM [PSMA_G-NAF].[dbo].[LOCALITY] 
                                                  WHERE DIFFERENCE(locality_name, @locality) > 2 and state_pid = @stateId 
                                                  ORDER BY [PSMA_G-NAF].[dbo].[Distance](locality_name, @locality)";

                    localityId = (string)GetValue(localityIdDifference, localityIdParams, db);
                }

                // Locality cannot be found so try searching without state exact
                if (localityId == null)
                {
                    string localityIdWithoutStateExact = @"SELECT TOP(1) locality_pid FROM [PSMA_G-NAF].[dbo].[LOCALITY] 
                                                         WHERE locality_name = @locality";

                    localityId = (string)GetValue(localityIdWithoutStateExact, localityIdParams, db);
                }

                // Locality cannot be found so try searching without state vage
                if (localityId == null)
                {
                    string localityIdWithoutStateDifference = @"SELECT TOP(1) locality_pid FROM [PSMA_G-NAF].[dbo].[LOCALITY] 
                                                              WHERE DIFFERENCE(locality_name, @locality) > 2 
                                                              ORDER BY [PSMA_G-NAF].[dbo].[Distance](locality_name, @locality)";

                    localityId = (string)GetValue(localityIdWithoutStateDifference, localityIdParams, db);
                }

                if (localityId != null)
                {
                    string[] streetName = street.Split();

                    string streetLocalityIdExact = @"SELECT TOP(1) street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY]
                                                   WHERE street_name = @name AND locality_pid = @localityId";

                    SqlParameter[] streetLocalityIdParams = new SqlParameter[]
                    {
                        new SqlParameter("@name", SqlDbType.NVarChar) { Value = string.Join(" ", streetName.Take(streetName.Length - 1)) },
                        new SqlParameter("@localityId", SqlDbType.NVarChar) { Value = localityId }
                    };

                    string streetLocalityId = (string)GetValue(streetLocalityIdExact, streetLocalityIdParams, db);
                    
                    // Street cannot be found try vage search
                    if (streetLocalityId == null)
                    {
                        string streetLocalityIdDifference = @"SELECT TOP(1) street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY] 
                                                            WHERE DIFFERENCE(street_name, @name) > 2 AND locality_pid = @localityId 
                                                            ORDER BY [PSMA_G-NAF].[dbo].[Distance](street_name, @name)";

                        streetLocalityId = (string)GetValue(streetLocalityIdDifference, streetLocalityIdParams, db);
                    }
                    
                    // Street cannot be found try searching without locality exact
                    if (streetLocalityId == null)
                    {
                        string streetLocalityIdWithoutLocalityExact = @"SELECT TOP(1) street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY]
                                                                      WHERE street_name = @name";
                        
                        streetLocalityId = (string)GetValue(streetLocalityIdWithoutLocalityExact, streetLocalityIdParams, db);
                    }

                    // Street cannot be found try searching without locality vague
                    if (streetLocalityId == null)
                    {
                        string streetLocalityIdWithoutLocalityDifference = @"SELECT TOP(1) street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY] 
                                                                           WHERE DIFFERENCE(street_name, @name) > 2
                                                                           ORDER BY [PSMA_G-NAF].[dbo].[Distance](street_name, @name)";

                        streetLocalityId = (string)GetValue(streetLocalityIdWithoutLocalityDifference, streetLocalityIdParams, db);
                    }

                    return streetLocalityId;
                }
            }
            return null;
        }
    }
}
