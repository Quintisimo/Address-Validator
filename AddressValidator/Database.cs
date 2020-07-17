using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text.RegularExpressions;

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

        /// <summary>
        /// Execute SQL command
        /// </summary>
        /// <param name="sqlCommand">sql query</param>
        /// <param name="parameters">sql query parameters</param>
        /// <param name="db">db connection</param>
        /// <returns>sql query result</returns>
        private static object GetValue(string sqlCommand, SqlParameter[] parameters, SqlConnection db)
        {
            SqlCommand command = new SqlCommand(sqlCommand, db);
            command.Parameters.AddRange(parameters);
            object value = command.ExecuteScalar();
            command.Parameters.Clear();
            return value;
        }

        /// <summary>
        /// Get state id
        /// </summary>
        /// <param name="state">state name</param>
        /// <param name="db">db connection</param>
        /// <returns>state id</returns>
        private static int GetState(string state, SqlConnection db)
        {
            if (state != "")
            {
                string stateIdExact = @"SELECT TOP(1) state_pid FROM [PSMA_G-NAF].[dbo].[STATE]
                                      WHERE state_abbreviation = @state";

                string stateIdDifference = @"SELECT TOP(1) state_pid FROM [PSMA_G-NAF].[dbo].[STATE] 
                                           WHERE DIFFERENCE(state_abbreviation, @state) > 2 
                                           ORDER BY [PSMA_G-NAF].[dbo].[Distance](state_abbreviation, @state) asc, DIFFERENCE(state_abbreviation, @state) desc";

                SqlParameter[] stateIdParams = new SqlParameter[] { new SqlParameter("@state", SqlDbType.NVarChar) { Value = state } };

                int stateId = Convert.ToInt32(GetValue(stateIdExact, stateIdParams, db));
                if (stateId == 0)
                {
                    stateId = Convert.ToInt32(GetValue(stateIdDifference, stateIdParams, db));
                }
                return stateId;
            }
            return 0;
        }

        /// <summary>
        /// Get locality without state
        /// </summary>
        /// <param name="locality">locality name</param>
        /// <param name="db">db connection</param>
        /// <returns>locality id</returns>
        private static string GetLocalityWithoutState(string locality, SqlConnection db)
        {
            string localityId = null;

            SqlParameter[] localityIdParams = new SqlParameter[]
            {
                new SqlParameter("@locality", SqlDbType.NVarChar) { Value = locality }
            };

            if (locality != "")
            {
                string localityIdWithoutStateExact = @"SELECT TOP(1) locality_pid FROM [PSMA_G-NAF].[dbo].[LOCALITY] 
                                                     WHERE locality_name = @locality";

                localityId = (string)GetValue(localityIdWithoutStateExact, localityIdParams, db);

                if (localityId == null)
                {
                    string localityIdWithoutStateDifference = @"SELECT TOP(1) locality_pid FROM [PSMA_G-NAF].[dbo].[LOCALITY] 
                                                              WHERE DIFFERENCE(locality_name, @locality) > 2 
                                                              ORDER BY [PSMA_G-NAF].[dbo].[Distance](locality_name, @locality), DIFFERENCE(locality_name, @locality) desc";

                    localityId = (string)GetValue(localityIdWithoutStateDifference, localityIdParams, db);
                }
            }
            return localityId;
        }

        /// <summary>
        /// Get locality
        /// </summary>
        /// <param name="stateId">state id</param>
        /// <param name="locality">locality name</param>
        /// <param name="db">db connection</param>
        /// <returns>locality id</returns>
        private static string GetLocality(int stateId, string locality, SqlConnection db)
        {
            string localityId = null;
            SqlParameter[] localityIdParams = new SqlParameter[]
            {
                    new SqlParameter("@locality", SqlDbType.NVarChar) { Value = locality },
                    new SqlParameter("@stateId", SqlDbType.Int) { Value = stateId },
            };

            if (locality != "")
            {
                string localityIdExact = @"SELECT TOP(1) locality_pid FROM [PSMA_G-NAF].[dbo].[LOCALITY]
                                         WHERE locality_name = @locality and state_pid = @stateId";

                localityId = (string)GetValue(localityIdExact, localityIdParams, db);

                // Locality cannot be found so try vage search
                if (localityId == null)
                {
                    string localityIdDifference = @"SELECT TOP(1) locality_pid FROM [PSMA_G-NAF].[dbo].[LOCALITY] 
                                                  WHERE DIFFERENCE(locality_name, @locality) > 2 and state_pid = @stateId 
                                                  ORDER BY [PSMA_G-NAF].[dbo].[Distance](locality_name, @locality) asc, DIFFERENCE(locality_name, @locality) desc";

                    localityId = (string)GetValue(localityIdDifference, localityIdParams, db);
                }
            }
            return localityId;
        }

        private static string GetStreetType(string type, SqlConnection db)
        {
            string streetType = null;
            SqlParameter[] streetTypeParams = new SqlParameter[]
            {
                new SqlParameter("@name", SqlDbType.NVarChar) { Value = type }
            };

            string streetTypeExact = @"SELECT TOP(1) [code] FROM [PSMA_G-NAF].[dbo].[STREET_TYPE_AUT]
                                     WHERE [name] = @name";
            streetType = (string)GetValue(streetTypeExact, streetTypeParams, db);

            if (streetType == null)
            {
                string streetTypeLike = @"SELECT TOP(1) [code] FROM [PSMA_G-NAF].[dbo].[STREET_TYPE_AUT]
                                        WHERE [code] LIKE @name + '%'";
                streetType = (string)GetValue(streetTypeLike, streetTypeParams, db);
            }
            return streetType;
        }

        /// <summary>
        /// Get street locality without locality
        /// </summary>
        /// <param name="streetName">street name</param>
        /// <param name="db"></param>
        /// <returns>street id</returns>
        private static string GetStreetWithoutLocality(string street, SqlConnection db)
        {
            string streetId = null;
            string[] streetName = street.Split();
            string type = GetStreetType(streetName[streetName.Length - 1], db);
            Console.WriteLine(type);
            SqlParameter[] streetIdParams = new SqlParameter[]
            {
                new SqlParameter("@name", SqlDbType.NVarChar) { Value = string.Join(" ", streetName.Take(streetName.Length - 1)) },
                new SqlParameter("@type", SqlDbType.NVarChar) { Value = type ?? (object)DBNull.Value }
            };

            if (street != "")
            {
                string streetLocalityIdWithoutLocalityExact = @"SELECT TOP(1) street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY]
                                                              WHERE street_name = @name";

                if (type != null)
                {
                    streetLocalityIdWithoutLocalityExact = @"SELECT TOP(1) street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY]
                                                           WHERE street_name = @name AND street_type_code = @type";
                }

                streetId = (string)GetValue(streetLocalityIdWithoutLocalityExact, streetIdParams, db);

                if (streetId == null)
                {
                    string streetLocalityIdWithoutLocalityDifference = @"SELECT TOP(1) street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY] 
                                                                       WHERE DIFFERENCE(street_name, @name) > 2
                                                                       ORDER BY [PSMA_G-NAF].[dbo].[Distance](street_name, @name) asc, DIFFERENCE(street_name, @name) desc";

                    if (type != null)
                    {
                        streetLocalityIdWithoutLocalityDifference = @"SELECT TOP(1) street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY] 
                                                                    WHERE DIFFERENCE(street_name, @name) > 2 AND street_type_code = @type
                                                                    ORDER BY [PSMA_G-NAF].[dbo].[Distance](street_name, @name) asc, DIFFERENCE(street_name, @name) desc";

                    }

                    streetId = (string)GetValue(streetLocalityIdWithoutLocalityDifference, streetIdParams, db);
                }

            }
            return streetId;
        }

        /// <summary>
        /// Get street
        /// </summary>
        /// <param name="localityId">locality id</param>
        /// <param name="street">street name</param>
        /// <param name="db">db connection</param>
        /// <returns>street id</returns>
        private static string GetStreet(string localityId, string street, SqlConnection db)
        {
            string streetId = null;
            string[] streetName = street.Split();
            string type = GetStreetType(streetName[streetName.Length - 1], db);
            Console.WriteLine(type);
            SqlParameter[] streetIdParams = new SqlParameter[]
            {
                new SqlParameter("@name", SqlDbType.NVarChar) { Value = string.Join(" ", streetName.Take(streetName.Length - 1)) },
                new SqlParameter("@localityId", SqlDbType.NVarChar) { Value = localityId },
                new SqlParameter("@type", SqlDbType.NVarChar) { Value = type ?? (object)DBNull.Value }
            };

            if (street != "")
            {
                string streetLocalityIdExact = @"SELECT TOP(1) street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY]
                                               WHERE street_name = @name AND locality_pid = @localityId";

                if (type != null)
                {
                    streetLocalityIdExact = @"SELECT TOP(1) street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY]
                                            WHERE street_name = @name AND locality_pid = @localityId AND street_type_code = @type";

                }

                streetId = (string)GetValue(streetLocalityIdExact, streetIdParams, db);

                if (streetId == null)
                {
                    string streetLocalityIdDifference = @"SELECT TOP(1) street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY] 
                                                        WHERE DIFFERENCE(street_name, @name) > 2 AND locality_pid = @localityId 
                                                        ORDER BY [PSMA_G-NAF].[dbo].[Distance](street_name, @name) asc, DIFFERENCE(street_name, @name) desc";

                    if (type != null)
                    {
                        streetLocalityIdDifference = @"SELECT TOP(1) street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY] 
                                                     WHERE DIFFERENCE(street_name, @name) > 2 AND locality_pid = @localityId AND street_type_code = @type
                                                     ORDER BY [PSMA_G-NAF].[dbo].[Distance](street_name, @name) asc, DIFFERENCE(street_name, @name) desc";
                    }

                    streetId = (string)GetValue(streetLocalityIdDifference, streetIdParams, db);
                }
            }
            return streetId;
        }

        private static string GetAddress(string streetId, string house, SqlConnection db)
        {
            string addressId = null;

            if (int.TryParse(house, out int houseNumber))
            {
                SqlParameter[] addressIdParams = new SqlParameter[]
                {
                   new SqlParameter("@streetId", SqlDbType.NVarChar) { Value = streetId },
                   new SqlParameter("@houseNumber", SqlDbType.Int) { Value = houseNumber }
                };

                string addressIdExact = @"SELECT TOP(1) address_detail_pid FROM [PSMA_G-NAF].[dbo].[ADDRESS_DETAIL]
                                        WHERE number_first = @houseNumber AND street_locality_pid = @streetId";
                addressId = (string)GetValue(addressIdExact, addressIdParams, db);
            }
            else if (Regex.IsMatch(house, @"[0-9]+[A-z]{1}$"))
            {
                SqlParameter[] addressIdParams = new SqlParameter[]
                {
                   new SqlParameter("@streetId", SqlDbType.NVarChar) { Value = streetId },
                   new SqlParameter("@houseNumber", SqlDbType.Int) { Value = int.Parse(house.Remove(house.Length - 1)) },
                   new SqlParameter("@suffix", SqlDbType.NVarChar) { Value = house[house.Length - 1] }
                };

                string addressIdExact = @"SELECT TOP(1) address_detail_pid FROM [PSMA_G-NAF].[dbo].[ADDRESS_DETAIL]
                                        WHERE number_first = @houseNumber AND street_locality_pid = @streetId AND number_first_suffix = @suffix";
                addressId = (string)GetValue(addressIdExact, addressIdParams, db);
            }
            return addressId;
        }

        /// <summary>
        /// Get address id of address in Australia
        /// </summary>
        /// <param name="state">state</param>
        /// <param name="locality">suburb</param>
        /// <param name="street">street name</param>
        /// <param name="db">database connection</param>
        /// <returns>street locality id if found otherwise null</returns>
        public static string GetAddressId(string state, string locality, string street, string streetNumber, SqlConnection db)
        {
            int stateId = GetState(state, db);
            string localityId = null;
            string streetId = null;
            string addressId = null;

            if (stateId == 0)
            {
                // Try searching without state
                localityId = GetLocalityWithoutState(locality, db);
            }
            else
            {
                localityId = GetLocality(stateId, locality, db);
            }

            if (localityId == null)
            {
                // Try searching without state
                localityId = GetLocalityWithoutState(locality, db);
            }

            if (localityId == null)
            {
                // Try searching without locality
                streetId = GetStreetWithoutLocality(street, db);
            }
            else
            {
                streetId = GetStreet(localityId, street, db);
            }

            if (streetId == null)
            {
                // Try searching without locality
                streetId = GetStreetWithoutLocality(street, db);
            }

            //if (streetId != null)
            //{
            //    addressId = GetAddress(streetId, streetNumber, db);
            //}
            return streetId;
        }
    }
}
