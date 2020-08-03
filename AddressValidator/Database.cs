using System;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using System.Text.RegularExpressions;

namespace AddressValidator
{
    class Database
    {
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
        /// Execute SQL command
        /// </summary>
        /// <param name="sqlCommand">sql query</param>
        /// <param name="parameters">sql query parameters</param>
        /// <param name="db">db connection</param>
        /// <returns>sql query result</returns>
        private static string GetValue(string sqlCommand, SqlConnection db, params Tuple<string, string>[] parameters)
        {
            SqlCommand command = new SqlCommand(sqlCommand, db);
            foreach (Tuple<string, string> param in parameters) command.Parameters.AddWithValue(param.Item1, param.Item2);
            return (string)command.ExecuteScalar();
        }

        /// <summary>
        /// Get state id
        /// </summary>
        /// <param name="state">state name</param>
        /// <param name="db">db connection</param>
        /// <returns>state id</returns>
        private static string GetState(string state, SqlConnection db)
        {
            if (state != "")
            {
                string stateIdExact = @"SELECT TOP(1) state_pid FROM [PSMA_G-NAF].[dbo].[STATE]
                                      WHERE state_abbreviation = @state";

                string stateIdDifference = @"SELECT TOP(1) state_pid FROM [PSMA_G-NAF].[dbo].[STATE] 
                                           WHERE DIFFERENCE(state_abbreviation, @state) > 2 
                                           ORDER BY [PSMA_G-NAF].[dbo].[Distance](state_abbreviation, @state) asc, DIFFERENCE(state_abbreviation, @state) desc";

                Tuple<string, string> sqlParam = Tuple.Create("@state", state);

                string stateId = GetValue(stateIdExact, db, sqlParam);
                if (stateId == null) stateId = GetValue(stateIdDifference, db, sqlParam);
                return stateId;
            }
            return null;
        }

        /// <summary>
        /// Get locality without state
        /// </summary>
        /// <param name="locality">locality name</param>
        /// <param name="db">db connection</param>
        /// <returns>locality id</returns>
        //private static string GetLocalityWithoutState(string locality, SqlConnection db)
        //{
        //    if (locality != "")
        //    {
        //        string localityIdWithoutStateExact = @"SELECT TOP(1) locality_pid FROM [PSMA_G-NAF].[dbo].[LOCALITY] 
        //                                             WHERE locality_name = @locality";

        //        string localityIdWithoutStateDifference = @"SELECT TOP(1) locality_pid FROM [PSMA_G-NAF].[dbo].[LOCALITY] 
        //                                                  WHERE DIFFERENCE(locality_name, @locality) > 2 
        //                                                  ORDER BY [PSMA_G-NAF].[dbo].[Distance](locality_name, @locality), DIFFERENCE(locality_name, @locality) desc";

        //        Tuple<string, string> sqlParam = Tuple.Create("@locality", locality);

        //        string localityId = GetValue(localityIdWithoutStateExact, db, sqlParam);
        //        if (localityId == null) localityId = GetValue(localityIdWithoutStateDifference, db, sqlParam);
        //        return localityId;
        //    }
        //    return null;
        //}

        /// <summary>
        /// Get locality
        /// </summary>
        /// <param name="stateId">state id</param>
        /// <param name="locality">locality name</param>
        /// <param name="db">db connection</param>
        /// <returns>locality id</returns>
        private static string GetLocality(string stateId, string locality, SqlConnection db)
        {

            if (locality != "")
            {
                string localityIdExact = @"SELECT TOP(1) locality_pid FROM [PSMA_G-NAF].[dbo].[LOCALITY]
                                         WHERE locality_name = @locality and state_pid = @stateId";

                string localityIdWithoutStateExact = @"SELECT TOP(1) locality_pid FROM [PSMA_G-NAF].[dbo].[LOCALITY] 
                                                     WHERE locality_name = @locality";

                string localityIdDifference = @"SELECT TOP(1) locality_pid FROM [PSMA_G-NAF].[dbo].[LOCALITY] 
                                              WHERE DIFFERENCE(locality_name, @locality) > 2 and state_pid = @stateId 
                                              ORDER BY [PSMA_G-NAF].[dbo].[Distance](locality_name, @locality) asc, DIFFERENCE(locality_name, @locality) desc";

                string localityIdWithoutStateDifference = @"SELECT TOP(1) locality_pid FROM [PSMA_G-NAF].[dbo].[LOCALITY] 
                                                          WHERE DIFFERENCE(locality_name, @locality) > 2 
                                                          ORDER BY [PSMA_G-NAF].[dbo].[Distance](locality_name, @locality), DIFFERENCE(locality_name, @locality) desc";

                Tuple<string, string>[] sqlParams =
                {
                    Tuple.Create("@locality", locality),
                };

                string localityId = null;
                if (stateId != null) localityId = GetValue(localityIdExact, db, sqlParams.Append(Tuple.Create("@stateId", stateId)).ToArray());
                if (localityId == null) localityId = GetValue(localityIdWithoutStateExact, db, sqlParams);
                if (localityId == null && stateId != null) localityId = GetValue(localityIdDifference, db, sqlParams.Append(Tuple.Create("@stateId", stateId)).ToArray());
                if (localityId == null) localityId = GetValue(localityIdWithoutStateDifference, db, sqlParams);
                return localityId;
            }
            return null;
        }

        private static string GetStreetType(string type, SqlConnection db)
        {
            if (type != "")
            {
                string streetTypeExact = @"SELECT TOP(1) [code] FROM [PSMA_G-NAF].[dbo].[STREET_TYPE_AUT]
                                         WHERE [name] = @name";

                string streetTypeLike = @"SELECT TOP(1) [code] FROM [PSMA_G-NAF].[dbo].[STREET_TYPE_AUT]
                                        WHERE [code] LIKE @name + '%'";

                Tuple<string, string> sqlParam = Tuple.Create("@name", type);

                string streetType = GetValue(streetTypeExact, db, sqlParam);
                if (streetType == null) streetType = GetValue(streetTypeLike, db, sqlParam);
                return streetType;
            }
            return null;
        }

        /// <summary>
        /// Get street locality without locality
        /// </summary>
        /// <param name="streetName">street name</param>
        /// <param name="db"></param>
        /// <returns>street id</returns>
        //private static string GetStreetWithoutLocality(string street, SqlConnection db)
        //{
        //    string[] streetName = street.Split();
        //    string type = GetStreetType(streetName[streetName.Length - 1], db);

        //    if (street != "")
        //    {
        //        string streetLocalityIdWithoutLocalityExact = @"SELECT TOP(1) street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY]
        //                                                      WHERE street_name = @name";

        //        Tuple<string, string>[] sqlParams =
        //        {
        //            Tuple.Create("@name", string.Join(" ", streetName.Take(streetName.Length - 1)))
        //        };

        //        if (type != null)
        //        {
        //            streetLocalityIdWithoutLocalityExact = @"SELECT TOP(1) street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY]
        //                                                   WHERE street_name = @name AND street_type_code = @type";

        //            sqlParams = sqlParams.Append(Tuple.Create("@type", type)).ToArray();
        //        }
                    

        //        string streetId = GetValue(streetLocalityIdWithoutLocalityExact, db, sqlParams);

        //        if (streetId == null)
        //        {
        //            string streetLocalityIdWithoutLocalityDifference = @"SELECT TOP(1) street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY] 
        //                                                               WHERE DIFFERENCE(street_name, @name) > 2
        //                                                               ORDER BY [PSMA_G-NAF].[dbo].[Distance](street_name, @name) asc, DIFFERENCE(street_name, @name) desc";

        //            if (type != null) streetLocalityIdWithoutLocalityDifference = @"SELECT TOP(1) street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY] 
        //                                                                          WHERE DIFFERENCE(street_name, @name) > 2 AND street_type_code = @type
        //                                                                          ORDER BY [PSMA_G-NAF].[dbo].[Distance](street_name, @name) asc, DIFFERENCE(street_name, @name) desc";

        //            streetId = GetValue(streetLocalityIdWithoutLocalityDifference, db, sqlParams);
        //        }
        //        return streetId;
        //    }
        //    return null;
        //}

        /// <summary>
        /// Get street
        /// </summary>
        /// <param name="localityId">locality id</param>
        /// <param name="street">street name</param>
        /// <param name="db">db connection</param>
        /// <returns>street id</returns>
        private static string GetStreet(string localityId, string street, SqlConnection db)
        {
            string[] streetName = street.Split();
            string type = GetStreetType(streetName[streetName.Length - 1], db);

            if (street != "")
            {
                string streetLocalityIdExact = @"SELECT TOP(1) street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY]
                                               WHERE street_name = @name AND locality_pid = @localityId";

                Tuple<string, string>[] sqlParams =
                {
                    Tuple.Create("@name", string.Join(" ", streetName.Take(streetName.Length - 1))),
                };

                if (type != null)
                {
                    streetLocalityIdExact = @"SELECT TOP(1) street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY]
                                            WHERE street_name = @name AND locality_pid = @localityId AND street_type_code = @type";

                    sqlParams = sqlParams.Append(Tuple.Create("@type", type)).ToArray();
                }

                string streetId = null;
                if (localityId != null) streetId = GetValue(streetLocalityIdExact, db, sqlParams.Append(Tuple.Create("@localityId", localityId)).ToArray());

                if (streetId == null)
                {
                    string streetLocalityIdWithoutLocalityExact = @"SELECT TOP(1) street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY]
                                                                  WHERE street_name = @name";

                    if (type != null) streetLocalityIdWithoutLocalityExact = @"SELECT TOP(1) street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY]
                                                                             WHERE street_name = @name AND street_type_code = @type";

                    streetId = GetValue(streetLocalityIdWithoutLocalityExact, db, sqlParams);
                }

                if (streetId == null)
                {
                    string streetLocalityIdDifference = @"SELECT TOP(1) street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY] 
                                                        WHERE DIFFERENCE(street_name, @name) > 2 AND locality_pid = @localityId 
                                                        ORDER BY [PSMA_G-NAF].[dbo].[Distance](street_name, @name) asc, DIFFERENCE(street_name, @name) desc";

                    if (type != null) streetLocalityIdDifference = @"SELECT TOP(1) street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY] 
                                                                   WHERE DIFFERENCE(street_name, @name) > 2 AND locality_pid = @localityId AND street_type_code = @type
                                                                   ORDER BY [PSMA_G-NAF].[dbo].[Distance](street_name, @name) asc, DIFFERENCE(street_name, @name) desc";

                    if (localityId != null) streetId = GetValue(streetLocalityIdDifference, db, sqlParams.Append(Tuple.Create("@localityId", localityId)).ToArray());
                }

                if (streetId == null)
                {
                    string streetLocalityIdWithoutLocalityDifference = @"SELECT TOP(1) street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY] 
                                                                       WHERE DIFFERENCE(street_name, @name) > 2
                                                                       ORDER BY [PSMA_G-NAF].[dbo].[Distance](street_name, @name) asc, DIFFERENCE(street_name, @name) desc";

                    if (type != null) streetLocalityIdWithoutLocalityDifference = @"SELECT TOP(1) street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY] 
                                                                                  WHERE DIFFERENCE(street_name, @name) > 2 AND street_type_code = @type
                                                                                  ORDER BY [PSMA_G-NAF].[dbo].[Distance](street_name, @name) asc, DIFFERENCE(street_name, @name) desc";

                    streetId = GetValue(streetLocalityIdWithoutLocalityDifference, db, sqlParams);
                }
                return streetId;
            }
            return null;
        }

        private static string GetHouseAddress(string streetId, string streetNumber, SqlConnection db)
        {
            string addressIdExact = @"SELECT TOP(1) address_detail_pid FROM [PSMA_G-NAF].[dbo].[ADDRESS_DETAIL]
                                            WHERE number_first = @houseNumber AND street_locality_pid = @streetId";

            Tuple<string, string>[] sqlParams =
            {
                        Tuple.Create("@streetId", streetId),
                        Tuple.Create("@houseNumber", streetNumber)
            };
            return GetValue(addressIdExact, db, sqlParams);
        }

        private static string GetAddress(string streetId, string streetNumber, SqlConnection db)
        {
            if (streetNumber != "")
            {
                if (Regex.IsMatch(streetNumber, @"^[0-9]+$")) return GetHouseAddress(streetId, streetNumber, db);
                else if (Regex.IsMatch(streetNumber, @"/|,"))
                {
                    string flat = Regex.Match(streetNumber, @"(\w+\s*)(?=/|,)").Value.Trim();
                    string houseRegex = Regex.Match(streetNumber, @"(?<=/|,)(\s*\w+)").Value.Trim();
                    string house = houseRegex.IndexOf('-') != -1 ? houseRegex.Substring(0, houseRegex.IndexOf('-')) : houseRegex;

                    if (Regex.IsMatch(flat, @"^[0-9]+$"))
                    {
                        string addressIdExact = @"SELECT TOP(1) address_detail_pid FROM [PSMA_G-NAF].[dbo].[ADDRESS_DETAIL]
                                                WHERE number_first = @houseNumber AND flat_number = @flatNumber AND street_locality_pid = @streetId";

                        Tuple<string, string>[] sqlParams =
                        {
                            Tuple.Create("@houseNumber", house),
                            Tuple.Create("@flatNumber", flat),
                            Tuple.Create("@streetId", streetId)
                        };

                        string addressId = GetValue(addressIdExact, db, sqlParams);
                        if (addressId == null) return GetHouseAddress(streetId, house, db);
                        return addressId;
                    }
                    else if (Regex.IsMatch(flat, @"[0-9]+[A-z]{1}$"))
                    {
                        string flatWithLetter = Regex.Match(flat, @"[0-9]+[A-z]{1}$").Value;

                        string addressIdExact = @"SELECT TOP(1) address_detail_pid FROM [PSMA_G-NAF].[dbo].[ADDRESS_DETAIL]
                                                WHERE number_first = @houseNumber AND flat_number = @flatNumber AND flat_number_suffix = @suffix AND street_locality_pid = @streetId";

                        Tuple<string, string>[] sqlParams =
                        {
                            Tuple.Create("@houseNumber", house),
                            Tuple.Create("@flatNumber", flatWithLetter.Remove(flatWithLetter.Length - 1)),
                            Tuple.Create("@suffix", flatWithLetter[flatWithLetter.Length - 1].ToString()),
                            Tuple.Create("@streetId", streetId)
                        };

                        string addressId = GetValue(addressIdExact, db, sqlParams);
                        if (addressId == null) return GetHouseAddress(streetId, house, db);
                        return addressId;
                    }
                }
                else if (Regex.IsMatch(streetNumber, @"[0-9]+[A-z]{1}$"))
                {

                    string addressIdExact = @"SELECT TOP(1) address_detail_pid FROM [PSMA_G-NAF].[dbo].[ADDRESS_DETAIL]
                                            WHERE number_first = @houseNumber AND number_first_suffix = @suffix AND street_locality_pid = @streetId";

                    Tuple<string, string>[] sqlParams =
                    {
                        Tuple.Create("@streetId", streetId),
                        Tuple.Create("@houseNumber", streetNumber.Remove(streetNumber.Length - 1)),
                        Tuple.Create("@suffix", streetNumber[streetNumber.Length - 1].ToString())
                    };

                    return GetValue(addressIdExact, db, sqlParams);
                }
            }
            return null;
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
            string stateId = GetState(state, db);
            string localityId = GetLocality(stateId, locality, db);
            string streetId = GetStreet(localityId, street, db);
            string addressId = null;

            //if (stateId == null)
            //{
            //    // Try searching without state
            //    localityId = GetLocalityWithoutState(locality, db);
            //}
            //else
            //{
            //    localityId = GetLocality(stateId, locality, db);
            //}

            //if (localityId == null)
            //{
            //    // Try searching without state
            //    localityId = GetLocalityWithoutState(locality, db);
            //}

            //if (localityId == null)
            //{
            //    // Try searching without locality
            //    streetId = GetStreetWithoutLocality(street, db);
            //}
            //else
            //{
            //    streetId = GetStreet(localityId, street, db);
            //}

            //if (streetId == null)
            //{
            //    // Try searching without locality
            //    streetId = GetStreetWithoutLocality(street, db);
            //}

            if (streetId != null)
            {
                addressId = GetAddress(streetId, streetNumber, db);
            }
            return addressId;
        }
    }
}
