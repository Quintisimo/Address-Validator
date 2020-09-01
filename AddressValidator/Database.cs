using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace AddressValidator
{
    class Database
    {
        /// <summary>
        /// Execute SQL command with single row
        /// </summary>
        /// <param name="sqlCommand">sql query</param>
        /// <param name="parameters">sql query parameters</param>
        /// <returns>sql query result</returns>
        private static string GetValue(string sqlCommand, params Tuple<string, string>[] parameters)
        {
            ConnectionStringSettings settings = ConfigurationManager.ConnectionStrings["DB Connection"];
            using (SqlConnection db = new SqlConnection(settings.ConnectionString))
            {
                SqlCommand command = new SqlCommand(sqlCommand, db);
                foreach (Tuple<string, string> param in parameters) command.Parameters.AddWithValue(param.Item1, param.Item2);
                command.Connection.Open();
                return (string)command.ExecuteScalar();
            }
        }
        
        /// <summary>
        /// Execute SQL command with multiple rows
        /// </summary>
        /// <param name="sqlCommand">sql query</param>
        /// <param name="parameters">sql query parameters</param>
        /// <returns>sql query results</returns>
        private static List<string> GetValues(string sqlCommand, params Tuple<string, string>[] parameters)
        {
            ConnectionStringSettings settings = ConfigurationManager.ConnectionStrings["DB Connection"];
            using (SqlConnection db = new SqlConnection(settings.ConnectionString))
            {
                SqlCommand command = new SqlCommand(sqlCommand, db);
                foreach (Tuple<string, string> param in parameters) command.Parameters.AddWithValue(param.Item1, param.Item2);
                command.Connection.Open();
                SqlDataReader reader = command.ExecuteReader();
                List<string> values = new List<string>();
                while (reader.Read()) values.Add(reader.GetString(0));
                return values;
            }
        }

        /// <summary>
        /// Get state id
        /// </summary>
        /// <param name="state">state name</param>
        /// <returns>state id</returns>
        private static string GetState(string state)
        {
            string stateIdExact = @"SELECT TOP(1) state_pid FROM [PSMA_G-NAF].[dbo].[STATE]
                                  WHERE state_abbreviation = @state";
            Tuple<string, string> sqlParam = Tuple.Create("@state", state);
            return GetValue(stateIdExact, sqlParam);
        }

        /// <summary>
        /// Get locality
        /// </summary>
        /// <param name="stateId">state id</param>
        /// <param name="locality">locality name</param>
        /// <returns>locality id</returns>
        private static string GetLocality(string stateId, string locality)
        {
            string localityIdExact = @"SELECT TOP(1) locality_pid FROM [PSMA_G-NAF].[dbo].[LOCALITY]
                                     WHERE locality_name = @locality and state_pid = @stateId";

            string localityIdDifference = @"SELECT TOP(1) locality_pid FROM [PSMA_G-NAF].[dbo].[LOCALITY] 
                                          WHERE DIFFERENCE(locality_name, @locality) > 2 and state_pid = @stateId 
                                          ORDER BY [PSMA_G-NAF].[dbo].[Distance](locality_name, @locality) asc, DIFFERENCE(locality_name, @locality) desc";

            Tuple<string, string>[] sqlParams =
            {
                Tuple.Create("@locality", locality),
                Tuple.Create("@stateId", stateId)
            };

            string localityId = GetValue(localityIdExact, sqlParams);
            if (localityId == null) localityId = GetValue(localityIdDifference, sqlParams);
            return localityId;
        }

        /// <summary>
        /// Get street type code
        /// </summary>
        /// <param name="type">abbrevation code</param>
        /// <returns></returns>
        private static string GetStreetType(string type)
        {
            if (type != "")
            {
                string streetTypeExact = @"SELECT TOP(1) [code] FROM [PSMA_G-NAF].[dbo].[STREET_TYPE_AUT]
                                         WHERE [name] = @name";

                string streetTypeLike = @"SELECT TOP(1) [code] FROM [PSMA_G-NAF].[dbo].[STREET_TYPE_AUT]
                                        WHERE [code] LIKE @name + '%'";

                Tuple<string, string> sqlParam = Tuple.Create("@name", type);

                string streetType = GetValue(streetTypeExact, sqlParam);
                if (streetType == null) streetType = GetValue(streetTypeLike, sqlParam);
                return streetType;
            }
            return null;
        }

        /// <summary>
        /// Get street
        /// </summary>
        /// <param name="localityId">locality id</param>
        /// <param name="street">street name</param>
        /// <returns>street id</returns>
        private static List<string> GetStreet(string state, string localityId, string street, bool ignoreType)
        {
            string[] streetName = street.Split();
            string type = null;
            if (!ignoreType) type = GetStreetType(streetName[streetName.Length - 1]);

            string streetLocalityIdExact = @"SELECT street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY]
                                           WHERE street_name = @name AND locality_pid = @localityId AND street_locality_pid LIKE @state + '%'";

            Tuple<string, string>[] sqlParams =
            {
                Tuple.Create("@name", type != null ? string.Join(" ", streetName.Take(streetName.Length - 1)) : street),
                Tuple.Create("@state", state)
            };

            Tuple<string, string>[] extraPramas =
            {
                    Tuple.Create("@localityId", localityId)
            };

            if (type != null)
            {
                streetLocalityIdExact = @"SELECT street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY]
                                        WHERE street_name = @name AND locality_pid = @localityId AND street_type_code = @type AND street_locality_pid LIKE @state + '%'";

                sqlParams = sqlParams.Append(Tuple.Create("@type", type)).ToArray();
            }

            List<string> streetId = new List<string>();
            if (localityId != null) streetId = GetValues(streetLocalityIdExact, sqlParams.Concat(extraPramas).ToArray());

            if (streetId.Count == 0)
            {
                string streetLocalityIdWithoutLocalityExact = @"SELECT street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY]
                                                              WHERE street_name = @name AND street_locality_pid LIKE @state + '%'";

                if (type != null) streetLocalityIdWithoutLocalityExact = @"SELECT street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY]
                                                                         WHERE street_name = @name AND street_type_code = @type AND street_locality_pid LIKE @state + '%'";

                streetId = GetValues(streetLocalityIdWithoutLocalityExact, sqlParams);
            }

            if (streetId.Count == 0)
            {
                string streetLocalityIdDifference = @"SELECT street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY] 
                                                    WHERE DIFFERENCE(street_name, @name) > 2 AND locality_pid = @localityId AND street_locality_pid LIKE @state + '%' 
                                                    ORDER BY [PSMA_G-NAF].[dbo].[Distance](street_name, @name) asc, DIFFERENCE(street_name, @name) desc";

                if (type != null) streetLocalityIdDifference = @"SELECT street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY] 
                                                               WHERE DIFFERENCE(street_name, @name) > 2 AND locality_pid = @localityId AND street_type_code = @type AND street_locality_pid LIKE @state + '%'
                                                               ORDER BY [PSMA_G-NAF].[dbo].[Distance](street_name, @name) asc, DIFFERENCE(street_name, @name) desc";

                if (localityId != null) streetId = GetValues(streetLocalityIdDifference, sqlParams.Concat(extraPramas).ToArray());
            }

            if (streetId.Count == 0)
            {
                string streetLocalityIdWithoutLocalityDifference = @"SELECT street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY] 
                                                                   WHERE DIFFERENCE(street_name, @name) > 2 AND street_locality_pid LIKE @state + '%'
                                                                   ORDER BY [PSMA_G-NAF].[dbo].[Distance](street_name, @name) asc, DIFFERENCE(street_name, @name) desc";

                if (type != null) streetLocalityIdWithoutLocalityDifference = @"SELECT street_locality_pid FROM [PSMA_G-NAF].[dbo].[STREET_LOCALITY] 
                                                                              WHERE DIFFERENCE(street_name, @name) > 2 AND street_type_code = @type AND street_locality_pid LIKE @state + '%'
                                                                              ORDER BY [PSMA_G-NAF].[dbo].[Distance](street_name, @name) asc, DIFFERENCE(street_name, @name) desc";

                streetId = GetValues(streetLocalityIdWithoutLocalityDifference, sqlParams);
            }
            return streetId;
        }

        /// <summary>
        /// Get house address
        /// </summary>
        /// <param name="streetId">street locality id</param>
        /// <param name="streetNumber">street number</param>
        /// <returns></returns>
        private static string GetHouseAddress(string streetId, string streetNumber)
        {
            string addressIdExact = @"SELECT TOP(1) address_detail_pid FROM [PSMA_G-NAF].[dbo].[ADDRESS_DETAIL]
                                    WHERE number_first = @houseNumber AND street_locality_pid = @streetId";

            string addressIdRange = @"SELECT TOP(1) address_detail_pid FROM [PSMA_G-NAF].[dbo].[ADDRESS_DETAIL]
                                    WHERE street_locality_pid = @streetId AND @houseNumber BETWEEN number_first and number_last";

            Tuple<string, string>[] sqlParams =
            {
                Tuple.Create("@streetId", streetId),
                Tuple.Create("@houseNumber", streetNumber)
            };
            string houseId = GetValue(addressIdExact, sqlParams);
            if (houseId == null) houseId = GetValue(addressIdRange, sqlParams);
            return houseId;
        }

        private static string GetAddress(string streetId, string streetNumber)
        {
            if (Regex.IsMatch(streetNumber, @"^[0-9]+$"))
            {
                string addressId = GetHouseAddress(streetId, streetNumber);

                string addressIdExact = @"SELECT TOP(1) address_detail_pid from [PSMA_G-NAF].[dbo].[ADDRESS_DETAIL]
                                            WHERE flat_number = @flatNumber AND street_locality_pid = @streetId";

                Tuple<string, string>[] sqlParams =
                {
                    Tuple.Create("@flatNumber", streetNumber),
                    Tuple.Create("@streetId", streetId)
                };

                if (addressId == null) addressId = GetValue(addressIdExact, sqlParams);
                return addressId;
            }
            else if (Regex.IsMatch(streetNumber, @"^[0-9]+[A-z]{1}$"))
            {

                string addressIdExact = @"SELECT TOP(1) address_detail_pid FROM [PSMA_G-NAF].[dbo].[ADDRESS_DETAIL]
                                            WHERE number_first = @houseNumber AND number_first_suffix = @suffix AND street_locality_pid = @streetId";

                Tuple<string, string>[] sqlParams =
                {
                    Tuple.Create("@streetId", streetId),
                    Tuple.Create("@houseNumber", streetNumber.Remove(streetNumber.Length - 1)),
                    Tuple.Create("@suffix", streetNumber[streetNumber.Length - 1].ToString())
                };

                return GetValue(addressIdExact, sqlParams);
            }
            else if (Regex.IsMatch(streetNumber, @"-"))
            {
                string first = Regex.Match(streetNumber, @"(\d+)(?=-)").Value.Trim();
                string last = Regex.Match(streetNumber, @"(?<=-)(\d+)").Value.Trim();

                string addressIdExact = @"SELECT TOP(1) address_detail_pid FROM [PSMA_G-NAF].[dbo].[ADDRESS_DETAIL]
                                            WHERE number_first = @first AND number_last = @last AND street_locality_pid = @streetId";

                Tuple<string, string>[] sqlParams =
                {
                    Tuple.Create("@streetId", streetId),
                    Tuple.Create("@first", first),
                    Tuple.Create("@last", last)
                };

                string addressId = GetValue(addressIdExact, sqlParams);

                if (addressId == null) addressId = GetHouseAddress(streetId, first);
                if (addressId == null) addressId = GetHouseAddress(streetId, last);
                return addressId;
            }
            else if (Regex.IsMatch(streetNumber, @"(\d+\w*\s*)(/|,|\s)"))
            {
                string flat = Regex.Match(streetNumber, @"(\d+\w*\s*)(?=/|,|\s)").Value.Trim();
                string houseRegex = Regex.Match(streetNumber, @"(?<=(\s|/|,)(?!.*(\s|/|,)))(.*)").Value.Trim();
                string house = houseRegex.IndexOf('-') != -1 ? houseRegex.Substring(0, houseRegex.IndexOf('-')) : houseRegex;

                if (Regex.IsMatch(flat, @"^[0-9]+$"))
                {
                    if (Regex.IsMatch(house, @"^[0-9]+[A-z]{1}$"))
                    {
                        string addressIdExact = @"SELECT TOP(1) address_detail_pid FROM [PSMA_G-NAF].[dbo].[ADDRESS_DETAIL]
                                                    WHERE number_first = @houseNumber AND number_first_suffix = @suffix AND flat_number = @flatNumber AND street_locality_pid = @streetId";

                        Tuple<string, string>[] sqlParams =
                        {
                            Tuple.Create("@houseNumber", house.Remove(house.Length - 1)),
                            Tuple.Create("@suffix", house[house.Length - 1].ToString()),
                            Tuple.Create("@flatNumber", flat),
                            Tuple.Create("@streetId", streetId)
                        };

                        string addressId = GetValue(addressIdExact, sqlParams);
                        return addressId;
                    }
                    else
                    {
                        string addressIdExact = @"SELECT TOP(1) address_detail_pid FROM [PSMA_G-NAF].[dbo].[ADDRESS_DETAIL]
                                                    WHERE number_first = @houseNumber AND flat_number = @flatNumber AND street_locality_pid = @streetId";

                        Tuple<string, string>[] sqlParams =
                        {
                            Tuple.Create("@houseNumber", house),
                            Tuple.Create("@flatNumber", flat),
                            Tuple.Create("@streetId", streetId)
                        };

                        string addressId = GetValue(addressIdExact, sqlParams);
                        if (addressId == null) return GetHouseAddress(streetId, house);
                        return addressId;
                    }
                }
                else if (Regex.IsMatch(flat, @"^[0-9]+[A-z]{1}$"))
                {
                    if (Regex.IsMatch(house, @"^[0-9]+[A-z]{1}$"))
                    {
                        string flatWithLetter = Regex.Match(flat, @"[0-9]+[A-z]{1}$").Value;

                        string addressIdExact = @"SELECT TOP(1) address_detail_pid FROM [PSMA_G-NAF].[dbo].[ADDRESS_DETAIL]
                                                WHERE number_first = @houseNumber AND number_first_suffix = @suffix AND flat_number = @flatNumber AND flat_number_suffix = @flatSuffix AND street_locality_pid = @streetId";

                        Tuple<string, string>[] sqlParams =
                        {
                                Tuple.Create("@houseNumber", house.Remove(house.Length - 1)),
                                Tuple.Create("@suffix", house[house.Length - 1].ToString()),
                                Tuple.Create("@flatNumber", flatWithLetter.Remove(flatWithLetter.Length - 1)),
                                Tuple.Create("@flatSuffix", flatWithLetter[flatWithLetter.Length - 1].ToString()),
                                Tuple.Create("@streetId", streetId)
                            };

                        string addressId = GetValue(addressIdExact, sqlParams);
                        return addressId;
                    }
                    else
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

                        string addressId = GetValue(addressIdExact, sqlParams);
                        if (addressId == null) return GetHouseAddress(streetId, house);
                        return addressId;
                    }
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
        public static string GetAddressId(string state, string locality, string street, string streetNumber)
        {
            string stateId = GetState(state);

            if (!string.IsNullOrEmpty(stateId))
            {
                string localityId = GetLocality(stateId, locality);
                List<string> streetIds = GetStreet(state, localityId, street, false);
                List<string> addressIds = new List<string>();

                if (streetIds.Count == 0) streetIds = GetStreet(state, localityId, street, true);

                //Parallel.ForEach(streetIds, streetId => addressIds.Add(GetAddress(streetId, streetNumber)));
                streetIds.ForEach(streetId => addressIds.Add(GetAddress(streetId, streetNumber)));
                addressIds = addressIds.Where(s => !string.IsNullOrEmpty(s)).Distinct().ToList();

                if (addressIds.Count == 0)
                {
                    streetIds = GetStreet(state, localityId, street, true);
                    streetIds.ForEach(streetId => addressIds.Add(GetAddress(streetId, streetNumber)));
                    //Parallel.ForEach(streetIds, streetId => addressIds.Add(GetAddress(streetId, streetNumber)));
                    addressIds = addressIds.Where(s => !string.IsNullOrEmpty(s)).Distinct().ToList();
                } 

                Console.WriteLine($"[{string.Join(", ", addressIds)}]");

                return addressIds.First();
            }
            return null;
        }
    }
}
