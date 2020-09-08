using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace AddressValidator
{
    internal class Database
    {
        private static SortedList<string, State> states;
        private static SortedList<string, SortedList<string, Locality>> postcodeLocalities;
        private static SortedList<int, SortedList<string, Locality>> localities;
        private static SortedList<string, StreeType> types;

        static Database()
        {
            states = new SortedList<string, State>();
            postcodeLocalities = new SortedList<string, SortedList<string, Locality>>();
            localities = new SortedList<int, SortedList<string, Locality>>();
            types = new SortedList<string, StreeType>();
        }

        private static SqlConnection Connect()
        {
            ConnectionStringSettings settings = ConfigurationManager.ConnectionStrings["DB Connection"];
            return new SqlConnection(settings.ConnectionString);
        }

        private static SqlConnection ConnectLoyaltyDB()
        {
            ConnectionStringSettings settings = ConfigurationManager.ConnectionStrings["DB Loyalty Connection"];
            return new SqlConnection(settings.ConnectionString);
        }

        ///// <summary>
        ///// Execute SQL command with single row
        ///// </summary>
        ///// <param name="sqlCommand">sql query</param>
        ///// <param name="parameters">sql query parameters</param>
        ///// <returns>sql query result</returns>
        //private static string GetValue(string sqlCommand, Tuple<string, string>[] parameters, SqlConnection dbc = null, int timemout = 30)
        //{
        //    if (dbc == null)
        //    {
        //        using (SqlConnection db = Connect())
        //        {
        //            SqlCommand command = new SqlCommand(sqlCommand, db);
        //            foreach (Tuple<string, string> param in parameters) command.Parameters.AddWithValue(param.Item1, param.Item2);
        //            command.Connection.Open();
        //            return (string)command.ExecuteScalar();
        //        }
        //    }
        //    else
        //    {
        //        SqlCommand command = new SqlCommand(sqlCommand, dbc);
        //        command.CommandTimeout = timemout;
        //        foreach (Tuple<string, string> param in parameters) command.Parameters.AddWithValue(param.Item1, param.Item2);
        //        try
        //        {
        //            return (string)command.ExecuteScalar();
        //        }
        //        catch
        //        {
        //            Console.WriteLine("Error in Query");
        //            return null;
        //        }
        //    }
        //}
        
        private static async Task<string> GetValueAsync(string sqlCommand, Tuple<string, string>[] parameters, SqlConnection dbc = null, int timemout = 30)
        {
            if (dbc == null)
            {
                using (SqlConnection db = Connect())
                {
                    SqlCommand command = new SqlCommand(sqlCommand, db);
                    foreach (Tuple<string, string> param in parameters) command.Parameters.AddWithValue(param.Item1, param.Item2);
                    await command.Connection.OpenAsync();
                    return (string) await command.ExecuteScalarAsync();
                }
            }
            else
            {
                SqlCommand command = new SqlCommand(sqlCommand, dbc);
                command.CommandTimeout = timemout;
                foreach (Tuple<string, string> param in parameters) command.Parameters.AddWithValue(param.Item1, param.Item2);
                try
                {
                    return (string) await command.ExecuteScalarAsync();
                }
                catch
                {
                    Console.WriteLine("Error in Query");
                    return null;
                }
            }
        }

        ///// <summary>
        ///// Execute SQL command with multiple rows
        ///// </summary>
        ///// <param name="sqlCommand">sql query</param>
        ///// <param name="parameters">sql query parameters</param>
        ///// <returns>sql query results</returns>
        //private static List<string> GetValues(string sqlCommand, params Tuple<string, string>[] parameters)
        //{
        //    using (SqlConnection db = Connect())
        //    {
        //        SqlCommand command = new SqlCommand(sqlCommand, db);
        //        foreach (Tuple<string, string> param in parameters) command.Parameters.AddWithValue(param.Item1, param.Item2);
        //        command.Connection.Open();
        //        SqlDataReader reader = command.ExecuteReader();
        //        List<string> values = new List<string>();
        //        while (reader.Read()) values.Add(reader.GetString(0));
        //        return values;
        //    }
        //}


        private static async Task<List<string>> GetValuesAsync(string sqlCommand, params Tuple<string, string>[] parameters)
        {
            using (SqlConnection db = Connect())
            {
                SqlCommand command = new SqlCommand(sqlCommand, db);
                foreach (Tuple<string, string> param in parameters) command.Parameters.AddWithValue(param.Item1, param.Item2);
                await command.Connection.OpenAsync();
                SqlDataReader reader = await command.ExecuteReaderAsync();
                List<string> values = new List<string>();
                while (await reader.ReadAsync()) values.Add(reader.GetString(0));
                return values;
            }
        }

        /// <summary>
        /// Get address db and cache it
        /// </summary>
        internal static async Task GetDBData()
        {
            try
            {
                using (SqlConnection db = Connect())
                {
                    SqlCommand query = new SqlCommand(@"SELECT state_pid, state_abbreviation, state_name FROM STATE
                                                      SELECT code, name FROM STREET_TYPE_AUT
                                                      SELECT * From (SELECT DISTINCT l.locality_pid, l.locality_name, l.state_pid, coalesce(coalesce(ad.postcode, l.primary_postcode), '') postcode 
                                                      FROM LOCALITY l LEFT JOIN (SELECT DISTINCT locality_pid, postcode FROM ADDRESS_DETAIL) ad on l.locality_pid = ad.locality_pid) T WHERE postcode IS NULL or postcode NOT LIKE '9%'", db);
                    await query.Connection.OpenAsync();
                    SqlDataReader reader = await query.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        string statePid = reader.GetString(0);
                        int stateId;
                        int.TryParse(statePid, out stateId);

                        State state = new State()
                        {
                            Pid = stateId,
                            abbreviation = reader.GetString(1),
                            name = reader.GetString(2)
                        };

                        states.Add(state.abbreviation, state);
                    }

                    if (await reader.NextResultAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            StreeType type = new StreeType 
                            { 
                                code = reader.GetString(0), 
                                Name = reader.GetString(1) 
                            };

                            types.Add(type.code, type);
                        }
                    }

                    if (await reader.NextResultAsync())
                    {
                        while (reader.Read())
                        {
                            string statePid = reader.GetString(2);
                            int stateId;
                            int.TryParse(statePid, out stateId);

                            var loc = new Locality()
                            {
                                Name = reader.GetString(1),
                                Pid = reader.GetString(0),
                                StateId = stateId,
                                Postcode = reader.GetString(3)
                            };

                            if (!localities.ContainsKey(loc.StateId)) localities.Add(loc.StateId, new SortedList<string, Locality>());
                            if (!postcodeLocalities.ContainsKey(loc.Postcode)) postcodeLocalities.Add(loc.Postcode, new SortedList<string, Locality>());
                            
                            postcodeLocalities[loc.Postcode].Add(loc.Pid, loc);
                            
                            if (localities[loc.StateId].ContainsKey(loc.Pid))
                            {
                                var temp = localities[loc.StateId][loc.Pid];
                                temp.Postcodes.Add(loc.Postcode);
                            }
                            else localities[loc.StateId].Add(loc.Pid, loc);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }


        /// <summary>
        /// Get street type code
        /// </summary>
        /// <param name="type">abbrevation code</param>
        /// <returns></returns>
        private static string GetStreetType(string type)
        {
            string rval;
            type = type.ToUpper();
            if (types.ContainsKey(type)) rval = types[type].code;
            else rval = types.Values.ToList().FirstOrDefault(v => v.code.StartsWith(type))?.code;
            return rval;
        }


        /// <summary>
        /// Get street
        /// </summary>
        /// <param name="localityId">locality id</param>
        /// <param name="street">street name</param>
        /// <returns>street id</returns>
        private static async Task<List<string>> GetStreet(string state, string localityId, string street, bool ignoreType)
        {
            string[] streetName = street.Split();
            string type = null;
            if (!ignoreType) type = GetStreetType(streetName[streetName.Length - 1]);

            string streetLocalityIdExact = @"SELECT street_locality_pid FROM STREET_LOCALITY
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
                streetLocalityIdExact = @"SELECT street_locality_pid FROM STREET_LOCALITY
                                        WHERE street_name = @name AND locality_pid = @localityId AND street_type_code = @type AND street_locality_pid LIKE @state + '%'";

                sqlParams = sqlParams.Append(Tuple.Create("@type", type)).ToArray();
            }

            List<string> streetId = new List<string>();
            if (localityId != null) streetId = await GetValuesAsync(streetLocalityIdExact, sqlParams.Concat(extraPramas).ToArray());

            if (streetId.Count == 0)
            {
                string streetLocalityIdWithoutLocalityExact = @"SELECT street_locality_pid FROM STREET_LOCALITY
                                                              WHERE street_name = @name AND street_locality_pid LIKE @state + '%'";

                if (type != null) streetLocalityIdWithoutLocalityExact = @"SELECT street_locality_pid FROM STREET_LOCALITY
                                                                         WHERE street_name = @name AND street_type_code = @type AND street_locality_pid LIKE @state + '%'";

                streetId = await GetValuesAsync(streetLocalityIdWithoutLocalityExact, sqlParams);
            }

            if (streetId.Count == 0)
            {
                string streetLocalityIdDifference = @"SELECT street_locality_pid FROM STREET_LOCALITY
                                                    WHERE DIFFERENCE(street_name, @name) > 2 AND locality_pid = @localityId AND street_locality_pid LIKE @state + '%' 
                                                    ORDER BY [PSMA_G-NAF].[dbo].[Distance](street_name, @name) asc, DIFFERENCE(street_name, @name) desc";

                if (type != null) streetLocalityIdDifference = @"SELECT street_locality_pid FROM STREET_LOCALITY
                                                               WHERE DIFFERENCE(street_name, @name) > 2 AND locality_pid = @localityId AND street_type_code = @type AND street_locality_pid LIKE @state + '%'
                                                               ORDER BY [PSMA_G-NAF].[dbo].[Distance](street_name, @name) asc, DIFFERENCE(street_name, @name) desc";

                if (localityId != null) streetId = await GetValuesAsync(streetLocalityIdDifference, sqlParams.Concat(extraPramas).ToArray());
            }

            if (streetId.Count == 0)
            {
                string streetLocalityIdWithoutLocalityDifference = @"SELECT street_locality_pid FROM STREET_LOCALITY 
                                                                   WHERE DIFFERENCE(street_name, @name) > 2 AND street_locality_pid LIKE @state + '%'
                                                                   ORDER BY [PSMA_G-NAF].[dbo].[Distance](street_name, @name) asc, DIFFERENCE(street_name, @name) desc";

                if (type != null) streetLocalityIdWithoutLocalityDifference = @"SELECT street_locality_pid FROM STREET_LOCALITY 
                                                                              WHERE DIFFERENCE(street_name, @name) > 2 AND street_type_code = @type AND street_locality_pid LIKE @state + '%'
                                                                              ORDER BY [PSMA_G-NAF].[dbo].[Distance](street_name, @name) asc, DIFFERENCE(street_name, @name) desc";

                streetId = await GetValuesAsync(streetLocalityIdWithoutLocalityDifference, sqlParams);
            }
            return streetId;
        }

        /// <summary>
        /// Get house address
        /// </summary>
        /// <param name="streetId">street locality id</param>
        /// <param name="streetNumber">street number</param>
        /// <returns></returns>
        private static async Task<string> GetHouseAddress(string streetId, string streetNumber, SqlConnection db, int timeout)
        {
            string addressIdExact = @"SELECT TOP(1) address_detail_pid FROM ADDRESS_DETAIL
                                    WHERE number_first = @houseNumber AND street_locality_pid = @streetId";

            string addressIdRange = @"SELECT TOP(1) address_detail_pid FROM ADDRESS_DETAIL
                                    WHERE street_locality_pid = @streetId AND @houseNumber BETWEEN number_first and number_last";

            Tuple<string, string>[] sqlParams =
            {
                Tuple.Create("@streetId", streetId),
                Tuple.Create("@houseNumber", streetNumber)
            };
            string houseId = await GetValueAsync(addressIdExact, sqlParams, db, timeout);
            if (houseId == null) houseId = await GetValueAsync(addressIdRange, sqlParams, db, timeout);
            return houseId;
        }

        private static async Task<string> GetAddress(string streetId, string streetNumber, int count, Address add)
        {
            using(SqlConnection db = Connect())
            {
                db.Open();
                int timeout = count * 3;
                if (Regex.IsMatch(streetNumber, @"^[0-9]+$"))
                {
                    string addressId = await GetHouseAddress(streetId, streetNumber, db, timeout);

                    string addressIdExact = @"SELECT TOP(1) address_detail_pid from ADDRESS_DETAIL
                                            WHERE flat_number = @flatNumber AND street_locality_pid = @streetId";

                    Tuple<string, string>[] sqlParams =
                    {
                        Tuple.Create("@flatNumber", streetNumber),
                        Tuple.Create("@streetId", streetId)
                    };

                    if (addressId == null) addressId = await GetValueAsync(addressIdExact, sqlParams, db, timeout);
                    return addressId;
                }
                else if (Regex.IsMatch(streetNumber, @"^[0-9]+[A-z]{1}$"))
                {

                    string addressIdExact = @"SELECT TOP(1) address_detail_pid FROM ADDRESS_DETAIL
                                            WHERE number_first = @houseNumber AND number_first_suffix = @suffix AND street_locality_pid = @streetId";

                    Tuple<string, string>[] sqlParams =
                    {
                        Tuple.Create("@streetId", streetId),
                        Tuple.Create("@houseNumber", streetNumber.Remove(streetNumber.Length - 1)),
                        Tuple.Create("@suffix", streetNumber[streetNumber.Length - 1].ToString())
                    };

                    return await GetValueAsync(addressIdExact, sqlParams, db, timeout);
                }
                else if (Regex.IsMatch(streetNumber, @"-"))
                {
                    string first = Regex.Match(streetNumber, @"(\d+)(?=-)").Value.Trim();
                    string last = Regex.Match(streetNumber, @"(?<=-)(\d+)").Value.Trim();

                    string addressIdExact = @"SELECT TOP(1) address_detail_pid FROM ADDRESS_DETAIL
                                            WHERE number_first = @first AND number_last = @last AND street_locality_pid = @streetId";

                    Tuple<string, string>[] sqlParams =
                    {
                        Tuple.Create("@streetId", streetId),
                        Tuple.Create("@first", first),
                        Tuple.Create("@last", last)
                    };

                    string addressId = await GetValueAsync(addressIdExact, sqlParams, db, timeout);

                    if (addressId == null) addressId = await GetHouseAddress(streetId, first, db, timeout);
                    if (addressId == null) addressId = await GetHouseAddress(streetId, last, db, timeout);
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
                            string addressIdExact = @"SELECT TOP(1) address_detail_pid FROM ADDRESS_DETAIL
                                                    WHERE number_first = @houseNumber AND number_first_suffix = @suffix AND flat_number = @flatNumber AND street_locality_pid = @streetId";

                            Tuple<string, string>[] sqlParams =
                            {
                                Tuple.Create("@houseNumber", house.Remove(house.Length - 1)),
                                Tuple.Create("@suffix", house[house.Length - 1].ToString()),
                                Tuple.Create("@flatNumber", flat),
                                Tuple.Create("@streetId", streetId)
                            };

                            return await GetValueAsync(addressIdExact, sqlParams, db, timeout);
                        }
                        else
                        {
                            string addressIdExact = @"SELECT TOP(1) address_detail_pid FROM ADDRESS_DETAIL
                                                    WHERE number_first = @houseNumber AND flat_number = @flatNumber AND street_locality_pid = @streetId";

                            Tuple<string, string>[] sqlParams =
                            {
                                Tuple.Create("@houseNumber", house),
                                Tuple.Create("@flatNumber", flat),
                                Tuple.Create("@streetId", streetId)
                            };

                            string addressId = await GetValueAsync(addressIdExact, sqlParams, db, timeout);
                            if (addressId == null) return await GetHouseAddress(streetId, house, db, timeout);
                            return addressId;
                        }
                    }
                    else if (Regex.IsMatch(flat, @"^[0-9]+[A-z]{1}$"))
                    {
                        if (Regex.IsMatch(house, @"^[0-9]+[A-z]{1}$"))
                        {
                            string flatWithLetter = Regex.Match(flat, @"[0-9]+[A-z]{1}$").Value;

                            string addressIdExact = @"SELECT TOP(1) address_detail_pid FROM ADDRESS_DETAIL
                                                WHERE number_first = @houseNumber AND number_first_suffix = @suffix AND flat_number = @flatNumber AND flat_number_suffix = @flatSuffix AND street_locality_pid = @streetId";

                            Tuple<string, string>[] sqlParams =
                            {
                                Tuple.Create("@houseNumber", house.Remove(house.Length - 1)),
                                Tuple.Create("@suffix", house[house.Length - 1].ToString()),
                                Tuple.Create("@flatNumber", flatWithLetter.Remove(flatWithLetter.Length - 1)),
                                Tuple.Create("@flatSuffix", flatWithLetter[flatWithLetter.Length - 1].ToString()),
                                Tuple.Create("@streetId", streetId)
                            };

                            return await GetValueAsync(addressIdExact, sqlParams, db, timeout);
                        }
                        else
                        {
                            string flatWithLetter = Regex.Match(flat, @"[0-9]+[A-z]{1}$").Value;

                            string addressIdExact = @"SELECT TOP(1) address_detail_pid FROM ADDRESS_DETAIL
                                                WHERE number_first = @houseNumber AND flat_number = @flatNumber AND flat_number_suffix = @suffix AND street_locality_pid = @streetId";

                            Tuple<string, string>[] sqlParams =
                            {
                                Tuple.Create("@houseNumber", house),
                                Tuple.Create("@flatNumber", flatWithLetter.Remove(flatWithLetter.Length - 1)),
                                Tuple.Create("@suffix", flatWithLetter[flatWithLetter.Length - 1].ToString()),
                                Tuple.Create("@streetId", streetId)
                            };

                            string addressId = await GetValueAsync(addressIdExact, sqlParams, db, timeout);
                            if (addressId == null) return await GetHouseAddress(streetId, house, db, timeout);
                            return addressId;
                        }
                    }

                }
            }
            return null;
        }

        private static async Task<List<string>> AddressIds(Address address, string localityId)
        {
            List<string> streetIds = await GetStreet(address.State, localityId, address.StreetData.Item1, false);
            ConcurrentBag<string> addressBag = new ConcurrentBag<string>();
            List<string> addressIds = new List<string>();

            if (streetIds.Count == 0) streetIds = await GetStreet(address.State, localityId, address.StreetData.Item1, true);
            
            if (streetIds.Count < 50)
            {
                streetIds.ForEach(async streetId => addressBag.Add(await GetAddress(streetId, address.StreetData.Item2, streetIds.Count, address)));
                //Parallel.ForEach(streetIds, async streetId => addressBag.Add(await GetAddress(streetId, address.StreetData.Item2, streetIds.Count, address)));
                addressIds = addressBag.ToArray().Where(s => !string.IsNullOrEmpty(s)).Distinct().ToList();

                if (addressIds.Count == 0)
                {
                    streetIds = await GetStreet(address.State, localityId, address.StreetData.Item1, true);
                    if (streetIds.Count < 50)
                    {
                        streetIds.ForEach(async streetId => addressBag.Add(await GetAddress(streetId, address.StreetData.Item2, streetIds.Count, address)));
                        //Parallel.ForEach(streetIds, async streetId => addressBag.Add(await GetAddress(streetId, address.StreetData.Item2, streetIds.Count, address)));
                        addressIds = addressBag.ToList().Where(s => !string.IsNullOrEmpty(s)).Distinct().ToList();

                    }
                    else
                    {
                        Console.WriteLine($"Too many streets - {streetIds.Count}");
                    }
                }
            }
            else
            {
                Console.WriteLine($"Too many streets - {streetIds.Count}");
            }
            return addressIds;
        }

        /// <summary>
        /// Get address id of address in Australia
        /// </summary>
        /// <param name="state">state</param>
        /// <param name="locality">suburb</param>
        /// <param name="street">street name</param>
        /// <param name="db">database connection</param>
        /// <returns>street locality id if found otherwise null</returns>
        internal static async Task GetAddressIds(List<Address> addresses)
        {

            foreach (Address address in addresses)
            {
                var each = System.Diagnostics.Stopwatch.StartNew();
                int stateId;

                if (states.ContainsKey(address.State))
                {
                    stateId = states[address.State].Pid;
                }
                else
                {
                    stateId = states.Values.ToList().Find(s => s.name == address.State.ToUpper()).Pid;
                }

                var memTime = System.Diagnostics.Stopwatch.StartNew();
                string localityId;
                var stateLocalities = localities[stateId];

                if (stateLocalities.ContainsKey(address.Locality))
                {
                    localityId = stateLocalities[address.Locality].Pid;
                }
                else
                {
                    var postcodeLoc = postcodeLocalities[address.Postcode].Values.ToList();
                    var distances = postcodeLoc.ConvertAll(loc => Levenshtein.Distance(loc.Name, address.Locality));
                    localityId = postcodeLoc[distances.IndexOf(distances.Min())].Pid;
                }
                memTime.Stop();

                var addressIds = await AddressIds(address, localityId);
                each.Stop();
                Console.WriteLine($"{addressIds.Count} Matches - {each.ElapsedMilliseconds} ms, mem - {memTime.ElapsedMilliseconds} ms");
                address.AddressIds = addressIds;
            }
        }

        internal static void UpdateAddressList(List<Address> addresses)
        {
            using(SqlConnection db = ConnectLoyaltyDB())
            {
                db.Open();
                StringBuilder bulkQuery = new StringBuilder();

                foreach(var address in addresses)
                {
                    if (address.AddressIds == null || address.AddressIds.Count == 0)
                    {
                        bulkQuery.AppendLine($@"INSERT INTO [KIALSVR05].[Loyalty].[dbo].[CustomerAddress_NORMALIZED] ([CustomerID], [ProcessedOn])
                                             VALUES ({address.CustomerId}, '{DateTime.Now}')");
                    }
                    else if (address.AddressIds.Count == 1)
                    {
                        bulkQuery.AppendLine($@"INSERT INTO [KIALSVR05].[Loyalty].[dbo].[CustomerAddress_NORMALIZED] ([CustomerID], [ProcessedOn], [GnafDetailPid])
                                             VALUES ({address.CustomerId}, '{DateTime.Now}', '{address.AddressIds.First()}')");
                    } 
                    else
                    {
                        for (int i = 0; i < address.AddressIds.Count; i++)
                        {
                            bulkQuery.AppendLine($@"INSERT INTO [dbo].[CustomerAddress_NORMALIZED_Extra] ([CustomerID], [ProcessedOn], [GnafDetailPid])
                                                 VALUES ({address.CustomerId}, '{DateTime.Now}', '{address.AddressIds[i]}')");
                        }
                    }
                }

                SqlCommand command = new SqlCommand(bulkQuery.ToString(), db);
                command.ExecuteNonQuery();
            }
        }
        internal static List<Address> GetAddresses()
        {
            List<Address> addresses = new List<Address>();
            using (SqlConnection db = ConnectLoyaltyDB())
            {
                db.Open();
                string query = @"SELECT TOP (500)
                                c.CustomerID,
                                c.AddressLine1,
                                c.AddressLine2,
                                c.Suburb,
                                c.State,
                                c.PostCode
                                FROM Customer c
                                LEFT JOIN CustomerAddress_NORMALIZED CAN on can.CustomerID = c.CustomerID
                                WHERE CAN.ProcessedOn IS NULL
                                ORDER BY c.CustomerID";

                SqlCommand command = new SqlCommand(query, db);
                SqlDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    Address item = new Address();
                    if (!reader.IsDBNull(0)) item.CustomerId = reader.GetInt64(0);
                    if (!reader.IsDBNull(1)) item.AddressLine = reader.GetString(1);
                    //if (!reader.IsDBNull(2))  
                    if (!reader.IsDBNull(3)) item.Locality = reader.GetString(3);
                    if (!reader.IsDBNull(4)) item.State = reader.GetString(4);
                    if (!reader.IsDBNull(5)) item.Postcode = reader.GetString(5);
                    if (item.CustomerId > 0 && !string.IsNullOrWhiteSpace(item.StreetData.Item1) && !string.IsNullOrWhiteSpace(item.StreetData.Item2) 
                        && !string.IsNullOrWhiteSpace(item.Locality) && !string.IsNullOrWhiteSpace(item.State) && !string.IsNullOrWhiteSpace(item.Postcode)) addresses.Add(item);
                }
            }

            return addresses;
        }
    }

    internal class Address
    {
        public long CustomerId { get; set; }

        private string state;
        public string State { get { return state; } set { state = RemoveSpaces(value); } }

        private string locality;
        public string Locality { get { return locality; } set { locality = RemoveSpaces(value); } }
        public string AddressLine { set { StreetData = StreetName(value); } }
        public string Postcode { get; set; }

        public List<string> AddressIds { get; set; }

        public Tuple<string, string> StreetData { get; private set; }
        private static Tuple<string, string> StreetName(string streetCombined)
        {
            Match numberCheck = Regex.Match(streetCombined, @"\d+");
            Match streetNumber = Regex.Match(streetCombined, @"(.*)\d+[A-z]?");
            Match postbpox = Regex.Match(streetCombined, @"P\.*O\.* BOX", RegexOptions.IgnoreCase);

            if (!numberCheck.Success) return Tuple.Create(streetCombined, streetNumber.Value);
            if (streetNumber.Success && !postbpox.Success) return Tuple.Create(RemoveSpaces(streetCombined.Substring(streetNumber.Index + streetNumber.Length).Trim()), streetNumber.Value);
            return Tuple.Create<string, string>(null, streetNumber.Value);
        }

        private static string RemoveSpaces(string str) => Regex.Replace(str, @"\s+|/", @" ").Trim();
    }

    internal class Locality
    {
        public string Pid { get; set; }
        public string Name { get; set; }
        public int StateId { get; set; }
        public string Postcode { get; set; }
        public List<string> Postcodes { get; set; }

        public Locality()
        {
            Postcodes = new List<string>();
        }
    }

    internal class State
    {
        public int Pid { get; set; }
        public string abbreviation { get; set; }
        public string name { get; set; }
    }

    internal class StreeType
    {
        public string Name { get; set; }
        public string code { get; set; }
    }
}
