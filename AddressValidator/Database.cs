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
        private static SortedList<int, SortedList<string, SortedList<string, Locality>>> nameLocalities;
        private static SortedList<int, SortedList<string, Locality>> stateLocalities;
        private static SortedList<string, Locality> localities;
        private static SortedList<string, StreeType> types;
        private static SortedList<string, StreetLocality> streets;
        private static SortedList<string, SortedList<string, List<StreetLocality>>> streetLocalities;
        private static SortedList<string, SortedList<string, List<StreetLocality>>> streetLocalitiesAlias;
        private static SortedList<string, StreeType> streetSuffix;

        static Database()
        {
            states = new SortedList<string, State>();
            postcodeLocalities = new SortedList<string, SortedList<string, Locality>>();
            nameLocalities = new SortedList<int, SortedList<string, SortedList<string, Locality>>>();
            stateLocalities = new SortedList<int, SortedList<string, Locality>>();
            localities = new SortedList<string, Locality>();
            types = new SortedList<string, StreeType>();
            streets = new SortedList<string, StreetLocality>();
            streetLocalities = new SortedList<string, SortedList<string, List<StreetLocality>>>();
            streetLocalitiesAlias = new SortedList<string, SortedList<string, List<StreetLocality>>>();
            streetSuffix = new SortedList<string, StreeType>();
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

        /// <summary>
        /// Execute SQL command with single row
        /// </summary>
        /// <param name="sqlCommand">sql query</param>
        /// <param name="parameters">sql query parameters</param>
        /// <param name="dbc">optional db connection</param>
        /// <param name="timemout">db connection timeout</param>
        /// <returns>sql query result</returns>
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

        /// <summary>
        /// Execute SQL command with multiple rows
        /// </summary>
        /// <param name="sqlCommand">sql query</param>
        /// <param name="parameters">sql query parameters</param>
        /// <returns>sql query results</returns>
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
                                                      SELECT DISTINCT l.locality_pid, l.locality_name, l.state_pid, coalesce(coalesce(ad.postcode, l.primary_postcode), '') postcode, locality_class_code 
                                                      FROM LOCALITY l LEFT JOIN (SELECT DISTINCT locality_pid, postcode FROM ADDRESS_DETAIL) ad on l.locality_pid = ad.locality_pid
                                                      SELECT oitpostcode, name, stateId, pid, created, reason from OITPOSTCODES
                                                      SELECT street_locality_pid, street_name, street_type_code, street_suffix_code, locality_pid from STREET_LOCALITY WHERE date_retired IS NULL
                                                      SELECT street_locality_pid, street_name, street_type_code, street_suffix_code from STREET_LOCALITY_ALIAS WHERE date_retired IS NULL
                                                      SELECT locality_pid, name, postcode, state_pid FROM LOCALITY_ALIAS WHERE date_retired IS NULL
                                                      SELECT code, name FROM STREET_SUFFIX_AUT", db);
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
                            Abbreviation = reader.GetString(1),
                            Name = reader.GetString(2)
                        };

                        states.Add(state.Abbreviation, state);
                    }

                    if (await reader.NextResultAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            StreeType type = new StreeType 
                            { 
                                Code = reader.GetString(0), 
                                Name = reader.GetString(1) 
                            };

                            types.Add(type.Code, type);
                        }
                    }

                    if (await reader.NextResultAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            string statePid = reader.GetString(2);
                            int stateId;
                            int.TryParse(statePid, out stateId);

                            var loc = new Locality()
                            {
                                Name = reader.GetString(1),
                                Pid = reader.GetString(0),
                                StateId = stateId,
                                Postcode = reader.GetString(3).PadLeft(4, '0')
                            };

                            string localityCode = reader.GetString(4);
                            switch(localityCode)
                            {
                                case "G":
                                    loc.ClassCodeOrder = 0;
                                    break;
                                case "I":
                                    loc.ClassCodeOrder = 1;
                                    break;
                                case "H":
                                    loc.ClassCodeOrder = 2;
                                    break;
                                case "T":
                                    loc.ClassCodeOrder = 3;
                                    break;
                                case "D":
                                    loc.ClassCodeOrder = 4;
                                    break;
                                case "U":
                                    loc.ClassCodeOrder = 6;
                                    break;
                                default:
                                    loc.ClassCodeOrder = 5;
                                    break;
                            }

                            if (!localities.ContainsKey(loc.Pid)) localities.Add(loc.Pid, loc);
                            else localities[loc.Pid].Postcodes.Add(loc.Postcode);

                            if (!stateLocalities.ContainsKey(loc.StateId)) stateLocalities.Add(loc.StateId, new SortedList<string, Locality>());
                            if (!postcodeLocalities.ContainsKey(loc.Postcode)) postcodeLocalities.Add(loc.Postcode, new SortedList<string, Locality>());
                            if (!nameLocalities.ContainsKey(loc.StateId)) nameLocalities.Add(loc.StateId, new SortedList<string, SortedList<string, Locality>>());
                            if (!nameLocalities[loc.StateId].ContainsKey(loc.Name)) nameLocalities[loc.StateId].Add(loc.Name, new SortedList<string, Locality>());

                            if (!nameLocalities[loc.StateId][loc.Name].ContainsKey(loc.Pid)) nameLocalities[loc.StateId][loc.Name].Add(loc.Pid, loc);
                            else
                            {
                                var locAdd = nameLocalities[loc.StateId][loc.Name][loc.Pid];
                                if (!locAdd.Postcodes.Contains(loc.Postcode))
                                {
                                    locAdd.Postcodes.Add(loc.Postcode);
                                }
                            }

                            postcodeLocalities[loc.Postcode].Add(loc.Pid, loc);

                            if (stateLocalities[loc.StateId].ContainsKey(loc.Pid))
                            {
                                var temp = stateLocalities[loc.StateId][loc.Pid];
                                temp.Postcodes.Add(loc.Postcode);
                            }
                            else stateLocalities[loc.StateId].Add(loc.Pid, loc);
                        }
                    }

                    if (await reader.NextResultAsync())
                    {
                        while(await reader.ReadAsync())
                        {
                            var loc = new Locality()
                            {
                                Pid = reader.GetString(3),
                                Name = reader.GetString(1),
                                StateId = reader.GetByte(2),
                                Postcode = reader.GetInt16(0).ToString("0000")
                            };

                            if (!stateLocalities.ContainsKey(loc.StateId)) stateLocalities.Add(loc.StateId, new SortedList<string, Locality>());
                            if (!postcodeLocalities.ContainsKey(loc.Postcode)) postcodeLocalities.Add(loc.Postcode, new SortedList<string, Locality>());
                            
                            postcodeLocalities[loc.Postcode].Add(loc.Pid, loc);

                            if (stateLocalities[loc.StateId].ContainsKey(loc.Pid))
                            {
                                var temp = stateLocalities[loc.StateId][loc.Pid];
                                temp.Postcodes.Add(loc.Postcode);
                            }
                            else stateLocalities[loc.StateId].Add(loc.Pid, loc);
                        }
                    }

                    if (await reader.NextResultAsync())
                    {
                        while(await reader.ReadAsync())
                        {
                            string localityId = reader.GetString(4);
                            var streetLoc = new StreetLocality()
                            {
                                Pid = reader.GetString(0),
                                Name = reader.GetString(1),
                                Type = "",
                                Suffix = ""
                            };

                            if (!reader.IsDBNull(2)) streetLoc.Type = reader.GetString(2);
                            if (!reader.IsDBNull(3)) streetLoc.Suffix = reader.GetString(3);

                            streets.Add(streetLoc.Pid, streetLoc);

                            if (localities.ContainsKey(localityId))
                            {
                                streetLoc.Locality = localities[localityId];
                                if (!streetLocalities.ContainsKey(localityId)) streetLocalities.Add(localityId, new SortedList<string, List<StreetLocality>>());
                                if (!streetLocalities[localityId].ContainsKey(streetLoc.Name)) streetLocalities[localityId].Add(streetLoc.Name, new List<StreetLocality>());
                                streetLocalities[localityId][streetLoc.Name].Add(streetLoc);
                            }
                            else
                            {
                                string whyTho = ":(";
                            }
                        }
                    }

                    if (await reader.NextResultAsync())
                    {
                        while(await reader.ReadAsync())
                        {
                            var streetAlias = new StreetLocality()
                            {
                                Pid = reader.GetString(0),
                                Name = reader.GetString(1),
                                Type = "",
                                Suffix = ""
                            };

                            if (!reader.IsDBNull(2)) streetAlias.Type = reader.GetString(2);
                            if (!reader.IsDBNull(3)) streetAlias.Suffix = reader.GetString(3);

                            if (streets.ContainsKey(streetAlias.Pid))
                            {
                                streetAlias.Locality = streets[streetAlias.Pid].Locality;
                                if (!streetLocalitiesAlias.ContainsKey(streetAlias.Locality.Pid)) streetLocalitiesAlias.Add(streetAlias.Locality.Pid, new SortedList<string, List<StreetLocality>>());
                                if (!streetLocalitiesAlias[streetAlias.Locality.Pid].ContainsKey(streetAlias.Name)) streetLocalitiesAlias[streetAlias.Locality.Pid].Add(streetAlias.Name, new List<StreetLocality>());
                                streetLocalitiesAlias[streetAlias.Locality.Pid][streetAlias.Name].Add(streetAlias);
                            }
                            else
                            {
                                string whyTho = ":(";
                            }
                        }
                    }

                    if (await reader.NextResultAsync())
                    {
                        while(await reader.ReadAsync())
                        {
                            string statePid = reader.GetString(3);
                            int stateId;
                            int.TryParse(statePid, out stateId);

                            var loc = new Locality()
                            {
                                Pid = reader.GetString(0),
                                Name = reader.GetString(1),
                                StateId = stateId,
                                Postcode = ""
                            };

                            if (!reader.IsDBNull(2)) loc.Postcode = reader.GetString(2).PadLeft(4, '0');

                            if (!localities.ContainsKey(loc.Pid))
                            {
                                localities.Add(loc.Pid, loc);
                            }
                            else
                            {
                                if (!string.IsNullOrWhiteSpace(loc.Postcode) && !localities[loc.Pid].Postcodes.Contains(loc.Postcode)) localities[loc.Pid].Postcodes.Add(loc.Postcode);
                                else if (string.IsNullOrWhiteSpace(loc.Postcode)) loc.Postcode = localities[loc.Pid].Postcode;
                            }

                            if (!postcodeLocalities.ContainsKey(loc.Postcode)) postcodeLocalities.Add(loc.Postcode, new SortedList<string, Locality>());
                            if (!nameLocalities[loc.StateId].ContainsKey(loc.Name)) nameLocalities[loc.StateId].Add(loc.Name, new SortedList<string, Locality>());

                            if (!nameLocalities[loc.StateId][loc.Name].ContainsKey(loc.Pid)) nameLocalities[loc.StateId][loc.Name].Add(loc.Pid, loc);
                            else
                            {
                                var locAdd = nameLocalities[loc.StateId][loc.Name][loc.Pid];
                                if (!loc.Postcodes.Contains(loc.Postcode))
                                {
                                    locAdd.Postcodes.Add(loc.Postcode);
                                }
                            }


                            if (!postcodeLocalities.ContainsKey(loc.Postcode)) postcodeLocalities[loc.Postcode].Add(loc.Pid, loc);

                            if (stateLocalities[loc.StateId].ContainsKey(loc.Pid))
                            {
                                var temp = stateLocalities[loc.StateId][loc.Pid];
                                temp.Postcodes.Add(loc.Postcode);
                            }
                            else stateLocalities[loc.StateId].Add(loc.Pid, loc);
                        }
                    }

                    if (await reader.NextResultAsync())
                    {
                        while(await reader.ReadAsync())
                        {
                            StreeType suffix = new StreeType()
                            {
                                Code = reader.GetString(0),
                                Name = reader.GetString(1)
                            };
                            streetSuffix.Add(suffix.Code, suffix);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Cache Error: {e.Message}");
            }
            finally
            {
                Console.WriteLine("Cache Loaded");
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
            if (types.ContainsKey(type)) rval = types[type].Code;
            else rval = types.Values.ToList().FirstOrDefault(v => v.Code.StartsWith(type))?.Code;
            return rval;
        }


        /// <summary>
        /// Get street
        /// </summary>
        /// <param name="localityId">locality id</param>
        /// <param name="street">street name</param>
        /// <returns>street id</returns>
        private static List<StreetLocality> GetStreet(Address address, Locality locality, string street, bool ignoreType)
        {
            string[] streetName = street.ToUpper().Split();
            string type = ignoreType ? null : GetStreetType(streetName[streetName.Length - 1]);
            string name = type != null ? string.Join(" ", streetName.Take(streetName.Length - 1)) : street;
            List<StreetLocality> matchedStreets = new List<StreetLocality>();

            if (streetLocalities.ContainsKey(locality.Pid) && streetLocalities[locality.Pid].ContainsKey(name))
            {
                matchedStreets = streetLocalities[locality.Pid][name];

                if (type != null && matchedStreets.Count > 1)
                {
                    var temp = matchedStreets.Where(s => s.Type == type).ToList();
                    if (temp.Count > 0) matchedStreets = temp;
                }
            }

            if (matchedStreets.Count == 0 && streetLocalitiesAlias.ContainsKey(locality.Pid) && streetLocalitiesAlias[locality.Pid].ContainsKey(name))
            {
                matchedStreets = streetLocalitiesAlias[locality.Pid][name];

                if (type != null && matchedStreets.Count > 1)
                {
                    var temp = matchedStreets.Where(s => s.Type == type).ToList();
                    if (temp.Count > 0) matchedStreets = temp;
                }
            }

            if (matchedStreets.Count == 0)
            {
                matchedStreets = streets.Values.Where(s => s.Name == name).ToList();

                if (type != null && matchedStreets.Count > 1)
                {
                    if (matchedStreets.Count > 1)
                    {
                        var temp = matchedStreets.Where(s => s.Locality.StateId == locality.StateId).ToList();
                        if (temp.Count > 0) matchedStreets = temp;
                    }

                    if (matchedStreets.Count > 1)
                    {
                        var temp = matchedStreets.Where(s => s.Type == type).ToList();
                        if (temp.Count > 0) matchedStreets = temp;
                    }
                }
            }

            if (streets.Count == 0 && streetLocalities.ContainsKey(locality.Pid) && streetName.Length > 1)
            {
                for (int i = streetName.Length - 1; i > 0; i--)
                {
                    if (streetSuffix.ContainsKey(streetName[i]))
                    {
                        var alias = string.Join(" ", streetName.Take(i - 1));

                        if (streetLocalities[locality.Pid].ContainsKey(alias))
                        {
                            matchedStreets = streetLocalities[locality.Pid][alias];

                            if (type != null && matchedStreets.Count > 1)
                            {
                                var temp = matchedStreets.FindAll(s => s.Type == type);
                                if (temp.Count > 0) matchedStreets = temp;
                            }
                        }
                    }
                }
            }
            
            if (matchedStreets.Count == 0 && streetLocalities.ContainsKey(locality.Pid))
            {
                var streetList = streetLocalities[locality.Pid];
                var distances = streetList.Select(s => new StreetLocalityDistance() { Distance = Levenshtein.Distance(s.Key, name), StreetLocality = s.Value }).OrderBy(x => x.Distance).ToList();
                
                if (distances.Count > 1)
                {
                    foreach(var item in distances)
                    {
                        var temp = item.StreetLocality.FindAll(s => s.Type == type);
                        if (temp.Count > 0)
                        {
                            matchedStreets = temp;
                            break;
                        }
                    }
                }
            }
            return matchedStreets;
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

        private static async Task<AddressLocality> GetAddress(StreetLocality street, string streetNumber, int count, Address add)
        {
            using(SqlConnection db = Connect())
            {
                db.Open();
                int timeout = count * 3;
                if (Regex.IsMatch(streetNumber, @"^[0-9]+$"))
                {
                    string addressId = await GetHouseAddress(street.Pid, streetNumber, db, timeout);

                    string addressIdExact = @"SELECT TOP(1) address_detail_pid from ADDRESS_DETAIL
                                            WHERE flat_number = @flatNumber AND street_locality_pid = @streetId";

                    Tuple<string, string>[] sqlParams =
                    {
                        Tuple.Create("@flatNumber", streetNumber),
                        Tuple.Create("@streetId", street.Pid)
                    };

                    if (addressId == null) addressId = await GetValueAsync(addressIdExact, sqlParams, db, timeout);
                    return new AddressLocality() { addressId = addressId, StreetLoc = street, CombinedStreet = "" };
                }
                else if (Regex.IsMatch(streetNumber, @"^[0-9]+[A-z]{1}$"))
                {

                    string addressIdExact = @"SELECT TOP(1) address_detail_pid FROM ADDRESS_DETAIL
                                            WHERE number_first = @houseNumber AND number_first_suffix = @suffix AND street_locality_pid = @streetId";

                    Tuple<string, string>[] sqlParams =
                    {
                        Tuple.Create("@streetId", street.Pid),
                        Tuple.Create("@houseNumber", streetNumber.Remove(streetNumber.Length - 1)),
                        Tuple.Create("@suffix", streetNumber[streetNumber.Length - 1].ToString())
                    };

                    string addressId = await GetValueAsync(addressIdExact, sqlParams, db, timeout);
                    return new AddressLocality() { addressId = addressId, StreetLoc = street, CombinedStreet = "" };
                }
                else if (Regex.IsMatch(streetNumber, @"-"))
                {
                    string first = Regex.Match(streetNumber, @"(\d+)(?=-)").Value.Trim();
                    string last = Regex.Match(streetNumber, @"(?<=-)(\d+)").Value.Trim();

                    string addressIdExact = @"SELECT TOP(1) address_detail_pid FROM ADDRESS_DETAIL
                                            WHERE number_first = @first AND number_last = @last AND street_locality_pid = @streetId";

                    Tuple<string, string>[] sqlParams =
                    {
                        Tuple.Create("@streetId", street.Pid),
                        Tuple.Create("@first", first),
                        Tuple.Create("@last", last)
                    };

                    string addressId = await GetValueAsync(addressIdExact, sqlParams, db, timeout);

                    if (addressId == null) addressId = await GetHouseAddress(street.Pid, first, db, timeout);
                    if (addressId == null) addressId = await GetHouseAddress(street.Pid, last, db, timeout);
                    return new AddressLocality() { addressId = addressId, StreetLoc = street, CombinedStreet = "" };
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
                                Tuple.Create("@streetId", street.Pid)
                            };

                            string addressId = await GetValueAsync(addressIdExact, sqlParams, db, timeout);
                            return new AddressLocality() { addressId = addressId, StreetLoc = street, CombinedStreet = "" };
                        }
                        else
                        {
                            string addressIdExact = @"SELECT TOP(1) address_detail_pid FROM ADDRESS_DETAIL
                                                    WHERE number_first = @houseNumber AND flat_number = @flatNumber AND street_locality_pid = @streetId";

                            Tuple<string, string>[] sqlParams =
                            {
                                Tuple.Create("@houseNumber", house),
                                Tuple.Create("@flatNumber", flat),
                                Tuple.Create("@streetId", street.Pid)
                            };

                            string addressId = await GetValueAsync(addressIdExact, sqlParams, db, timeout);
                            if (addressId == null) addressId = await GetHouseAddress(street.Pid, house, db, timeout);
                            return new AddressLocality() { addressId = addressId, StreetLoc = street, CombinedStreet = "" };
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
                                Tuple.Create("@streetId", street.Pid)
                            };

                            string addressId = await GetValueAsync(addressIdExact, sqlParams, db, timeout);
                            return new AddressLocality() { addressId = addressId, StreetLoc = street, CombinedStreet = "" };
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
                                Tuple.Create("@streetId", street.Pid)
                            };

                            string addressId = await GetValueAsync(addressIdExact, sqlParams, db, timeout);
                            if (addressId == null) addressId = await GetHouseAddress(street.Pid, house, db, timeout);
                            return new AddressLocality() { addressId = addressId, StreetLoc = street, CombinedStreet = "" };
                        }
                    }

                }
            }
            return null;
        }


        private static async Task<List<AddressLocality>> AddressIds(Address address, Locality locality)
        {
            var streets = GetStreet(address, locality, address.StreetData.Item1, false);
            ConcurrentBag<AddressLocality> addressBag = new ConcurrentBag<AddressLocality>();
            List<AddressLocality> addressIds = new List<AddressLocality>();

            if (streets.Count == 0) streets =  GetStreet(address, locality, address.StreetData.Item1, true);
            
            if (streets.Count < 50)
            {
                foreach (var street in streets) addressBag.Add(await GetAddress(street, address.StreetData.Item2, streets.Count, address));
                addressIds = addressBag.ToArray().Where(s => s != null && !string.IsNullOrEmpty(s.addressId)).Distinct().ToList();

                if (addressIds.Count == 0)
                {
                    streets = GetStreet(address, locality, address.StreetData.Item1, true);
                    if (streets.Count < 50)
                    {
                        foreach (var street in streets) addressBag.Add(await GetAddress(street, address.StreetData.Item2, streets.Count, address));
                        addressIds = addressBag.ToList().Where(s => s != null && !string.IsNullOrEmpty(s.addressId)).Distinct().ToList();

                    }
                    else
                    {
                        Console.WriteLine($"Too many streets - {streets.Count}");
                    }
                }
            }
            else
            {
                Console.WriteLine($"Too many streets - {streets.Count}");
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
            try
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
                        var state = states.Values.ToList().FirstOrDefault(s => s.Name == address.State.ToUpper());
                        if (state == null) state = states.Values.ToList().FirstOrDefault(s => s.Abbreviation == address.State.ToUpper());

                        if (state != null) stateId = state.Pid;
                        else
                        {
                            stateId = 0;
                            if (postcodeLocalities.ContainsKey(address.Postcode))
                            {
                                var postcodeLoc = postcodeLocalities[address.Postcode].Values.FirstOrDefault();
                                if (postcodeLoc != null) stateId = postcodeLoc.StateId;
                            }
                        }
                    }

                    var memTime = System.Diagnostics.Stopwatch.StartNew();
                    Locality locality = null;
                    var stateLocalities = Database.stateLocalities[stateId];

                    if (stateLocalities.ContainsKey(address.Locality))
                    {
                        locality = stateLocalities[address.Locality];
                    }
                    else
                    {
                        List<Locality> postcodeLoc = null;

                        if (postcodeLocalities.ContainsKey(address.Postcode))
                        {
                            postcodeLoc = postcodeLocalities[address.Postcode].Values.ToList();

                            if (postcodeLoc.Count > 1)
                            {
                                var postcodeLocName = postcodeLoc.FindAll(l => l.Name == address.Locality.ToUpper());
                                if (postcodeLocName.Count > 0) postcodeLoc = postcodeLocName;
                            }
                        }
                        else if (nameLocalities[stateId].ContainsKey(address.Locality.ToUpper())) postcodeLoc = nameLocalities[stateId][address.Locality.ToUpper()].Values.ToList();
                        else locality = null;

                        if (postcodeLoc != null && postcodeLoc.Count > 0)
                        {
                            if (postcodeLoc.Count != 1)
                            {
                                var distances = postcodeLoc.ConvertAll(l => Levenshtein.Distance(l.Name, address.Locality));
                                locality = postcodeLoc[distances.IndexOf(distances.Min())];
                            }
                            else locality = postcodeLoc.First();
                        }
                    }

                    memTime.Stop();

                    var addressIds = await AddressIds(address, locality);
                    each.Stop();
                    //Console.WriteLine($"{addressIds.Count} Matches - {each.ElapsedMilliseconds} ms, mem - {memTime.ElapsedMilliseconds} ms {address.CustomerId}");
                    address.AddressIds = addressIds;
                    Console.WriteLine($"Expected Locality: {address.Locality} Expected Street: {address.StreetData.Item1.ToUpper()}");
                    
                    foreach(var addressId in addressIds)
                    {
                        Console.WriteLine($"Found Locality: {addressId.StreetLoc.Locality.Name} Expected Street: {addressId.StreetLoc.Name} {addressId.StreetLoc.Type}");
                    }
                    Console.WriteLine();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Error: {e.Message}");
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

        public List<AddressLocality> AddressIds { get; set; }

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
        public byte ClassCodeOrder { get; set; }
        public List<string> Postcodes { get; set; }

        public Locality()
        {
            Postcodes = new List<string>();
        }
    }

    internal class StreetLocality
    {
        public string Pid { get; set; }
        public string Name { get; set; }
        public string Type { get; set; }
        public string Suffix { get; set; }
        public Locality Locality { get; set; }
    }

    internal class StreetLocalityDistance
    {
        public int Distance { get; set; }
        public List<StreetLocality> StreetLocality { get; set; }

        public StreetLocalityDistance()
        {
            StreetLocality = new List<StreetLocality>();
        }
    }

    internal class State
    {
        public int Pid { get; set; }
        public string Abbreviation { get; set; }
        public string Name { get; set; }
    }

    internal class StreeType
    {
        public string Name { get; set; }
        public string Code { get; set; }
    }

    internal class AddressLocality
    {
        public StreetLocality StreetLoc;
        public string addressId;
        public string CombinedStreet;
    }
}
