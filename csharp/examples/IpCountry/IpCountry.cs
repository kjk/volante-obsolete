#if WITH_PATRICIA
using System;
using Volante;
using System.IO;

/**
 * Get country for IP address using PATRICIA Trie.
 */
public class IpCountry 
{ 
    const int PagePoolSize = 32*1024*1024;

    class Root : Persistent { 
        internal IIndex<string,Country> countries;
        internal IPatriciaTrie<Country> trie;
    }

    class Country : Persistent { 
        internal string name;

        internal Country(string name) { 
            this.name = name;
        }
        Country() {}
    }

    public static void Main(string[] args) 
    { 
        IDatabase db = DatabaseFactory.CreateDatabase();
        db.Open("ipcountry.dbs", PagePoolSize);
        Root root = (Root)db.Root;
        if (root == null) { 
            root = new Root();
            root.countries = db.CreateIndex<string,Country>(IndexType.Unique);
            root.trie = db.CreatePatriciaTrie<Country>();
            loadCountries(root.countries);
            db.Root = root;
        }
        for (int i = 0; i < args.Length; i++) {
            loadIpCountryTable(root, args[i]);
        }

        string ip;
        while ((ip = Console.ReadLine()) != null) { 
            Country country = root.trie.FindBestMatch(PatriciaTrieKey.FromIpAddress(ip));
            if (country != null) { 
                Console.WriteLine(ip + "->" + country.name);
            }
        }
        db.Close();
    }

    static void loadIpCountryTable(Root root, string fileName) {

        FileStream fs = new FileStream(fileName, FileMode.Open);
        StreamReader sr = new StreamReader(fs);
        string line;
        while ((line = sr.ReadLine()) != null) { 
            int sep1 = line.IndexOf('|');
            if (sep1 >= 0) {
                int sep2 = line.IndexOf('|', sep1+1);
                int sep3 = line.IndexOf('|', sep2+1);
                int sep4 = line.IndexOf('|', sep3+1);
                if (sep2 > sep1 && sep4 > sep3) { 
                    String iso = line.Substring(sep1+1, sep2-sep1-1).ToUpper();
                    String ip = line.Substring(sep3+1, sep4-sep3-1);
                    if (ip.IndexOf('.') > 0 && iso.Length == 2) { 
                        Country c = (Country)root.countries[iso];
                        if (c == null) { 
                            Console.WriteLine("Unknown country code: " + iso);
                        } else { 
                            root.trie.Add(PatriciaTrieKey.FromIpAddress(ip), c);
                        }
                    }
                }
            }
        }
    }

    static void addCountry(IIndex<string,Country> countries, string country, string iso) { 
        countries[iso] = new Country(country);
    }

    static void loadCountries(IIndex<string,Country> countries) { 
        addCountry(countries, "Burundi", "BI");
        addCountry(countries, "Central African Republic", "CF");
        addCountry(countries, "Chad", "TD");
        addCountry(countries, "Congo", "CG");
        addCountry(countries, "Rwanda", "RW");
        addCountry(countries, "Zaire (Congo)", "ZR");

        addCountry(countries, "Djibouti", "DJ");
        addCountry(countries, "Eritrea", "ER");
        addCountry(countries, "Ethiopia", "ET");
        addCountry(countries, "Kenya", "KE");
        addCountry(countries, "Somalia", "SO");
        addCountry(countries, "Tanzania", "TZ");
        addCountry(countries, "Uganda", "UG");

        addCountry(countries, "Comoros", "KM");
        addCountry(countries, "Madagascar", "MG");
        addCountry(countries, "Mauritius", "MU");
        addCountry(countries, "Mayotte", "YT");
        addCountry(countries, "Reunion", "RE");
        addCountry(countries, "Seychelles", "SC");

        addCountry(countries, "Algeria", "DZ");
        addCountry(countries, "Egypt", "EG");
        addCountry(countries, "Libya", "LY");
        addCountry(countries, "Morocco", "MA");
        addCountry(countries, "Sudan", "SD");
        addCountry(countries, "Tunisia", "TN");
        addCountry(countries, "Western Sahara", "EH");

        addCountry(countries, "Angola", "AO");
        addCountry(countries, "Botswana", "BW");
        addCountry(countries, "Lesotho", "LS");
        addCountry(countries, "Malawi", "MW");
        addCountry(countries, "Mozambique", "MZ");
        addCountry(countries, "Namibia", "NA");
        addCountry(countries, "South Africa", "ZA");
        addCountry(countries, "Swaziland", "SZ");
        addCountry(countries, "Zambia", "ZM");
        addCountry(countries, "Zimbabwe", "ZW");

        addCountry(countries, "Benin", "BJ");
        addCountry(countries, "Burkina Faso", "BF");
        addCountry(countries, "Cameroon", "CM");
        addCountry(countries, "Cape Verde", "CV");
        addCountry(countries, "Cote d'Ivoire", "CI");
        addCountry(countries, "Equatorial Guinea", "GQ");
        addCountry(countries, "Gabon", "GA");
        addCountry(countries, "Gambia, The", "GM");
        addCountry(countries, "Ghana", "GH");
        addCountry(countries, "Guinea", "GN");
        addCountry(countries, "Guinea-Bissau", "GW");
        addCountry(countries, "Liberia", "LR");
        addCountry(countries, "Mali", "ML");
        addCountry(countries, "Mauritania", "MR");
        addCountry(countries, "Niger", "NE");
        addCountry(countries, "Nigeria", "NG");
        addCountry(countries, "Sao Tome and Principe", "ST");
        addCountry(countries, "Senegal", "SN");
        addCountry(countries, "Sierra Leone", "SL");
        addCountry(countries, "Togo", "TG");

        addCountry(countries, "Belize", "BZ");
        addCountry(countries, "Costa Rica", "CR");
        addCountry(countries, "El Salvador", "SV");
        addCountry(countries, "Guatemala", "GT");
        addCountry(countries, "Honduras", "HN");
        addCountry(countries, "Mexico", "MX");
        addCountry(countries, "Nicaragua", "NI");
        addCountry(countries, "Panama", "PA");
        addCountry(countries, "Canada", "CA");
        addCountry(countries, "Greenland", "GL");
        addCountry(countries, "Saint-Pierre et Miquelon", "PM");
        addCountry(countries, "United States", "US");
        addCountry(countries, "Argentina", "AR");
        addCountry(countries, "Bolivia", "BO");
        addCountry(countries, "Brazil", "BR");
        addCountry(countries, "Chile", "CL");
        addCountry(countries, "Colombia", "CO");
        addCountry(countries, "Ecuador", "EC");
        addCountry(countries, "Falkland Islands", "FK");
        addCountry(countries, "French Guiana", "GF");
        addCountry(countries, "Guyana", "GY");
        addCountry(countries, "Paraguay", "PY");
        addCountry(countries, "Peru", "PE");
        addCountry(countries, "Suriname", "SR");
        addCountry(countries, "Uruguay", "UY");
        addCountry(countries, "Venezuela", "VE");

        addCountry(countries, "Anguilla", "AI");
        addCountry(countries, "Antigua&Barbuda", "AG");
        addCountry(countries, "Aruba", "AW");
        addCountry(countries, "Bahamas, The", "BS");
        addCountry(countries, "Barbados", "BB");
        addCountry(countries, "Bermuda", "BM");
        addCountry(countries, "British Virgin Islands", "VG");
        addCountry(countries, "Cayman Islands", "KY");
        addCountry(countries, "Cuba", "CU");
        addCountry(countries, "Dominica", "DM");
        addCountry(countries, "Dominican Republic", "DO");
        addCountry(countries, "Grenada", "GD");
        addCountry(countries, "Guadeloupe", "GP");
        addCountry(countries, "Haiti", "HT");
        addCountry(countries, "Jamaica", "JM");
        addCountry(countries, "Martinique", "MQ");
        addCountry(countries, "Montserrat", "MS");
        addCountry(countries, "Netherlands Antilles", "AN");
        addCountry(countries, "Puerto Rico", "PR");
        addCountry(countries, "Saint Kitts and Nevis", "KN");
        addCountry(countries, "Saint Lucia", "LC");
        addCountry(countries, "Saint Vincent and the Grenadines", "VC");
        addCountry(countries, "Trinidad and Tobago", "TT");
        addCountry(countries, "Turks and Caicos Islands", "TC");
        addCountry(countries, "Virgin Islands", "VI");

        addCountry(countries, "Kazakhstan", "KZ");
        addCountry(countries, "Kyrgyzstan", "KG");
        addCountry(countries, "Tajikistan", "TJ");
        addCountry(countries, "Turkmenistan", "TM");
        addCountry(countries, "Uzbekistan", "UZ");
        addCountry(countries, "China", "CN");
        addCountry(countries, "Hong Kong", "HK");
        addCountry(countries, "Japan", "JP");
        addCountry(countries, "Korea, North", "KP");
        addCountry(countries, "Korea, South", "KR");
        addCountry(countries, "Taiwan", "TW");
        addCountry(countries, "Mongolia", "MN");
        addCountry(countries, "Russia", "RU");
        addCountry(countries, "Afghanistan", "AF");
        addCountry(countries, "Bangladesh", "BD");
        addCountry(countries, "Bhutan", "BT");
        addCountry(countries, "India", "IN");
        addCountry(countries, "Maldives", "MV");
        addCountry(countries, "Nepal", "NP");
        addCountry(countries, "Pakistan", "PK");
        addCountry(countries, "Sri Lanka", "LK");
        addCountry(countries, "Brunei", "BN");
        addCountry(countries, "Cambodia", "KH");
        addCountry(countries, "Christmas Island", "CX");
        addCountry(countries, "Cocos (Keeling) Islands", "CC");
        addCountry(countries, "Indonesia", "ID");
        addCountry(countries, "Laos", "LA");
        addCountry(countries, "Malaysia", "MY");
        addCountry(countries, "Myanmar (Burma)", "MM");
        addCountry(countries, "Philippines", "PH");
        addCountry(countries, "Singapore", "SG");
        addCountry(countries, "Thailand", "TH");
        addCountry(countries, "Vietnam", "VN");
        addCountry(countries, "Armenia", "AM");
        addCountry(countries, "Azerbaijan", "AZ");
        addCountry(countries, "Bahrain", "BH");
        addCountry(countries, "Cyprus", "CY");
        addCountry(countries, "Georgia", "GE");
        addCountry(countries, "Iran", "IR");
        addCountry(countries, "Iraq", "IQ");
        addCountry(countries, "Israel", "IL");
        addCountry(countries, "Jordan", "JO");
        addCountry(countries, "Kuwait", "KW");
        addCountry(countries, "Lebanon", "LB");
        addCountry(countries, "Oman", "OM");
        addCountry(countries, "Qatar", "QA");
        addCountry(countries, "Saudi Arabia", "SA");
        addCountry(countries, "Syria", "SY");
        addCountry(countries, "Turkey", "TR");
        addCountry(countries, "United Arab Emirates", "AE");
        addCountry(countries, "Yemen", "YE");


        addCountry(countries, "Austria", "AT");
        addCountry(countries, "Czech Republic", "CZ");
        addCountry(countries, "Hungary", "HU");
        addCountry(countries, "Liechtenstein", "LI");
        addCountry(countries, "Slovakia", "SK");
        addCountry(countries, "Switzerland", "CH");
        addCountry(countries, "Belarus", "BY");
        addCountry(countries, "Estonia", "EE");
        addCountry(countries, "Latvia", "LV");
        addCountry(countries, "Lithuania", "LT");
        addCountry(countries, "Moldova", "MD");
        addCountry(countries, "Poland", "PL");
        addCountry(countries, "Ukraine", "UA");
        addCountry(countries, "Denmark", "DK");
        addCountry(countries, "Faroe Islands", "FO");
        addCountry(countries, "Finland", "FI");
        addCountry(countries, "Iceland", "IS");
        addCountry(countries, "Norway", "NO");
        addCountry(countries, "Svalbard", "SJ");
        addCountry(countries, "Sweden", "SE");
        addCountry(countries, "Albania", "AL");
        addCountry(countries, "Bosnia Herzegovina", "BA");
        addCountry(countries, "Bulgaria", "BG");
        addCountry(countries, "Croatia", "HR");
        addCountry(countries, "Greece", "GR");
        addCountry(countries, "Macedonia", "MK");
        addCountry(countries, "Romania", "RO");
        addCountry(countries, "Slovenia", "SI");
        addCountry(countries, "Yugoslavia", "YU");
        addCountry(countries, "Andorra", "AD");
        addCountry(countries, "Gibraltar", "GI");
        addCountry(countries, "Portugal", "PT");
        addCountry(countries, "Spain", "ES");
        addCountry(countries, "Vatican", "VA");
        addCountry(countries, "Italy", "IT");
        addCountry(countries, "Malta", "MT");
        addCountry(countries, "San Marino", "SM");
        addCountry(countries, "Belgium", "BE");
        addCountry(countries, "France", "FR");
        addCountry(countries, "Germany", "DE");
        addCountry(countries, "Ireland", "IE");
        addCountry(countries, "Luxembourg", "LU");
        addCountry(countries, "Monaco", "MC");
        addCountry(countries, "Netherlands", "NL");
        addCountry(countries, "United Kingdom", "GB");
        addCountry(countries, "United Kingdom", "UK");

        addCountry(countries, "American Samoa", "AS");
        addCountry(countries, "Australia", "AU");
        addCountry(countries, "Cook Islands", "CK");
        addCountry(countries, "Fiji", "FJ");
        addCountry(countries, "French Polynesia", "PF");
        addCountry(countries, "Guam", "GU");
        addCountry(countries, "Kiribati", "KI");
        addCountry(countries, "Marshall Islands", "MH");
        addCountry(countries, "Micronesia", "FM");
        addCountry(countries, "Nauru", "NR");
        addCountry(countries, "New Caledonia", "NC");
        addCountry(countries, "New Zealand", "NZ");
        addCountry(countries, "Niue", "NU");
        addCountry(countries, "Norfolk Island", "NF");
        addCountry(countries, "Northern Mariana Islands", "MP");
        addCountry(countries, "Palau", "PW");
        addCountry(countries, "Papua New-Guinea", "PG");
        addCountry(countries, "Pitcairn Islands", "PN");
        addCountry(countries, "Solomon Islands", "SB");
        addCountry(countries, "Tokelau", "TK");
        addCountry(countries, "Tonga", "TO");
        addCountry(countries, "Tuvalu", "TV");
        addCountry(countries, "Vanuatu", "VU");
        addCountry(countries, "Wallis & Futuna", "WF");
        addCountry(countries, "Western Samoa", "WS");
    }
}
#else
using System;
public class IpCountry
{
    public static void Main(string[] args)
    {
        Console.WriteLine("IpCountry not available if not compiled with WITH_PATRICIA");
    }
}
#endif
