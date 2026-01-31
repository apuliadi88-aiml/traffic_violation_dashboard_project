import pandas as pd
from sqlalchemy.types import Integer, Float, Boolean, Date, DateTime, String

# created a map to replace few invalid makes to valid ones
make_map = {
    'AACURA':'ACURA',
    'ACRUAA': 'ACURA',
    'ACUR':'ACURA',
    'HONDRA': 'HONDA',
    'CHEV': 'CHEVROLET',
    'CHEVY': 'CHEVROLET',
    'CHEVEROLT': 'CHEVROLET',
    'DODG':'DODGE',
    'TOY':'TOYOTA',
    'TOYT': 'TOYOTA',
    'TOYOTA': 'TOYOTA',
    'TOTOTA':'TOYOTA',
    'PRUIS': 'TOYOTA',  
    'HOND': 'HONDA',
    'HONDA': 'HONDA',
    'HUNDAI':'HYUNDAI',
    'HYUDAI':'HYUNDAI',
    'HYUN': 'HYUNDAI',
    'HYUNDIA':'HYUNDAI',
    'HYUNDAI': 'HYUNDAI',
    'MAZD': 'MAZDA',
    'MERCEDEZ':'MERCEDES',
    'MERCEDESBENZ':'MERCEDES',
    'MERZ': 'MERCEDES',
    'MERCEDES': 'MERCEDES',
    'SUBA': 'SUBARU',
    'INFI': 'INFINITI',
    'INFINITI': 'INFINITI',
    'EX':'LEXUS',
    'LEX':'LEXUS',
    'EXUS':'LEXUS',
    'LEXS': 'LEXUS',
    'LEXUS': 'LEXUS',
    'LINC': 'LINCOLN',
    'VOLV': 'VOLVO',
    'VOLVO': 'VOLVO',
    'LNDR': 'LAND ROVER',
    'NISS':'NISSAN',
    'MITS': 'MITSUBISHI',
    'CHRYS':'CHRYSLER',
    'CHRISTLER':'CHRYSLER',
    'MERCES':'MERCEDES',
    'BUW': 'BMW',
    'INTN':'INFINITI',
    'LGCH':'LEXUS',
    'MITSUBIHSHI': 'MITSUBISHI',
    'VOVOL': 'VOLVO',
    'FLEETWOOD': 'CADILLAC', 
    'KWORTH': 'KENWORTH',
    'RELEXUS':'LEXUS',
    'NISSIAN':'NISSAN',
    'NISAN':'NISSAN',
    'NSISAN':'NISSAN',
    'MERCEDIS': 'MERCEDES',
    'VOLKWAGON':'VOLKSWAGEN',
    'VOLK':'VOLKSWAGEN',
    'VOKLKSWAGON': 'VOLKSWAGEN',
    'INFINTI':'INFINITI',
    'PORCHE':'PORSCHE',
    'MASE': 'MASERATI',
    'PETE': 'PETERBILT',
    'MNNI': 'MINI',
    'SUZI': 'SUZUKI',
    'SUZU':'SUZUKI',
    'SCIO': 'SCION',
    'FRHT': 'FREIGHTLINER',
    'ISU': 'ISUZU',
    'ISUZ': 'ISUZU',
    'INTL': 'INTERNATIONAL',
    'JAGU':'JAGUAR',
    'TESL':'TESLA',
    'CHRY':'CHRYSLER',
    'NONE':'OTHER',
    'VOLKSWAGON': 'VOLKSWAGEN',
    'VW': 'VOLKSWAGEN',
    'LEXU': 'LEXUS',
    'MERC': 'MERCEDES',
    'CADI': 'CADILLAC',
    'BUIC': 'BUICK',
    'TOYO': 'TOYOTA',
    'PONT': 'PONTIAC',
    'INFINITY': 'INFINITI',
    'KENW': 'KENWORTH',
    'TOYTOTA': 'TOYOTA',
    'IZUZU': 'ISUZU',
    'HUYN': 'HYUNDAI',
    'SAUTN': 'SATURN',
    'HYUNDAY': 'HYUNDAI',
    'SMRT': 'SMART',
    'MER': 'MERCEDES',
    'LAND': 'LANDROVER',
    'HODDA': 'HONDA',
    'ACURAMDX': 'ACURA',
    'MAAZDA': 'MAZDA',
    'LAMBROGHINI': 'LAMBORGHINI',
    'HARLEY': 'HARLEY-DAVIDSON',
    'ACCURA': 'ACURA',
    'VOLKSBLUE': 'VOLKSWAGEN',
    'KIAV': 'KIA',
    'VALTSWAGON': 'VOLKSWAGEN',
    'CHRYTK': 'CHRYSLER',
    'VOLKWAGEN': 'VOLKSWAGEN',
    'VOLSWAGON': 'VOLKSWAGEN',
    'HNDA': 'HONDA',
    'INTE': 'INFINITI',
    'HYANDAI': 'HYUNDAI',
    'HYN': 'HYUNDAI',
    'TOYOVAL': 'TOYOTA',
    'CRYSLER': 'CHRYSLER',
    'BENZ': 'MERCEDES',
    'MERZBENZ': 'MERCEDES',
    'RANG': 'LANDROVER',
    'RAGEROVER': 'LANDROVER',
    'DOEGE': 'DODGE',
    'CVEV': 'CHEVROLET',
    'HUMM': 'HUMMER',
    'MAZ': 'MAZDA',
    'HYUNDA': 'HYUNDAI',
    'KW': 'KENWORTH',
    'HODD': 'HONDA',
    'SAA': 'SAAB',
    'TOTY': 'TOYOTA',
    'PRRSCHE': 'PORSCHE',
    'THOMASBUUS': 'THOMAS',
    'TAOTAO':'TOYOTA',
    'HINDA':'HONDA',
    'MAZADA':'MAZDA',
    'YAMA':'YAMAHA',
    'NISSANSUV':'NISSAN',
    'HYUNDI':'HYUNDAI',
    'MITZ':'MITSUBISHI',
    'MITSU':'MITSUBISHI',
    'SUBU':'SUBARU',
    'CHEVEROLET':'CHEVROLET',
    'SUBURU':'SUBARU',
    'INFIN':'INFINITI',
    'CHRYSTLER':'CHRYSLER',
    'PTRB':'PETERBILT',
    'CADILAC':'CADILLAC',
    'SMRT': 'SMART',
    'PLYM': 'PLYMOUTH',
    'SUB':'SUBARU',
    'PORS':'PORSCHE',
    'TOYTA':'TOYOTA',
    'VOLKS':'VOLKSWAGEN',
    'TOYOT':'TOYOTA',
    'HYUND':'HYUNDAI',
    'ZUZUKI':'SUZUKI'
}

color_map = {
    'BLUE, DARK':'DARK BLUE',
    'BLUE, LIGHT':'LIGHT BLUE',
    'GREEN, DK':'DARK GREEN',
    'GREEN, LGT':'LIGHT GREEN'
}

dtype_map = {
    # Datetime columns
    "Date Of Stop": "datetime64[ns]",
    "Timestamp": "datetime64[ns]",
    # String columns
    "SeqID": "string",
    "Agency": "string",
    "SubAgency": "string",
    "Description": "string",
    "Location": "string",
    "Time Of Stop": "string",
    "Search Disposition": "string",
    "Search Outcome": "string",
    "Search Reason": "string",
    "Search Reason For Stop": "string",
    "Search Type": "string",
    "Search Arrest Reason": "string",
    "State": "string",
    "VehicleType": "string",
    "Vehicle Code": "string",
    "Vehicle Category": "string",
    "Make": "string",
    "Model": "string",
    "Color": "string",
    "Violation Type": "string",
    "Charge": "string",
    "Article": "string",
    "Race": "string",
    "Gender": "string",
    "Driver City": "string",
    "Driver State": "string",
    "DL State": "string",
    "Arrest Type": "string",
    "Geolocation": "string",
        
    # Float Columns
    "Latitude": "float64",
    "Longitude": "float64",

    # Integer Column
    "Year": "Int64"
}


# State(US States, Union Territories, Canadian Provinces, Canadian Territories)
us_states_codes = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN",
    "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV",
    "NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN",
    "TX","UT","VT","VA","WA","WV","WI","WY"
]
us_territories_codes = [
    "AS","GU","MP","PR","VI"
]
canadian_provinces_codes = [
    "AB","BC","MB","NB","NL","NS","ON","PE","QC","SK"
]
canadian_territories_codes = [
    "NT","NU","YT"
]
valid_codes = (
    us_states_codes + us_territories_codes +
    canadian_provinces_codes + canadian_territories_codes
)
# State Columns
state_columns = ['State', 'Driver State', 'DL State']

# Search Columns
search_columns = [
    'Search Disposition', 'Search Outcome', 'Search Reason',
    'Search Reason For Stop', 'Search Type' ,'Search Arrest Reason']

# Boolean Columns
boolean_columns = [
    'Accident', 'Belts', 'Personal Injury', 'Property Damage',
    'Fatal', 'Commercial License', 'HAZMAT', 'Commercial Vehicle',
    'Alcohol','Work Zone', 'Search Conducted'
]

# Other Columns
other_columns = [
    'Violation Type', 'Arrest Type', 'Article',
    'Race', 'Gender', 'Contributed To Accident'
]