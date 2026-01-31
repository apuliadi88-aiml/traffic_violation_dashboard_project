import pandas as pd
import numpy as np

# ------------------ Utility ------------------
def to_upper_strip(column):
    return (
        column.astype(str)
        .str.strip()
        .str.upper()
        .replace({'NAN': np.nan, 'NONE': np.nan, '': np.nan})
    )

# ----------------- SeqID, Description,Charge----------------------#

def merge_duplicates(df, id_col='SeqID', description_col='Description', charge_col='Charge',
                     desc_sep=' | ', charge_sep=','):

    df.drop_duplicates(inplace=True)
    # Step 1: Converting description & charge columns to string and cleaning 
    df[description_col] = to_upper_strip(df[description_col])
    df[description_col] = df[description_col].str.lstrip(',| )]\\')
    df[charge_col] = df[charge_col].astype(str)

    # Step 2: Merge duplicates based on ID
    merged = (
        df.groupby(id_col, sort=False, as_index=False)
        .agg({
          description_col: lambda x: desc_sep.join(str(i) for i in pd.unique(x) if pd.notna(i)),
          charge_col: lambda x: charge_sep.join(str(i) for i in pd.unique(x) if pd.notna(i))
        })
)


    # Step 3: Keep first occurrence of other columns
    df_unique = df.drop_duplicates(subset=id_col, keep='first')
    df_unique = df_unique.drop(columns=[description_col, charge_col])

    # Step 4: Merge cleaned D ?escriptions and Charges back
    df_clean = df_unique.merge(merged[[id_col, description_col, charge_col]],
                               on=id_col, how='left')

    return df_clean


# ---------------------- Date of Stop ------------------#
def clean_date_of_stop(df, column='Date Of Stop', min_year=1990):
    today = pd.Timestamp.today().normalize()

    df[column] = pd.to_datetime(
        df[column],
        errors='coerce',
        format='mixed'
    )

    mask = (
        df[column].notna() &
        (df[column] <= today) &
        (df[column].dt.year.between(min_year, today.year))
    )

    df.loc[~mask, column] = pd.NaT
    return df

# ---------------------- Time Of Stop, Timestamp--------------------#

def clean_time_of_stop(df, time_col='Time Of Stop', date_col='Date Of Stop', timestamp_col='Timestamp'):

    
    # Step 1: Remove inconsistencies in the time column
    df[time_col] = df[time_col].astype(str).str.strip().str.replace('.', ':', regex=False)
    
    # Step 2: Convert to proper time format
    df[time_col] = pd.to_datetime(df[time_col], errors='coerce').dt.time
    
    # Step 3: Create combined timestamp column
    df[timestamp_col] = pd.to_datetime(
        df[date_col].astype(str) + ' ' + df[time_col].astype(str),
        errors='coerce'
    )
    
    return df

# ------------------ Agency, SubAgency--------------------------------#

def clean_agency_subagency(df, agency_col='Agency', subagency_col='SubAgency'):
    df[agency_col] = to_upper_strip(df[agency_col])
    df[subagency_col] = to_upper_strip(df[subagency_col])

    return df

# ------------------- Location ---------------------------------------#

def clean_location(df, location_col='Location'):
    df[location_col] = to_upper_strip(df[location_col])
    df[location_col] = df[location_col].str.replace('@','/')

    return df


# ------------------- Latitude, Longitude, Geolocation-----------------#

def clean_lat_long_geo(df, lat_col='Latitude', long_col='Longitude', geo_col='Geolocation'):
    df[lat_col] = pd.to_numeric(df[lat_col], errors='coerce').replace(0.0,np.nan).round(6)
    df[lat_col] = df[lat_col].where((df[lat_col] >= 18.9) & (df[lat_col] <= 71.5), np.nan)

    df[long_col] = pd.to_numeric(df[long_col], errors='coerce').replace(0.0, np.nan).round(6)
    df[long_col] = df[long_col].where((df[long_col] >= -179.9) & (df[long_col] <= -66.9),np.nan)

    mask = df[lat_col].notna() & df[long_col].notna() 

    df[geo_col] = None
    df.loc[mask, geo_col] = pd.Series(list(zip(df.loc[mask, lat_col], df.loc[mask, long_col])),index=df.loc[mask].index)


    return df

# ------------------ Boolean Columns ------------------#
TRUE_SET = {'YES', 'Y', 'TRUE', 'T', '1', 1}
FALSE_SET = {'NO', 'N', 'FALSE', 'F', '0', 0, '', np.nan}

def clean_boolean_columns(df, columns):
    for col in columns:
        s = to_upper_strip(df[col])
        df[col] = pd.NA  # default to missing
        df.loc[s.isin(TRUE_SET), col] = True
        df.loc[s.isin(FALSE_SET), col] = False
    return df    

# -------------------Search Disposition, Search Outcome------------#
def clean_search_columns(df,search_columns):
    for col in search_columns:
        df[col] = to_upper_strip(df[col])
        df[col] = df[col].replace(['NAN','NOTHING'], 'NA')
        if col == 'Search Reason For Stop':
            df[col] = df[col].str.rstrip("-")
        if col == 'Search Arrest Reason':
            df[col] = df[col].replace({'MARIHUANA':'MARIJUANA','DRIVING':'TRAFFIC'})
    return df

# --------------------State---------------------------------#
def clean_state(df, valid_codes, state_columns):
    for col in state_columns:
        df[col] = to_upper_strip(df[col])
        df[col] = np.where(df[col].isin(valid_codes),df[col],np.nan)

    return df

#-----------------Vehicle Type, Vehicle Code, Vehicle Category--------#

def clean_vehicle_columns(df, type_col='VehicleType', code_col='Vehicle Code', category_col='Vehicle Category'):
    s = to_upper_strip(df[type_col])
    df[[code_col, category_col]] = s.str.split(' - ', expand=True)
    return df

# ------------------ Year ------------------
def clean_year(df, column='Year', min_val=None, max_val=None):
    df[column] = pd.to_numeric(df[column], errors='coerce')

    if min_val is not None:
        df.loc[df[column] < min_val, column] = np.nan
    if max_val is not None:
        df.loc[df[column] > max_val, column] = np.nan

    return df


# ------------------ Make ------------------
def clean_make(df, make_map, column='Make'):
    s = to_upper_strip(df[column])
    s = s.str.replace(r'[^A-Z]', '', regex=True)
    s = s.replace(make_map)

    make_counts = s.value_counts()
    popular_makes = make_counts[make_counts > 200].index

    df[column] = np.where(s.isin(popular_makes), s, 'OTHER')
    return df


# ------------------ Model----------------------#
def clean_model(df, column='Model'):
    s = to_upper_strip(df[column])
    s = s.str.replace(r'[^A-Z0-9 ]', '', regex=True)
    s = s.str.lstrip('0')
    s = s.replace({'': np.nan, 'NONE': np.nan})
    df[column] = s.replace({'': np.nan, 'NONE': np.nan})

    model_counts = s.value_counts()
    popular_models = model_counts[model_counts > 50].index
    df[column] = np.where(s.isin(popular_models), s, np.nan)
    return df

# ------------------ Color ------------------------#
def clean_color(df, color_map, column ='Color'):
    df[column]= df[column].replace(color_map)
    return df

# -----------------Driver City-----------------------#
def clean_driver_city(df, city_col= 'Driver City', min_count = 50):
    s = to_upper_strip(df[city_col])
    s = s.str.replace(r'[^A-Z]','',regex=True)
    s = s.str.replace(r'^[X]+','',regex=True).replace('',np.nan)
    city_counts = s.value_counts()
    popular_cities = city_counts[city_counts > min_count].index
    df[city_col]= np.where(s.isin(popular_cities), s, np.nan)
    
    return df
    

# ------------------ Violation Type, Arrest Type, Article---------

def clean_other_columns(df, other_columns):
    for col in other_columns:
        df[col] = to_upper_strip(df[col])
        if col == 'Article':
            df[col] = df[col].replace('00',np.nan)
        if col == 'Gender':
            df[col] = df[col].replace('U','UNKNOWN')        
    return df



