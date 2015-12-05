'''
    Script to create and save a bunch of datasets for use in R. Combines
    several older scripts (MultiYearFluxes, MultiYearPrecip, etc) from the 
    NMEG_seasonality_poster analysis in NMEG_miscellany.
'''
import sys

# laptop
sys.path.append( '/home/greg/current/NMEG_utils/py_modules/' )
af_path = '/home/greg/sftp/eddyflux/Ameriflux_files/provisional/'
outpath = 'processed_data/'

import load_nmeg as ld
import transform_nmeg as tr
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pdb as pdb
import datetime as dt

# Years to load
startyr = 2007
endyr = 2014
# Sites to load
sites = ['Seg', 'Ses', 'Wjs', 'Mpj', 'Mpg', 'Vcp', 'Vcm']

# Fill a dict with multiyear dataframes for each site in sites
hourly = { x : 
        ld.get_multiyr_aflx( 'US-' + x, af_path, gapfilled=True,
            startyear=startyr, endyear=endyr) 
        for x in sites }
# Resample to daily sums with integration
daily = { x : 
        tr.resample_30min_aflx( hourly[x], freq='1D', 
            c_fluxes=[ 'GPP', 'RECO', 'FC_F' ],
            le_flux=[ 'LE_F' ], 
            avg_cols=[ 'TA_F', 'RH_F', 'SW_IN_F', 'RNET', 'VPD_F' ],
            precip_col='P_F' , tair_col='TA_F' ) 
        for x in hourly.keys() }

# Get dataframe with gauge precip from all sites
daily_P = tr.get_var_allsites( daily, 'P_F', sites, startyear=startyr,
        endyear=endyr)

# Append '_gauge' to each column
daily_P.columns = [x + '_gauge' for x in daily_P.columns]

# Then get PRISM data
PRISM_datapath = ('/home/greg/sftp/eddyflux/Ancillary_met_data/PRISM_daily/')
PRISM_filelist = os.listdir(PRISM_datapath)

# Fill a new data frame by appending each file
data = pd.DataFrame()
for i in range(startyr, endyr + 1):
    fName = 'PRISM_DailyPrecip_{0}.csv'.format(i)
    fData = ld.loadPRISMfile(PRISM_datapath + fName)
    data = data.append(fData)

# Append '_PRISM' to each column
data.columns = [x[3:6] + '_PRISM' for x in data.columns]

# Make gauge and PRISM indices match then concatenate
PRISM_df = data.reindex( daily_P.index )
combined = pd.concat( [daily_P, PRISM_df], axis=1)

# Add wateryear col
combined = ld.add_WY_cols( combined )

# Export daily and then yearly tables
combined.to_csv( outpath + 'daily_precip.csv', na_rep='NaN' )

grouped = combined.groupby( combined.index.year )
precipSum = grouped.aggregate(np.sum) 
grouped = combined.groupby('year_w')
wyrPrecipSum = grouped.aggregate(np.sum)

precipSum.to_csv( outpath + 'precipSums.csv', na_rep='Nan')
wyrPrecipSum.to_csv( outpath + 'precipSums_wyear.csv', na_rep='Nan') 


# Now create daily fluxes for export

# Load measurement specific dataframes that will contain a
# multi-year column for each site
FC_df = tr.get_var_allsites( daily, 'FC_F_g_int', sites, startyear=startyr,
        endyear=endyr)
GPP_df = tr.get_var_allsites( daily, 'GPP_g_int', sites, startyear=startyr,
        endyear=endyr)
RE_df = tr.get_var_allsites( daily, 'RECO_g_int', sites, startyear=startyr,
        endyear=endyr)
ET_df = tr.get_var_allsites( daily, 'ET_mm_int_0', sites, startyear=startyr,
        endyear=endyr)

# Add water year and season columns
FC_int_daily = ld.add_WY_cols(FC_df)
GPP_int_daily = ld.add_WY_cols(GPP_df)
RE_int_daily = ld.add_WY_cols(RE_df)
ET_int_daily = ld.add_WY_cols(ET_df)

# output to csv
FC_int_daily.to_csv(outpath + 'FC_int_daily.csv', na_rep='NaN')
GPP_int_daily.to_csv(outpath + 'GPP_int_daily.csv', na_rep='NaN')
RE_int_daily.to_csv(outpath + 'RE_int_daily.csv', na_rep='NaN')
ET_int_daily.to_csv(outpath + 'ET_int_daily.csv', na_rep='NaN')
