import loaddata as ld
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import ipdb as ipdb
import datetime as dt

# Cold season = Nov - Feb
# Spring = March - June
# Summer = July - October

SM_datapath = ('/home/greg/sftp/eddyflux/Tower sites/Tower Sites/PJ/PJ/Data/PJ_CR23X_Data/PJC_SWC_FILTERED OUTPUTS/')

SM_filelist = os.listdir(SM_datapath)

startYear = 2009
endYear = 2013

SM_idx = pd.date_range(str(startYear) + '-01-01',
        str(endYear + 1) + '-01-01', freq = '1D')
SM_df = pd.DataFrame(index = SM_idx)

data = pd.DataFrame()
for i in range(startYear, endYear + 1):
    fName = 'SWC_PJC_{0}_REFINED_Daily.CSV'.format(i)
    fData = ld.load_PJ_VWC_file(SM_datapath + str(i) + '/' + fName)
    #ipdb.set_trace()
    fData.index = fData.year_month_mday
    data = data.append(fData)

SM_df = data.reindex(SM_idx)

# Add water year and season columns
SM_df = ld.add_WY_cols(SM_df)

SM_df.to_csv('../processed_data/' + 'PJ_VWC_daily.csv', na_rep='NaN')

