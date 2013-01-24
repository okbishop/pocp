'''Initial POCP version control script currently running with the following two crontab entries:

5 1,7,13,19 * * * /usr/bin/python /home/humed/python/POCP/pocp.py --pocp_pass="password" --dw_pass="password" >> /home/humed/python/POCP/pocp_cron.log 2>&1
10 1,7,13,19 * * * cd /home/humed/python/POCP && /usr/local/bin/git commit -a -m "Cron commit at $(date +\%Y_\%m_\%d_\%H)" >> /home/hume$

The first part runs this python script every 6 hours at five minutes past the hour, for the hours, 1am,7am,1pm and 7pm.
This script is currently basic and needs to be rewritten in a proper class format with exception handling etc, (future work) 
The basic idea is: 1) grab all POCP data (confirmed/tentative/cancelled) for Transmission/Generation, and, 
                      Direct Connects looking back 6 months and forward 6 months from the current date
                   2) add this data to the all time pocp data history and remove duplicates
                   3) save this to a text file, in this case, pocp_all.csv
                   4) modify the generation POCP data to be more usefull historically - see code below
                   5) create time series data with POCP_to_timeseries function
                   6) plot a long and short version of the pocp data, save to time stamped png
                    
D J Hume, 19 December, 2012
'''

from pandas import *
import mechanize
from bs4 import BeautifulSoup
import os,sys
from datetime import date, datetime, time, timedelta
from io import StringIO
from ea_colours import ea_p,ea_s,part
import pandas
import pyodbc
import pandas.io.sql as sql
from dateutil.parser import parse
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import argparse

def parse_time(x,parse):
    if x != None:
        x = parse(x)
    else:
       x= float('nan')
    return x    

#############################################################################################################################################################################        
#Setup command line option and argument parsing
#############################################################################################################################################################################        
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('--pocp_host', action="store",dest='pocp_host',default='http://pocp.redspider.co.nz/')
parser.add_argument('--pocp_user', action="store",dest='pocp_user',default='david.hume')
parser.add_argument('--pocp_pass', action="store",dest='pocp_pass')
parser.add_argument('--dw_user', action="store",dest='dw_user',default='linux_user')
parser.add_argument('--dw_pass', action="store",dest='dw_pass')
cmd_line = parser.parse_args()
pocp_path = '/home/humed/python/POCP/'

#Get Max_node capacity offered from datawarehouse
con = pyodbc.connect('DSN=NZxDaily_LIVE;UID=' + cmd_line.dw_user + ';PWD=' + cmd_line.dw_pass)
x = 'select * from fact.Max_Node_Capacity_Offered'
max_offered = sql.read_frame(x, con,index_col='pnode_name')
max_offered['First_MaxEnergyCapacity_Date'] = max_offered.First_MaxEnergyCapacity_Date.map(lambda x: parse(x))
max_offered['Last_MaxEnergyCapacity_Date'] = max_offered.Last_MaxEnergyCapacity_Date.map(lambda x: parse(x))
max_offered['Last_NonZeroOffer_Date'] = max_offered.Last_NonZeroOffer_Date.map(lambda x: parse_time(x,parse))
max_offered = max_offered.rename(columns={'trader_id':'ID','Max_EnergyCapacity':'Max_offer','First_MaxEnergyCapacity_Date':'First_max_offer_date','Last_MaxEnergyCapacity_Date':'Last_max_offer_date','Last_NonZeroOffer_Date':'Last_nonzero_offer_date','Trader_CompanyCode':'Company_code','Trader_CompanyName':'Company_name'})
ID_map_name = max_offered.ix[:,['ID','Company_name']].set_index('ID').drop_duplicates()
ID_map = max_offered.ix[:,['ID','Company_code']].set_index('ID').drop_duplicates()
ID_map['Colour'] = ID_map.Company_code.map(lambda x: part[x])

#Get some useful mappings from Nicky's ConnectionNames.xls file, found at P:\HH\Source\Locations
names = ExcelFile('/home/dave/.gvfs/common on ecomfp01/HH/Source/Locations/ConnectionNames.xls') 
cron_maps = names.parse('Names')
maps = names.parse('Connection Points')
nsp = names.parse('NSP Mapping Table',skiprows=1)
island = maps.ix[:,['Island','TPNZ Grid Busname','Point of Connection','Unnamed: 7','Unnamed: 8']].set_index('TPNZ Grid Busname').rename(columns={'Unnamed: 7':'ID','Unnamed: 8':'Type'})
island = maps[maps['Point of Connection'] == maps['TPNZ Grid Busname']].set_index('TPNZ Grid Busname').rename(columns={'Unnamed: 7':'ID','Unnamed: 8':'Type'})
GT={'ABY': 'Hydro','ANC': 'Thermal','ARA': 'Hydro','ARG': 'Hydro','ARI': 'Hydro','ASB': 'Hydro','ATI': 'Hydro','AVI': 'Hydro','BEN': 'Hydro','BLN': 'Hydro',\
    'BPE': 'Wind','BRB': 'Thermal','BWK': 'Hydro','COB': 'Hydro','COL': 'Hydro','CST': 'Hydro','CYD': 'Hydro','DOB': 'Hydro','HAM': 'Thermal','HKK': 'Hydro',\
    'HLY': 'Thermal','HUI': 'Hydro','HWA': 'Hydro','HWB': 'Wind','KAG': 'Geothermal','KAW': 'Geothermal','KIN':'Thermal','KPO': 'Hydro','KTW': 'Hydro','KUM': 'Hydro',\
    'LTN': 'Wind','MAN': 'Hydro','MAT': 'Hydro','MDN': 'Thermal','MTI': 'Hydro','NAP': 'Geothermal','NPL': 'Thermal','NSY': 'Hydro','OHA': 'Hydro','OHB': 'Hydro',\
    'OHC': 'Hydro','OHK': 'Hydro','OKI': 'Geothermal','OTA': 'Thermal','PPI': 'Geothermal','PRI': 'Hydro','ROT': 'Thermal','ROX': 'Hydro','RPO': 'Hydro',\
    'SFD': 'Thermal','SWN': 'Thermal','TAA': 'Geothermal','TAP': 'Wind','TGA': 'Hydro','TKA': 'Hydro','TKB': 'Hydro','TKU': 'Hydro','TMU': 'Thermal','TUI': 'Hydro',\
    'TUK': 'Wind','TWC': 'Wind','WHI': 'Thermal','WHL': 'Wind','WKM': 'Hydro','WPA': 'Hydro','WRK': 'Geothermal','WTK': 'Hydro','WWD': 'Wind'}
island_map2 = island.reset_index().ix[:,['TPNZ Grid Busname','Island']]
island_map2['TPNZ Grid Busname'] = island_map2['TPNZ Grid Busname'].map(lambda x: x[:3])
island_map2=island_map2.drop_duplicates('TPNZ Grid Busname').set_index('TPNZ Grid Busname').Island.to_dict()
island_map2[u'ANC'] = u'North'
island_map2[u'KAG'] = u'North'
island_map2[u'KTW'] = u'North'
island_map2[u'NAP'] = u'North'
island_map2[u'PRI'] = u'North'
island_map2[u'TAA'] = u'North'
island_map2[u'TAP'] = u'North'
island_map2[u'TUK'] = u'North'
island_map2[u'WHL'] = u'South'
island_map2[u'WWD'] = u'North'
island_map = DataFrame({'Island':max_offered.island})
island_map['code1'] = island_map.index.map(lambda x: x.split(' ')[0][0:3])
island_map['code2'] = island_map.index.map(lambda x: x.split(' ')[1][0:3])
island_map = island_map.drop_duplicates(cols='code2').set_index('code2')

#Note: downloads from the POCP redspider server appear to be limited to 10000 rows max.  This is about 1 years worth of data, so downloads can not be any more than this.
bufferIO = StringIO()
strt = (datetime.now() - timedelta(0.5*365))
start_time = strt.isoformat().split('T')[0].split('-')[2] + '/' + strt.isoformat().split('T')[0].split('-')[1] + '/' + strt.isoformat().split('T')[0].split('-')[0]
endt = (datetime.now() + timedelta(0.5*365))
end_time = endt.isoformat().split('T')[0].split('-')[2] + '/' + endt.isoformat().split('T')[0].split('-')[1] + '/' + endt.isoformat().split('T')[0].split('-')[0]
print 'Get POCP between ' + start_time + ' and ' + end_time

def POCP_date_parser(datestr):
    d=datestr.replace('/',' ').replace(':',' ').split(' ')
    return datetime(int('20' + d[2]),int(d[1]),int(d[0]),int(d[3]),int(d[4]))

def groupbywhat(data,what):
    grouped = data.groupby(what)
    groupy = {}
    for whats,sliced in grouped:
        groupy[whats] = sliced
    return Panel(groupy)

def POCP_current(P,curr_wk_4wk):
    def get_curr(tdc):
        X = P[P['Category']==tdc]
        current_bool = (X['Start']<=datetime.today()) & (X['End']>=(datetime.today())) & (X['Planning Status']!='Cancelled')
        week_bool = (X['Start']<=(datetime.today()+timedelta(days=7))) & (X['End']>=(datetime.today()))
        four_weeks_bool = (X['Start']<=(datetime.today()+timedelta(days=28))) & (X['End']>=(datetime.today()))
        if curr_wk_4wk == 'current':
            X = X[current_bool]
        if curr_wk_4wk == 'week':
            X = X[week_bool]
        if curr_wk_4wk == '4week':
            X = X[four_weeks_bool]
        if X.empty:
            print 62*'=' + ' NO ' + curr_wk_4wk.upper() + ' ' + tdc.upper() + ' OUTAGES AT ' + str(datetime.today()) + 62*'='
        else:
            X['Duration'] = X.End-X.Start
            if tdc == 'Transmission':
                del X['MW remaining']
                del X['MW Loss']
                del X['MV remaining']
                print 61*'=' + curr_wk_4wk.upper() + ' ' + tdc.upper() + ' OUTAGES AT ' + str(datetime.today()) + 61*'='
            if tdc == 'Generation':
                del X['Nature']
                X['NP_MWh']=X['Duration'].map(lambda x: x.total_seconds()/3600) * X['MW Loss']
                X=X.sort(columns=['NP_MWh'],ascending = False)
                total_out = X['MW Loss'].sum()
                print 68*'=' + curr_wk_4wk.upper() + ' ' + tdc.upper() + ' OUTAGES AT ' + str(datetime.today()) + ' ***TOTAL=' + str(total_out) + 'MW***' + 68*'=' 
            if tdc == 'Direct Connection':        
                print 75*'=' + curr_wk_4wk.upper() + ' ' + tdc.upper() + ' OUTAGES AT ' + str(datetime.today()) + 75*'='
            X=X.set_index('id')
            del X['Category']
            print X

            return X 

    T = get_curr('Transmission')
    G = get_curr('Generation')
    D = get_curr('Direct Connection')
    
    return T,G,D

#Download the the last year of ther POCP database and whack into a DataFrame with read_csv
br=mechanize.Browser(factory=mechanize.RobustFactory())
br.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(), max_time=1) # Follows refresh 0 but not hangs on refresh > 0
br.addheaders = [('User-agent', 'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1')] # User-Agent (this is cheating, ok?)
r=br.open(cmd_line.pocp_host)
br.select_form(nr=0)
br.submit()  #click I agree

#Now login
br.select_form(nr=0)
br['email'] = cmd_line.pocp_user
br['password'] = cmd_line.pocp_pass
br.submit()  #submit user name and password.
br.select_form(nr=0)     #select form 
br['sview'] = ['excel']  #select "excel" although this is in fact a tab delimited table
br['start'] = start_time #set start and end times from above
br['end'] = end_time
br['planning_status_id[]'] = ['1','2','3'] #['1','2','3']

response = br.submit()   #submit the search/download for all POCP data between start_time and end_time, save as response
bufferIO = StringIO()    #Open a string buffer object, write the POCP database to this then read_csv the data...
bufferIO.write(unicode(response.read())) 
bufferIO.seek(0)
pocp = read_csv(bufferIO,parse_dates=['Start','End','Last Modified'],date_parser=POCP_date_parser,sep='\t')

#add current POCP download to all time POCP data then drop duplicates
store = HDFStore(pocp_path + 'pocp_all.h5','r')
P_all = store['POCP']   # read all previous pocp data
store.close()
P = concat([P_all,pocp])   #add latest download 
P = P.drop_duplicates()    #and drop any duplicates
store = HDFStore(pocp_path + 'pocp_all.h5','w')
store['POCP'] = P  # read all previous pocp data
store.close()
P.to_csv(pocp_path + 'pocp_all.csv') #save the updated POCP database as a csv (good for looking a diffs using gitk)


#Report current confirmed (+tentative) outages
[T,G,D] = POCP_current(pocp,'current')

# <codecell>
#P['End2'] = P['End'].copy()
#mask = ((P['Last Modified']>P['Start']) & (P['Last Modified']<P['End']))
#P['End2'].ix[mask] = P['Last Modified'].ix[mask]
P['End2'] = P['Last Modified'].where(((P['Last Modified']>P['Start']) & (P['Last Modified']<P['End'])),P['End'])
#POCP entries cancelled after the end time
P_cancelled = P[((P['Planning Status']==('Cancelled')) & (P['Last Modified']>=P['End2']))] #return outages that were cancelled after the end date (this is not robust as we use the last modified tag to determine this...lets try anyway)
#All current past and present POCP entries that are/remain confirmed
P_confirmed = P[(P['Planning Status']==('Confirmed'))] 
#For fun look at tentative outages
P_tent = P[(P['Planning Status']==('Tentative'))] 
P_all = P[(((P['Planning Status']==('Cancelled')) & (P['Last Modified']>=P['End2'])) | (P['Planning Status']=='Confirmed'))] #does not include tentative...
P_all_tent = P[(((P['Planning Status']==('Cancelled')) & (P['Last Modified']>=P['End2'])) | (P['Planning Status']=='Confirmed') | (P['Planning Status']=='Tentative'))] #includes tentative...
P_G = P_all[P_all['Category'] == 'Generation']
P_T = P_all[P_all['Category'] == 'Transmission']
P_D = P_all[P_all['Category'] == 'Direct Connection']

#Clean the data as best we can...probably can do better?
P_G = P_G[notnull(P_G.Category)]    #remove any nulls
P_G.Owner[isnull(P_G.Owner)] = 'Other' #set nulls to other if they exist
P_G=P_G[notnull(P_G['GIP/GXPs'])]
P_G['GIP/GXPs']=P_G['GIP/GXPs'].map(lambda x: x[0:3])
P_G = P_G.ix[:,['Start','End2','MW Loss','Outage Block','GIP/GXPs','Owner','id']]
P_G = P_G.rename(columns={'GIP/GXPs':'GIP'})
P_G['GIP'][P_G['GIP']=='#N/']='NAP'
P_G = P_G.dropna(how='any')
P_G = P_G[P_G['MW Loss']>0]
P_G['Island'] = P_G.GIP.map(lambda x: island_map2[x])
P_Gn=P_G[P_G['Island']=='North']
P_Gs=P_G[P_G['Island']=='South']

# <codecell>

def POCP_to_timeseries(P_G,start,stop):
    P_G = P_G.drop_duplicates() #just in case there are any double-ups
    DF = {}
    MI=[]

    def timeify(P_G,start,stop):    #Broadcast the boolean conditions that exist, i.e., the time when generators are off-line according to POCP
        dr=date_range(start,stop, freq='30min')
        P_G = P_G.reset_index()
        P_Gt = Series(0,index=dr)
        for r in P_G.index: #for each row we want to convert it into a timeseries, then += it up
            temp = Series(0,index=dr)
            temp[(P_Gt.index>P_G.ix[r,'Start']) & (P_Gt.index<=P_G.ix[r,'End2'])] = P_G.ix[r,'MW Loss']
            P_Gt+=temp
        return P_Gt

    for i in ['North','South']:
        for o in unique(P_G['Owner'][P_G['Island']==i]):
           for g in unique(P_G['GIP'][((P_G['Island']==i) & (P_G['Owner']==o))]):
               MI.append(tuple([i,o,g,GT[g]]))
               DF[tuple([i,o,g,GT[g]])] = timeify(P_G[((P_G['Island']==i) & (P_G['Owner']==o) & (P_G['GIP']==g))],start,stop)
    DF=DataFrame(DF).T
    DF.index = MultiIndex.from_tuples(DF.index.values,names=['Island','Company','GIP','Type'])
    return DF.T


def plot_fig(x,start_time,end_time,short_long,ymax1,ymax2):
    plt.close('all')
    fig = plt.figure(figsize=[15,13.5])
    plt.subplots_adjust(left=0.1, right=0.9, top=0.9, bottom=0.1)
    ax1 = fig.add_subplot(211)
    x.groupby(level=['Company'],axis=1).sum()['Contact Energy'].plot(c=part['CTCT'],drawstyle='steps-post',ax=ax1,lw=1,label = 'Contact Energy')
    x.groupby(level=['Company'],axis=1).sum()['Genesis'].plot(c = part['GENE'],drawstyle='steps-post',ax=ax1,lw=1,label = 'Genesis')
    x.groupby(level=['Company'],axis=1).sum()['Meridian'].plot(c = part['MERI'],drawstyle='steps-post',ax=ax1,lw=1,label = 'Meridian')
    x.groupby(level=['Company'],axis=1).sum()['Mighty River'].plot(c = part['MRPL'],drawstyle='steps-post',ax=ax1,lw=1,label = 'Mighty River Power')
    x.groupby(level=['Company'],axis=1).sum()['Trustpower'].plot(c = part['TRUS'],drawstyle='steps-post',ax=ax1,lw=1,label = 'Trustpower')
    plt.ylabel('MW')
    plt.title('Current POCP outage data as of ' + str(datetime.now())[:-10])
    plt.legend(loc=2)
    plt.grid('on')
    plt.axvline(datetime.now(),alpha=0.5,lw=2,c=ea_s['pk1'])
    plt.xticks(rotation=0)
    plt.xlim([start_time,end_time])
    plt.ylim([0,ymax1])
    ax2 = fig.add_subplot(212, sharex=ax1)
    x.sum(axis=1).plot(c = ea_p['br1'],drawstyle='steps-post',ax=ax2,lw=1,label = 'Total')
    x.groupby(level=['Island'],axis=1).sum()['North'].plot(drawstyle='steps-post',ax=ax2,c=ea_p['rd1'],label='North Island')
    x.groupby(level=['Island'],axis=1).sum()['South'].plot(drawstyle='steps-post',ax=ax2,c=ea_p['bl1'],label='South Island')
    plt.legend(loc=2)
    plt.ylabel('MW')
    plt.grid('on')
    plt.axvline(datetime.now(),alpha=0.5,lw=2,c=ea_s['pk1'])
    plt.xticks(rotation=0)
    plt.xlim([start_time,end_time])
    plt.ylim([0,ymax2])
    plt.savefig(pocp_path + 'plots/' + short_long + '/pocp_at_' + datetime.now().date().isoformat() + '-' + str(datetime.now().hour) + '.png',axis='tight')
strt = (datetime.now() - timedelta(183))
endt = (datetime.now() + timedelta(183))
x =POCP_to_timeseries(P_G,strt,endt)
plot_fig(x,strt,endt,'long',1800,3000)
strt = (datetime.now() - timedelta(7*2))
endt = (datetime.now() + timedelta(7*4))
plot_fig(x,strt,endt,'short',1800,3000)

