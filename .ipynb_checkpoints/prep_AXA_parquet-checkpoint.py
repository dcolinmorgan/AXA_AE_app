import pyarrow as pa
import pyarrow.parquet as pq

NL=pd.read_parquet('run/AXA_AE_app/data/AE_AXA_asthma.parquet')
# lung=pd.read_parquet('run/AXA_AE/data/AE_AXA_all_LUNG.parquet')
# NL=lung[lung['diag1'].str.contains('COPD')]
# NL=lung[lung['diag1'].str.contains('Asthma')]


NL.rename(columns={'s6':'date'},inplace=True)
NL.rename(columns={'s2':'sex'},inplace=True)
NL.rename(columns={'s3':'age'},inplace=True)

NL['date']=pd.to_datetime(NL['date'])
NL=NL.sort_values(by='date')
NL['age']=np.round(NL['age'].astype(float))
NL['sex'] = NL['sex'].apply({'F':0, 'M':1}.get)
NL['sex']=np.round(NL['sex'].astype(float))

plt.hist(NL['age'])
print([len(NL[NL['age']>21]),len(NL[NL['age']<18])]) ## check COPD, M v F, rural v city (test labels ~CBD)

oNL=NL[NL['age']>21]
oNL['group']=1
yNL=NL[NL['age']<18]
yNL['group']=0
NL=oNL.append(yNL)


d2=NL.groupby(by=['s1','date','group']).agg({'diag1':'count','sex':'mean','age':'median'})
d2['sex']=np.round(d2['sex'].astype(float))
# d2['age']=np.round(d2['age'].astype(float))

# table = pa.Table.from_pandas(d2.astype(str))
# pq.write_table(table, 'run/AXA_AE/AE_AXA_asthma_count.parquet')

d2.reset_index(inplace=True)
d2['age']=np.round(d2['age'].astype(float))

d2.rename(columns={'s1':'cd9_loc'},inplace=True)
d2['cd9_loc'].replace({'RH':'Ruttonjee Hospital'},regex=True,inplace=True)
d2['cd9_loc'].replace({'PYN':'Pamela Youde Nethersole Eastern Hospital'},regex=True,inplace=True)
d2['cd9_loc'].replace({'QEH':'Queen Elizabeth Hospital'},regex=True,inplace=True)
d2['cd9_loc'].replace({'CMC':'Caritas Medical Centre'},regex=True,inplace=True)
d2['cd9_loc'].replace({'KWH':'Kwong Wah Hospital'},regex=True,inplace=True)
d2['cd9_loc'].replace({'TMH':'Tuen Mun Hospital'},regex=True,inplace=True)
d2['cd9_loc'].replace({'PWH':'Prince of Wales Hospital'},regex=True,inplace=True)
d2['cd9_loc'].replace({'NDH':'North District Hospital'},regex=True,inplace=True)
d2['cd9_loc'].replace({'YCH':'Yan Chai Hospital'},regex=True,inplace=True)
d2['cd9_loc'].replace({'UCH':'United Christian Hospital'},regex=True,inplace=True)
d2['cd9_loc'].replace({'QMH':'Queen Mary Hospital'},regex=True,inplace=True)
d2['cd9_loc'].replace({'PWH':'Princess Margaret Hospital'},regex=True,inplace=True)
d2['cd9_loc'].replace({'POH':'Pok Oi Hospital'},regex=True,inplace=True)
d2['cd9_loc'].replace({'TKO':'Tseung Kwan O Hospital'},regex=True,inplace=True)
d2['cd9_loc'].replace({'AHN':'Alice Ho Miu Ling Nethersole Hospital'},regex=True,inplace=True)
d2['cd9_loc'].replace({'SJH':'St. John Hospital'},regex=True,inplace=True)
d2['cd9_loc'].replace({'NLT':'North Lantau Hospital'},regex=True,inplace=True)
d2['cd9_loc'].replace({'TSH':'Tang Shiu Kin Hospital'},regex=True,inplace=True)
d2['cd9_loc'].replace({'PMH':'Princess Margaret Hospital'},regex=True,inplace=True)

# from varname import nameof
cc=pd.DataFrame()#(columns=['date','pm25','pm10','o3','no2','so2','co','loc'])
files=glob.glob('aqi-stations-scraper/data/japan-aqi/*')
for file in files:
    data=pd.read_csv(file,sep=' |,',engine='python')
    data['loc']=os.path.basename(file).split(',')[0]
    cc=pd.concat([cc,data]) #cc.append(data)
    
data2=cc[['date','pm25','pm10','o3','no2','so2','co','loc']]
data2.date=data2.date.astype(np.datetime64)
data2['year']=data2['date'].dt.year
data2['week']=data2['date'].dt.week


d2['cd9_loc'].replace('centralnaya-str','central',inplace=True)
d2['cd9_loc'].replace('southern','southern island',inplace=True)
d2['cd9_loc'].replace('southern-part of chengyang district','chengyang district',inplace=True)
geolocator = Nominatim(user_agent="example app")
data_loc=pd.DataFrame(columns=['lat','long','name'])

JEFFFF=pd.concat([pd.Series(pd.unique(d2['cd9_loc'])),pd.Series(pd.unique(data2['loc']))], ignore_index=True)
for ii,i in enumerate(JEFFFF):
    try:
        a,b,c=geolocator.geocode(str(i)+", Hong Kong").point
    except AttributeError:
        print('no location data')
    data_loc[ii]=[a,b,i]
data_loc=data_loc.transpose()
data_loc.columns=['lat','long','name']
data_loc=data_loc[3:]

# geopy DOES use latlon configuration
data_loc['latlon'] = list(zip(data_loc['lat'], data_loc['long']))
square = pd.DataFrame(
    np.zeros((data_loc.shape[0], data_loc.shape[0])),
    index=data_loc.index, columns=data_loc.index
)

# replacing distance.vicenty with distance.distance
def get_distance(col):
    end = data_loc.loc[col.name, 'latlon']
    return data_loc['latlon'].apply(geopy.distance.distance,
                              args=(end,),
                              ellipsoid='WGS-84'
                             )

distances = square.apply(get_distance, axis=1).T

from sklearn.cluster import KMeans#, DBSCAN

X=np.array(data_loc[['lat','long']],dtype='float64')
k=16
model = KMeans(n_clusters=k,algorithm='full',random_state=2).fit(X) #algorithm{“lloyd”, “elkan”, “auto”, “full”
class_predictions=model.predict(X)
data_loc['kmeans{k}'] = class_predictions

# 

data_loc.at[39,'kmeans{k}'] = data_loc[data_loc['name']=='North District Hospital']['kmeans{k}']
data_loc.at[32,'kmeans{k}'] = data_loc[data_loc['name']=='St. John Hospital']['kmeans{k}']


RR=pd.DataFrame(data_loc.groupby(['kmeans{k}'])['name'].apply(','.join))

cc=data_loc.iloc[:18]
# cc['region']=[1,0,0,1,1,0,1,1,0,0,0,0,1,1,1,0,0,0]##0=urban, 1=rural manually searched in gmaps
# cc['region']=[1,1,1,1,1,0,1,1,0,0,0,0,1,1,1,0,1,1]##0=urban, 1=rural manually searched in gmaps
RR['region']=[1,0,1,0,1,1,0,0,1,0,1,0,1,0]
# RR['region']=[1,0,1,0,1,1,0,0,1,0,1*,0*,1,0]

dd=cc.merge(RR,on='kmeans{k}')
ee=data_loc.iloc[18:]
ff=dd.merge(ee,on='kmeans{k}')
# ff.to_csv('run/AXA_AE_app/data/master_data_loc',sep='\t')

gg=d2.merge(ff,left_on='cd9_loc',right_on='name_x')
# time lag
# gg['date']=gg['date'].shift(+7)
d6=gg.merge(data2,left_on=['name','date'],right_on=['loc','date'])

[len(d6[d6.diag1<3]),len(d6[d6.diag1>4])]

# del d6['new']
# d6['new']=5
# d6[d6['diag1']<5]['new']#=0
# a=d6.diag1
# d6.diag1=np.where(a <3,a,0)#,inplace=True)
# d6.diag1=np.where(a ==4,a,2)
# d6=d6[~d6.diag1==2]
# d6.diag1=np.where(a >4,a,1)#,inplace=True)
# d6[d6['diag1']>10]['new']=1
d6.loc[d6['diag1'] <3, 'new'] = 0
d6.loc[d6['diag1'] >4, 'new'] = 1
d6.loc[d6['diag1'] ==4, 'new'] = 4
plt.hist(d6.new)
d6=d6[d6['new']!=4]

d6.reset_index(inplace=True)
d6=d6.reset_index().rename(columns={'index':'ds'})
d6['ds']=d6['ds'].astype('float64')

d6.to_parquet('run/AXA_AE_app/AE_AXA_poll-ae.parquet', compression='GZIP')#.mode('overwrite')
