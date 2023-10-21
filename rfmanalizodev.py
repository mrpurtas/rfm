import datetime as dt
import pandas as pd
pd.set_option('display.max_columns', None)
#pd.set_option('display.max_rows', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
# Veri seti son alışverişlerini 2020 - 2021 yıllarında OmniChannel(hem online hem offline alışveriş yapan) olarak yapan müşterilerin geçmiş alışveriş davranışlarından
# elde edilen bilgilerden oluşmaktadır.

# master_id: Eşsiz müşteri numarası
# order_channel : Alışveriş yapılan platforma ait hangi kanalın kullanıldığı (Android, ios, Desktop, Mobile, Offline)
# last_order_channel : En son alışverişin yapıldığı kanal
# first_order_date : Müşterinin yaptığı ilk alışveriş tarihi
# last_order_date : Müşterinin yaptığı son alışveriş tarihi
# last_order_date_online : Muşterinin online platformda yaptığı son alışveriş tarihi
# last_order_date_offline : Muşterinin offline platformda yaptığı son alışveriş tarihi
# order_num_total_ever_online : Müşterinin online platformda yaptığı toplam alışveriş sayısı
# order_num_total_ever_offline : Müşterinin offline'da yaptığı toplam alışveriş sayısı
# customer_value_total_ever_offline : Müşterinin offline alışverişlerinde ödediği toplam ücret
# customer_value_total_ever_online : Müşterinin online alışverişlerinde ödediği toplam ücret
# interested_in_categories_12 : Müşterinin son 12 ayda alışveriş yaptığı kategorilerin listesi
dataframe["last_order_date"] = pd.to_datetime(dataframe["last_order_date"])
dataframe["first_order_date"] = pd.to_datetime(dataframe["first_order_date"])
dataframe["order_num_total_ever_offline"] = pd.to_datetime(dataframe["order_num_total_ever_offline"])
dataframe["order_num_total_ever_online"] = pd.to_datetime(dataframe["order_num_total_ever_online"])


data1 = pd.read_csv("/Users/mrpurtas/Downloads/FLOMusteriSegmentasyonu/flo_data_20k.csv")
df = df_.copy()

df.head()
df.head()
df.columns
df.describe().T
df.isnull().sum()

df.dtypes

df["total_order_num"] = df["order_num_total_ever_offline"] + df["order_num_total_ever_online"]

df["total_order_price"] = df["customer_value_total_ever_offline"] + df["customer_value_total_ever_online"]



date_columns = ['first_order_date', 'last_order_date', 'last_order_date_online', 'last_order_date_offline']
for col in date_columns:
    df[col] = pd.to_datetime(df[col])



#df["last_order_date"] = pd.to_datetime(df["last_order_date"])
#df["first_order_date"] = pd.to_datetime(df["first_order_date"])

#df["order_num_total_ever_offline"] = pd.to_datetime(df["order_num_total_ever_offline"])
#df["order_num_total_ever_online"] = pd.to_datetime(df["order_num_total_ever_online"])


df.groupby("order_channel").agg({"order_channel": lambda x: x.value_counts(),
                                 "total_order_num": lambda y: y.sum(),
                                 "total_order_price": lambda z: z.sum()})

df.groupby("master_id").agg({"total_order_price": "sum"}).sort_values(by="total_order_price", ascending=False).head(10)
df.groupby("master_id").agg({"total_order_num": "sum"}).sort_values(by="total_order_num", ascending=False).head(10)

#########################################################################################################################
def create_data_prep(dataframe):
    df = dataframe.copy()
    df["total_order_num"] =df["order_num_total_ever_offline"] + df["order_num_total_ever_online"]
    df["total_order_price"] = df["customer_value_total_ever_offline"] + df["customer_value_total_ever_online"]
    date_columns = ['first_order_date', 'last_order_date', 'last_order_date_online', 'last_order_date_offline']
    for col in date_columns:
        df[col] = pd.to_datetime(df[col])
    ordered = df.groupby("order_channel").agg({"order_channel": lambda x: x.value_counts(),
                                            "total_order_num": lambda y: y.sum(),
                                            "total_order_price": lambda z: z.sum()})
    top_10_revenue = df.groupby("master_id").agg({"total_order_price": "sum"}).sort_values(by="total_order_price", ascending=False).head(10)
    top_10_order_customers = df.groupby("master_id").agg({"total_order_num": "sum"}).sort_values(by="total_order_num", ascending=False).head(10)

    return df, top_10_revenue, top_10_order_customers, ordered
#########################################################################################################################

create_data_prep(data1)
df.head()

df["last_order_date"].max()

today_date = dt.datetime(2021, 6, 1)

rfm = df.groupby('master_id').agg({'last_order_date': lambda x: (today_date - x.max()).days,
                                     'master_id': lambda y: y.nunique(),
                                     'total_order_price': lambda z: z.sum()}).head(10)
rfm.columns = ["recency", "frequency", "monetary"]

rfm.head()

rfm["recency_score"] = pd.qcut(rfm['recency'], 5, labels=[5, 4, 3, 2, 1])

rfm["frequency_score"] = pd.qcut(rfm['frequency'].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])

rfm["monetary_score"] = pd.qcut(rfm['monetary'], 5, labels=[1, 2, 3, 4, 5])

rfm["RF_SCORE"] = (rfm['recency_score'].astype(str) +
                    rfm['frequency_score'].astype(str))

rfm.head()

seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}
rfm['segment'] = rfm['RF_SCORE'].replace(seg_map, regex=True)

rfm.head()

df.head()
segment_means = rfm.groupby("segment").agg({"recency": "mean", 'frequency': "mean", "monetary": "mean"})

df.head(10)
rfm.columns

new_df = pd.DataFrame()
new_df["new_customer_id"] = rfm[(rfm["segment"] == "champions") | (rfm["segment"] == "loyal_customers")].index
new_df

#df indexim master idlerden olusmadıgı için indexe master id atıyorum

df.set_index("master_id", inplace=True)

best_cust = rfm[rfm["segment"].isin(["loyal_customer", "champions"])].index

woman_cust = df[df["interested_in_categories_12"].str.contains("KADIN")].index


df_best_cust = pd.DataFrame(best_cust)

df_woman_cust = pd.DataFrame(woman_cust)

result1 = pd.concat([df_woman_cust, df_best_cust]).drop_duplicates(keep=False)

#### en sonda keşisen idleri attım

result1.to_csv("result1.csv")