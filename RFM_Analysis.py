import pandas as pd
import datetime as dt
pd.set_option("display.max_columns", None)
pd.set_option("display.float_format", lambda x: "%.1f" % x)


# Adım 1: flo_data_20K.csv verisini okuyunuz.Dataframe’in kopyasını oluşturunuz.
df_ = pd.read_csv(r"C:\Users\arsla\PycharmProjects\pythonProject\zahide\flo_data_20k.csv")
df = df_.copy()

# Adım 2: Veri setinde
# a. İlk 10 gözlem,
df.head(10)

# b. Değişken isimleri,
df.columns
# c. Betimsel istatistik,
df. describe().T
df.shape
# d. Boş değer,
df.isnull().sum()
# e. Değişken tipleri, incelemesi yapınız.
df.dtypes
df
# Adım 3: Omnichannel müşterilerin hem online'dan hemde offline platformlardan alışveriş yaptığını ifade etmektedir. Her bir müşterinin toplam
# alışveriş sayısı ve harcaması için yeni değişkenler oluşturunuz.
df["top_alisveris"] = df["order_num_total_ever_offline"] + df["order_num_total_ever_online"]
df["top_harcama"] = df["customer_value_total_ever_online"] + df["customer_value_total_ever_offline"]


# Adım 4: Değişken tiplerini inceleyiniz. Tarih ifade eden değişkenlerin tipini date'e çeviriniz.
df.dtypes
df["first_order_date"] = df["first_order_date"].astype("datetime64")
df["last_order_date"]=df["last_order_date"].astype("datetime64")
df[["last_order_date_online", "last_order_date_offline"]] = df[["last_order_date_online","last_order_date_offline"]].astype("datetime64")




# Adım 5: Alışveriş kanallarındaki müşteri sayısının, toplam alınan ürün sayısının ve toplam harcamaların dağılımına bakınız.

df.groupby("order_channel").agg( {"top_alisveris" : "sum"})
df.groupby("order_channel")[["top_alisveris", "top_harcama"]].sum()
df["order_channel"].value_counts()



# Adım 6: En fazla kazancı getiren ilk 10 müşteriyi sıralayınız.
df["top_harcama"].sort_values(ascending=False).head(10)

# Adım 7: En fazla siparişi veren ilk 10 müşteriyi sıralayınız.
df["top_alisveris"].sort_values(ascending=False).head(10)

# Adım 8: Veri ön hazırlık sürecini fonksiyonlaştırınız
def data_prep(x):
     #dataframe = x
     #veri hazırlık
     x["top_alisveris"] = x["order_num_total_ever_offline"] + x["order_num_total_ever_online"]
     x["top_harcama"] = x["customer_value_total_ever_online"] + x["customer_value_total_ever_offline"]
     x[["first_order_date","last_order_date"]] =x[["first_order_date","last_order_date"]].astype("datetime64")
     x[["last_order_date_online", "last_order_date_offline"]] = x[["last_order_date_online", "last_order_date_offline"]].astype("datetime64")
     x.groupby("order_channel").agg({"top_alisveris": "sum"})
     x.groupby("order_channel")[["top_alisveris", "top_harcama"]].sum()
     x["order_channel"].value_counts()
     return x


data_prep(df)


# Görev 2: RFM Metriklerinin Hesaplanması

df.head()
df.columns

# Adım 1: Recency, Frequency ve Monetary tanımlarını yapınız.


# Adım 2: Müşteri özelinde Recency, Frequency ve Monetary metriklerini hesaplayınız.
today_date = dt.datetime(2021,6,2)
df["last_order_date"].max()
df.groupby("master_id").agg({"last_order_date": lambda last_order_date: (today_date - last_order_date.max()).days,
                             "top_alisveris": lambda x: x.nunique(),
                            "top_harcama": lambda x: x.sum()})


# Adım 3: Hesapladığınız metrikleri rfm isimli bir değişkene atayınız.
rfm_metrikleri = df.groupby("master_id").agg({"last_order_date": lambda last_order_date: (today_date - last_order_date.max()).days,
                             "top_alisveris": lambda x: x.sum(),
                            "top_harcama": lambda x: x.sum()})

# Adım 4: Oluşturduğunuz metriklerin isimlerini recency, frequency ve monetary olarak değiştiriniz.
rfm_metrikleri.columns = ["recency","frequency","monetary"]
rfm_metrikleri.head()
rfm_metrikleri[rfm_metrikleri["recency"] >0]

#
# Görev 3: RF Skorunun Hesaplanması
# Adım 1: Recency, Frequency ve Monetary metriklerini qcut yardımı ile 1-5 arasında skorlara çeviriniz.


# Adım 2: Bu skorları recency_score, frequency_score ve monetary_score olarak kaydediniz.
#
rfm_metrikleri["recency_score"] = pd.qcut(rfm_metrikleri["recency"] , 5, labels= [5,4,3,2,1])

rfm_metrikleri["frequency_score"] = pd.qcut(rfm_metrikleri["frequency"].rank(method="first"), q = 5, labels= [1,2,3,4,5])

rfm_metrikleri["monetary_score"] = pd.qcut(rfm_metrikleri["monetary"] , q = 5, labels= [1,2,3,4,5])

rfm_metrikleri
# Adım 3: recency_score ve frequency_score’u tek bir değişken olarak ifade ediniz ve RF_SCORE olarak kaydediniz.
rfm_metrikleri["RF_SCORE"] = rfm_metrikleri["recency_score"].astype(str) + rfm_metrikleri["frequency_score"].astype(str)



# Görev 4: RF Skorunun Segment Olarak Tanımlanması
# Adım 1: Oluşturulan RF skorları için segment tanımlamaları yapınız.
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


# Adım 2: Aşağıdaki seg_map yardımı ile skorları segmentlere çeviriniz.
rfm_metrikleri["segment"] = rfm_metrikleri["RF_SCORE"].replace(seg_map, regex=True)

# Görev 5: Aksiyon Zamanı ! Copyright © Miuul, Inc. All Rights Reserved
# Adım 1: Segmentlerin recency, frequnecy ve monetary ortalamalarını inceleyiniz.
rfm_metrikleri[["segment","recency","frequency","monetary"]].groupby("segment").agg("mean")

# Adım 2: RFM analizi yardımıyla aşağıda verilen 2 case için ilgili profildeki müşterileri bulun ve müşteri id'lerini csv olarak kaydediniz.
# a. FLO bünyesine yeni bir kadın ayakkabı markası dahil ediyor. Dahil ettiği markanın ürün fiyatları genel müşteri
# tercihlerinin üstünde. Bu nedenle markanın tanıtımı ve ürün satışları için ilgilenecek profildeki müşterilerle özel olarak
# iletişime geçmek isteniliyor. Sadık müşterilerinden(champions, loyal_customers) ve kadın kategorisinden alışveriş
# yapan kişiler özel olarak iletişim kurulacak müşteriler. Bu müşterilerin id numaralarını csv dosyasına kaydediniz.

new_df = pd.DataFrame()
# new_df.set_index(rfm_metrikleri["master_id"])
rfm_metrikleri.columns
criteria_df =rfm_metrikleri[(rfm_metrikleri.segment=="champions") | (rfm_metrikleri.segment=="loyal_customers") ]["segment"]
indexler = df[df["interested_in_categories_12"].str.contains("KADIN")]["master_id"]

inde

criteria_df[criteria_df.index = indexler]



new_df.to_csv("champions_and_loyal.csv")
rfm_metrikleri[rfm_metrikleri["segment"] == "champions"]

df.shape


type("interested_in_categories_12")
df[df["interested_in_categories_12"].str.contains("KADIN")]["master_id"]


# b. Erkek ve Çocuk ürünlerinde %40'a yakın indirim planlanmaktadır. Bu indirimle ilgili kategorilerle ilgilenen geçmişte
# iyi müşteri olan ama uzun süredir alışveriş yapmayan kaybedilmemesi gereken müşteriler, uykuda olanlar ve yeni
# gelen müşteriler özel olarak hedef alınmak isteniyor. Uygun profildeki müşterilerin id'lerini csv dosyasına kaydediniz.
#
# new_df2 = pd.DataFrame()
# new_df2 = rfm_metrikleri[rfm_metrikleri["segment"]=="hibernating" & rfm_metrikleri["segment"]=="cant_loose" & rfm_metrikleri["segment"]=="new_customers"]
# new_df2.to_csv("champions_and_loyal.csv")