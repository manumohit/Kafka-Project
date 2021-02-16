# Kafka producer

from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

def jsonSerializer(data):
	return json.dumps(data).encode('utf-8')

producer = KafkaProducer(bootstrap_servers = ['localhost:9092'],value_serializer = jsonSerializer)

if __name__ == "__main__":

	brand_list = ['Wayfair','Dutil','Think Crucial',' Hollar','Knoxlabs','Everytable','Harrys','Magnolia_Market','levis','puma']
	description_list = ['Best in market','affordable','lightweight','simple and sweet']
	size_list = ['XL','L','XXL','M','W']
	category_list = ['Armwear','Belts','Coats','Dresses','Footwear','Gowns','Headgear','Hosiery','Jackets']
	sub_category_list = ['Masks','Neckwear','Onepiecesuits','ponchos','robes','saris','sportswear','suits']
	price_list = ['8000','200','1000','10000','5000','9000','1500','7200','1340','2276']
	quantity_list = ['500','1000','100','250','200','800','600','2000','324']
	country_list = ['ind','eng','aus','jap','sgpr','usa','china','france']
	seller_code_list = ['110','111','112','113','114','115','116','117','118','119','110']
	stock_list = ['A','B','C','D','E','F','G','H','I','J']
	Supc_list = ['AA','BB','CC','DD','EE','FF','GG','HH','II','JJ','KK']

	for i in range(1,1040216):
		now = datetime.now()
		userinfo = {'PogId':i,'Supc':random.choice(Supc_list),'Brand': random.choice(brand_list),'Description':random.choice(description_list),'Size':random.choice(size_list),'Category':random.choice(category_list),'Sub_Category':random.choice(sub_category_list),'Price':random.choice(price_list),'Quantity':random.choice(quantity_list),'Country':random.choice(country_list),'Seller_Code':random.choice(seller_code_list),"Creation_time":str(now.strftime("%H:%M:%S")),'Stock':random.choice(stock_list)}
		print(userinfo)
		producer.send('user_info',userinfo)
		#time.sleep(5)
