from kafka import KafkaConsumer
import json
import pymysql 

def mysqlconnect(): 
    # To connect MySQL database 
    conn = pymysql.connect( 
        host='localhost', 
        user='root',  
        password = "root", 
        db='project', 
        ) 
    return conn

if __name__ == "__main__":
	
	
	# getting data from the producer
	consumer = KafkaConsumer('user_info', group_id = 'usergroup1', bootstrap_servers = 'localhost:9092', auto_offset_reset = 'earliest') 
	conn = mysqlconnect()
	cur = conn.cursor() 
	count = 0
	for msg in consumer:

		data = json.loads(msg.value) # loading json into python dictionary
		PogId = data['PogId']
		Supc = data['Supc']
		price = data['Price']
		quantity = data['Quantity']
		query = f"insert into products (PogID,Supc,Price,Quantity) values ('{PogId}','{Supc}','{price}','{quantity}') on duplicate key update Supc='{PogId}',Price='{price}',Quantity='{quantity}';"
		#print(query)
		cur.execute(query)
		conn.commit()

	conn.close() 
  
