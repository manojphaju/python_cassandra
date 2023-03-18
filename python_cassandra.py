# pip install cassandra-driver

from cassandra.cluster import Cluster

# here Cluster() has multiple ip address for different cluster that data needs to be write
# we are only using here one cluster ie localhost 
# so we are not specifying the ip address

cluster = Cluster()

# connecting with the cluster name 
session = cluster.connect('test_keyspace')
# inserting data to table in to the cluster
session.execute("INSERT INTO python_test(id, first_name, last_name) VALUES(uuid(), 'harry','porter')")

print('success')

# reading data from the cluster

read_data = session.execute("SELECT * FROM python_test;")

for data in read_data:
    print(data.id, data.first_name, data.last_name)

