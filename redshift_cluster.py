import requests
import pandas as pd
import boto3
from configparser import ConfigParser
import psycopg2
import json

config = ConfigParser()
config.read('cluster.config')

#print(config.get("aws","aws_secret_access_key"))

# AWS Configuration
region = config.get('aws', 'region')
access_key = config.get('aws', 'access_key', fallback=None)
secret_key = config.get('aws', 'secret_key', fallback=None)

# Redshift Cluster Configuration
cluster_Type = config.get('redshift', 'cluster_Type')
cluster_identifier = config.get('redshift', 'cluster_identifier')
db_name = config.get('redshift', 'db_name')
node_type = config.get('redshift', 'node_type')
master_username = config.get('redshift', 'master_username')
master_password = config.get('redshift', 'master_password')
number_of_nodes = config.get('redshift', 'number_of_nodes')
iam_role_arn = config.get('redshift', 'iam_role_name')

#establish connection
s3 = boto3.resource('s3',
                        region_name = region,
                        aws_access_key_id=access_key,
                        aws_secret_access_key= secret_key)

iam = boto3.client('iam',
                        region_name = region,
                        aws_access_key_id=access_key,
                        aws_secret_access_key= secret_key)

redshift = boto3.client('redshift',
                        region_name = region,
                        aws_access_key_id=access_key,
                        aws_secret_access_key= secret_key)
EC2 = boto3.resource('ec2',
                        region_name = region,
                        aws_access_key_id=access_key,
                        aws_secret_access_key= secret_key
)

#read s3 objects
bucket = s3.Bucket("web-analytics-input")
data_file = [filename.key for filename in bucket.objects.all()]
print(data_file)

#provide IAM role to redshift for s3 access
roleArn = iam.get_role(RoleName=iam_role_arn)['Role']['Arn']
print(roleArn)


#Create redshift cluster
try:
response = redshift.create_cluster(
        ClusterType=cluster_Type,
        NodeType=node_type,

        #identifier & credentials
        DBName=db_name,
       ClusterIdentifier=cluster_identifier,
       MasterUsername=master_username,
       MasterUserPassword=master_password,

     #roles
       IamRoles=[roleArn]
    )

except Exception as e:
    print(e)

#print cluster values and items

def preetyRedshiftProps(props):
    pd.set_option('display.max_colwidth',50)
    keysToShow = ["ClusterIdentifier","NodeType","ClusterStatus","MasterUsername","DBName","Endpoint","VpcId"]
    x = [(k,v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["key","Value"])

myClusterprops = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]


From_port = myClusterprops['Endpoint']['Port']
To_Port = myClusterprops['Endpoint']['Port']
Cluster_EndPoint = myClusterprops['Endpoint']['Address']

#print(f"Cluster_EndPoint is {Cluster_EndPoint}")


#attach vpc to cluster

try:
    vpc = ec2.Vpc(id=myClusterprops['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)

    defaultSg.authorise_ingress(
        GroupName=defaultSg.group_name,
        CidrIP='0.0.0.0/0',
        IpProtocol= 'TCP',
        FromPort=int(From_port),
        ToPort=int(To_Port)
    )

except Exception as e:
    print(EC2)

 #Connecting to Redshift
try:
    conn = psycopg2.connect(host=Cluster_EndPoint,dbname=db_name, user=master_username, password=master_password, port=To_Port)
    print("Connected to the database!")

except Exception as e:
    print(f"Error: {e}")

# set session with database
conn.set_session(autocommit=True)

def connect_to_redshift(cluster_endpoint, db_name, master_username, master_password, port):
    
    try:
        # Connecting to Redshift
        conn = psycopg2.connect(
            host=cluster_endpoint,
            dbname=db_name,
            user=master_username,
            password=master_password,
            port=To_Port
        )
        print("Connected to the database!")
        # Set session with the database
        conn.set_session(autocommit=True)
        return conn

    except Exception as e:
        print(f"Error: {e}")
        return None
    
# Call the function to get the conn object
conn = connect_to_redshift(Cluster_EndPoint, db_name, master_username, master_password, To_Port)


#create cursor to database
try:
    cur = conn.cursor()
    print("cursor created")

except psycopg2.Error as e:
    print("Error: could not get cursor to the DB")
    print(e)

 # Define the table schema, create db table in redshift
    
try:
    cur.execute("""CREATE TABLE website_traffic (
        website VARCHAR(255),
        year INT,
        pagepath VARCHAR(255),
        pageurl VARCHAR(255),
        pageviews INT,
        uniqueviews INT,
        avgtimeonpage FLOAT,
        entrances FLOAT,
        bouncerate FLOAT,
        exitrate FLOAT
    );""")
except psycopg2.Error as e:
    print("Error: Issue creating Table")
    print(e)

#insert data from s3 to redshift db
try:
    cur.execute("""
    COPY website_traffic FROM 's3://web-analytics-input/analytics_input.csv'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::325297895154:role/web-analytics-redshift-s3'
    DELIMITER ','
    IGNOREHEADER 1
    REMOVEQUOTES
    REGION 'us-east-1'         
    """)
except psycopg2.Error as e:
    print("Error: issue while inserting data")
    print(e)

#execute quries

try:
    cur.execute("""
    select * from website_traffic;         
    """)
except psycopg2.Error as e:
    print("Error: issue while quering data")
    print(e)
row = cur.fetchone()
print(row)


#close the connection
try:
    cur.close()
except psycopg2.Error as e:
    print(e)
