#create db table in redshift
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
    CREDENTIALS 'aws_iam_role=arn:aws:iam::your role'
    DELIMITER ','
    IGNOREHEADER 1
    REMOVEQUOTES
    REGION 'us-east-1'         
    """)
except psycopg2.Error as e:
    print("Error: issue while inserting data")
    print(e)
