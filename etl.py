import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import boto3

config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY=config.get('AWS','KEY')
SECRET= config.get('AWS','SECRET')

HOST= config.get("DB","HOST")
DB_USER= config.get("DB","DB_USER")
DB_NAME= config.get("DB","DB_NAME")
DB_PASSWORD= config.get("DB","DB_PASSWORD")
DB_PORT = config.get("DB","DB_PORT")

CLUSTER_TYPE = config.get("CLUSTER","CLUSTER_TYPE")
NUM_NODES = config.get("CLUSTER","NUM_NODES")
NODE_TYPE = config.get("CLUSTER","NODE_TYPE")

ROLE_NAME = config.get("IAM_ROLE","ROLE_NAME")


def load_staging_tables(cur, conn):
    """ Load data and copy to staging tables """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ Insert data staging tables to tables postgres AWS Redshift. """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    try:
        redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=HOST)['Clusters'][0]
        ENDPOINT = myClusterProps['Endpoint']['Address']

        conn = psycopg2.connect("postgresql://{}:{}@{}:{}/{}".format(DB_USER, DB_PASSWORD, ENDPOINT, DB_PORT,DB_NAME))
        cur = conn.cursor()
        
        load_staging_tables(cur, conn)
        insert_tables(cur, conn)

        conn.close()
    except psycopg2.Error as e:
        print(e)


if __name__ == "__main__":
    main()