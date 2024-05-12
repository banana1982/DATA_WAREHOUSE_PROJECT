import configparser
import pandas as pd
import boto3
import json
from botocore.exceptions import ClientError

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

S3_LOG_DATA = config.get("S3","LOG_DATA")
S3_LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
S3_SONG_DATA = config.get("S3","SONG_DATA")

ROLE_NAME = config.get("IAM_ROLE","ROLE_NAME")

pd.DataFrame({"Param":
            ["CLUSTER_TYPE", "NUM_NODES", "NODE_TYPE", "HOST", "DB_NAME", "DB_USER", "DB_PASSWORD", "DB_PORT", "ROLE_NAME"],
        "Value":
            [CLUSTER_TYPE, NUM_NODES, NODE_TYPE, HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT, ROLE_NAME]
        })

s3 = boto3.resource('s3',
        region_name="us-west-2",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )
ec2 = boto3.resource('ec2',
        region_name="us-west-2",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )
iam = boto3.client('iam',
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
        region_name='us-west-2'
    )
redshift = boto3.client('redshift',
        region_name="us-west-2",
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET
    )

def initRole():
    #1.1 Create the role, 
    try:
        print("1.1 Creating a new IAM Role") 
        myRole = iam.create_role(
            Path='/',
            RoleName=ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )
    except Exception as e:
        print(e)
        
        
    print("1.2 Attaching Policy")

def attachRole():
    iam.attach_role_policy(RoleName=ROLE_NAME,
                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                        )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=ROLE_NAME)['Role']['Arn']

    print(roleArn)

def goPublic():
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DB_PORT),
            ToPort=int(DB_PORT)
        )
    except Exception as e:
        print(e)

    endPoint = myClusterProps['Endpoint']['Address']
    roleArn = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("ENDPOINT :: ", endPoint)
    print("ROLE_ARN :: ", roleArn)

def initCluster():
    try:
        response = redshift.create_cluster(
            #HW
            ClusterType=CLUSTER_TYPE,
            NodeType=NODE_TYPE,
            NumberOfNodes=int(NUM_NODES),

            #Identifiers & Credentials
            DBName=DB_NAME,
            ClusterIdentifier=HOST,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,
            
            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )
    except Exception as e:
        print(e)

def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

def main():
    # Init Role
    initRole()

    # Attach Role
    attachRole()
    
    # Init Cluster Redshift
    initCluster()
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=HOST)['Clusters'][0]
    prettyRedshiftProps(myClusterProps)

    # Go public for DB
    goPublic()

    # ENDPOINT = myClusterProps['Endpoint']['Address']
    # ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']

    print('Connect AWS Redshift successfuly!')


if __name__ == "__main__":
    main()