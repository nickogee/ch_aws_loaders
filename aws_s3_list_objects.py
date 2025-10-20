from boto3.session import Session
from config.cred.enviroment import Environment



env = Environment()
bucket_name = env.aws_s3_bucket_name
region_name = env.aws_s3_region_name
ACCESS_KEY = env.aws_s3_access_key
SECRET_KEY = env.aws_s3_secret_key


session = Session(aws_access_key_id=ACCESS_KEY,
                aws_secret_access_key=SECRET_KEY,
                region_name = region_name)
s3 = session.resource('s3')
your_bucket = s3.Bucket(bucket_name)

# Define the specific path you want to list objects from
target_path = "partner_metrics/backend_events/delivered_orders"
# target_path = "financial_metrics/transactions/init/"

print(f"Listing objects in path: {target_path}")
print("-" * 50)

# List objects with the specified prefix (path)
for s3_file in your_bucket.objects.filter(Prefix=target_path):
    print(f'--- {s3_file.key}')