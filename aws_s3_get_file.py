from boto3.session import Session
from botocore.exceptions import ClientError
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

def download_s3_file(bucket_obj, key: str, local_filename: str):
    """Downloads a file from S3."""
    try:
        print(f"Attempting to download s3://{bucket_obj.name}/{key} to {local_filename}")
        bucket_obj.download_file(Key=key, Filename=local_filename)
        print(f"Successfully downloaded {key} to {local_filename}")
    except ClientError as e:
        print(f"Error downloading file {key}: {e}")
        raise

def delete_s3_file(bucket_obj, key: str):
    """Deletes a file from S3."""
    try:
        print(f"Attempting to delete s3://{bucket_obj.name}/{key}")
        s3_object = bucket_obj.Object(key)
        s3_object.delete()
        # You can add a wait or verification step if needed, e.g., s3_object.wait_until_not_exists()
        print(f"Successfully deleted {key} from bucket {bucket_obj.name}")
    except ClientError as e:
        print(f"Error deleting file {key}: {e}")
        raise

if __name__ == "__main__":
    # # Example: Download a file
    file_to_download_key = 'partner_metrics/backend_events/cancelled_orders/2025-11-15/68b608ac_08:55:15.parquet.gz'
    local_download_path = 'temp/cancelled_orders_2025-11-15.parquet.gz' # Downloads to the script's directory
    download_s3_file(your_bucket, file_to_download_key, local_download_path)

    # # Example: Delete a file (BE VERY CAREFUL WITH THIS)
    # # Make sure this is the correct file you want to delete.
    # file_to_delete_key = 'partner_metrics/catalogs/warehouses/2025-09-03/9efa31ef_05:24:02.parquet.gz' # Change this to the actual file key you want to delete
    # delete_s3_file(your_bucket, file_to_delete_key)
    # print("Operations complete. Uncomment function calls to perform actions.")