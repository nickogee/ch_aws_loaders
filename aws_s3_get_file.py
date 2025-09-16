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

# def delete_s3_file(bucket_obj, key: str):
#     """Deletes a file from S3."""
#     try:
#         print(f"Attempting to delete s3://{bucket_obj.name}/{key}")
#         s3_object = bucket_obj.Object(key)
#         s3_object.delete()
#         # You can add a wait or verification step if needed, e.g., s3_object.wait_until_not_exists()
#         print(f"Successfully deleted {key} from bucket {bucket_obj.name}")
#     except ClientError as e:
#         print(f"Error deleting file {key}: {e}")
#         raise

if __name__ == "__main__":
    # Example: Download a file
    file_to_download_key = 'partner_metrics/interval_metrics/closed_session_funnel/2025-09-15/874f37e8_01:00:24.parquet.gz'
    local_download_path = 'temp/closed_session_funnel_2025-09-15.parquet.gz' # Downloads to the script's directory
    download_s3_file(your_bucket, file_to_download_key, local_download_path)

    # Example: Delete a file (BE VERY CAREFUL WITH THIS)
    # Make sure this is the correct file you want to delete.
    # file_to_delete_key = 'financial_metrics/transactions/2025-06-13/79189f454ed4a077_02:00:00.parquet' # Change this to the actual file key you want to delete
    # delete_s3_file(your_bucket, file_to_delete_key)
    print("Operations complete. Uncomment function calls to perform actions.")