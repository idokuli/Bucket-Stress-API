import boto3
from botocore.config import Config

class S3Service:
    def __init__(self, access_key, secret_key, region):
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=Config(signature_version='s3v4')
        )

    def list_files(self, bucket_name):
        res = self.s3.list_objects_v2(Bucket=bucket_name)
        return res, [obj['Key'] for obj in res.get('Contents', [])]

    def upload_file(self, bucket_name, file_obj, s3_key):
        return self.s3.upload_fileobj(Fileobj=file_obj, Bucket=bucket_name, Key=s3_key)

    def get_versioning_status(self, bucket_name):
        res = self.s3.get_bucket_versioning(Bucket=bucket_name)
        return res.get('Status', 'Disabled')

    def set_versioning(self, bucket_name, status):
        return self.s3.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={'Status': status}
        )

    def get_file_versions(self, bucket_name, filename):
        versions = self.s3.list_object_versions(Bucket=bucket_name, Prefix=filename)
        return [{
            'id': v['VersionId'],
            'last_modified': v['LastModified'],
            'size': round(v['Size'] / 1024, 2),
            'is_latest': v['IsLatest']
        } for v in versions.get('Versions', []) if v['Key'] == filename]

    def get_url(self, bucket_name, filename):
        clean_name = filename.split("/")[-1]
        disposition = f'attachment; filename="{clean_name}"'
        return self.s3.generate_presigned_url(
            'get_object', 
            Params={'Bucket': bucket_name, 'Key': filename, 'ResponseContentDisposition': disposition}, 
            ExpiresIn=3600
        )

    def delete_object(self, bucket_name, filename):
        return self.s3.delete_object(Bucket=bucket_name, Key=filename)

    def apply_lifecycle(self, bucket_name):
        return self.s3.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration={
                'Rules': [{
                    'ID': '30DayDelete',
                    'Status': 'Enabled',
                    'Filter': {'Prefix': ''},
                    'Expiration': {'Days': 30}
                }]
            }
        )

    def find_word_in_file(self, bucket_name, filename, word, case_sensitive=False):
        """
        Search for a word in a file stored in the bucket.
        Returns a list of matches with line numbers and content.
        """
        import io
        
        # Download file content
        response = self.s3.get_object(Bucket=bucket_name, Key=filename)
        content = response['Body'].read()
        
        # Try to decode as text
        try:
            text = content.decode('utf-8')
        except UnicodeDecodeError:
            try:
                text = content.decode('latin-1')
            except Exception:
                return {'error': 'File is not a text file or has unsupported encoding'}
        
        # Search for the word
        lines = text.splitlines()
        matches = []
        search_word = word if case_sensitive else word.lower()
        
        for line_num, line in enumerate(lines, 1):
            search_line = line if case_sensitive else line.lower()
            if search_word in search_line:
                matches.append({
                    'line_number': line_num,
                    'content': line.strip(),
                    'occurrences': search_line.count(search_word)
                })
        
        return {
            'filename': filename,
            'word': word,
            'total_matches': len(matches),
            'total_occurrences': sum(m['occurrences'] for m in matches),
            'matches': matches
        }