import boto3
from collections import defaultdict
import argparse
import sys
from typing import Dict, Set, Tuple
import logging
import hashlib

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class S3BucketComparer:
    def __init__(self, source_bucket: str, target_bucket: str, source_profile: str = None, target_profile: str = None, 
             ignore_etags: bool = False, compare_hashes: bool = False):
        """
        Initialize the S3 bucket comparer with source and target bucket names and optional AWS profiles.
        
        Args:
            source_bucket (str): Name of the source bucket
            target_bucket (str): Name of the target bucket
            source_profile (str, optional): AWS profile for source bucket
            target_profile (str, optional): AWS profile for target bucket
        """
        self.source_bucket = source_bucket
        self.target_bucket = target_bucket
        
        # Create S3 clients with appropriate profiles
        self.source_s3 = boto3.Session(profile_name=source_profile).client('s3') if source_profile else boto3.client('s3')
        self.target_s3 = boto3.Session(profile_name=target_profile).client('s3') if target_profile else boto3.client('s3')
        self.ignore_etags = ignore_etags
        self.compare_hashes = compare_hashes

    def get_bucket_objects(self, s3_client, bucket_name: str) -> Dict[str, Dict]:
        """
        Get all objects from a bucket with their metadata.
        
        Returns:
            Dict[str, Dict]: Dictionary with keys as object keys and values as object metadata
        """
        objects = {}
        paginator = s3_client.get_paginator('list_objects_v2')
        
        try:
            for page in paginator.paginate(Bucket=bucket_name):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        objects[obj['Key']] = {
                            'Size': obj['Size'],
                            'ETag': obj['ETag'],
                            'LastModified': obj['LastModified']
                        }
        except Exception as e:
            logger.error(f"Error accessing bucket {bucket_name}: {str(e)}")
            raise
            
        return objects

    def _calculate_md5(self, s3_client, bucket: str, key: str) -> str:
        """
        Calculate MD5 hash of a file in S3 by downloading and processing it in chunks.
        """
        try:
            response = s3_client.get_object(Bucket=bucket, Key=key)
            hash_md5 = hashlib.md5()
            
            # Read and update hash in chunks of 4K
            for chunk in iter(lambda: response['Body'].read(4096), b''):
                hash_md5.update(chunk)
                
            return hash_md5.hexdigest()
        except Exception as e:
            logger.error(f"Error calculating hash for {key}: {str(e)}")
            return None

    def _compare_file_hashes(self, key: str) -> bool:
        """
        Compare MD5 hashes of files from both buckets.
        """
        source_hash = self._calculate_md5(self.source_s3, self.source_bucket, key)
        target_hash = self._calculate_md5(self.target_s3, self.target_bucket, key)
        
        logger.info("  - Comparing hashes for file: %s  %s:%s  ", key, source_hash, target_hash)
        
        if source_hash is None or target_hash is None:
            return False
            
        return source_hash == target_hash

    def compare_buckets(self) -> Tuple[Dict[str, Set[str]], Dict[str, Set[str]]]:
        """
        Compare the contents of source and target buckets.
        
        Returns:
            Tuple containing:
            - Dict of differences (missing files, size mismatches, ETag mismatches)
            - Dict of statistics (total files, matched files, etc.)
        """
        logger.info(f"Comparing buckets: {self.source_bucket} -> {self.target_bucket}")
        
        # Get objects from both buckets
        source_objects = self.get_bucket_objects(self.source_s3, self.source_bucket)
        target_objects = self.get_bucket_objects(self.target_s3, self.target_bucket)
        
        differences = defaultdict(set)
        stats = defaultdict(int)
        
        # Compare objects
        all_keys = set(source_objects.keys()) | set(target_objects.keys())
        stats['total_files'] = len(all_keys)
        stats['matched_files'] = 0 
        
        for key in all_keys:
            if key not in source_objects:
                differences['extra_in_target'].add(key)
                continue
                
            if key not in target_objects:
                differences['missing_in_target'].add(key)
                continue
            
            source_obj = source_objects[key]
            target_obj = target_objects[key]
            
            if source_obj['Size'] != target_obj['Size']:
                differences['size_mismatch'].add(key)
            elif not self.ignore_etags and source_obj['ETag'] != target_obj['ETag']:
                differences['etag_mismatch'].add(key)
            else:
                if self.compare_hashes and not self._compare_file_hashes(key):
                    differences['content_mismatch'].add(key)
                else:
                    stats['matched_files'] += 1
        
        return dict(differences), dict(stats)

def main():
    parser = argparse.ArgumentParser(description='Compare two S3 buckets')
    parser.add_argument('source_bucket', help='Source bucket name')
    parser.add_argument('target_bucket', help='Target bucket name')
    parser.add_argument('--source-profile', help='AWS profile for source bucket')
    parser.add_argument('--target-profile', help='AWS profile for target bucket')
    parser.add_argument('--ignore-etags', action='store_true', help='Ignore ETag differences (useful for restored objects)')
    parser.add_argument('--compare-hashes', action='store_true', help='Compare actual file contents using MD5 hashes (slower but more accurate)')
    
    args = parser.parse_args()
    
    try:
        comparer = S3BucketComparer(
            args.source_bucket,
            args.target_bucket,
            args.source_profile,
            args.target_profile,
            args.ignore_etags,
            args.compare_hashes
        )
        
        differences, stats = comparer.compare_buckets()
        
        # Print results
        logger.info("\nComparison Results:")
        logger.info(f"Total files processed: {stats['total_files']}")
        logger.info(f"Matched files: {stats['matched_files']}")
        
        if not any(differences.values()):
            logger.info("\nBuckets are identical! ✅")
            sys.exit(0)
            
        logger.info("\nDifferences found:")
        for diff_type, keys in differences.items():
            if keys:
                logger.info(f"\n{diff_type.replace('_', ' ').title()}:")
                for key in sorted(keys):
                    logger.info(f"  - {key}")
        
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()