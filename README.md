# S3-Compare

`S3-Compare` is a Python-based tool designed to compare the contents of two Amazon S3 buckets. It allows users to identify differences in files between a source bucket and a target bucket, based on several criteria such as file size, ETag values, and optionally, the actual file content via MD5 hash comparisons.

Used to compare a restored if a restored bucket is the same at the original backup.


## Features

- Compares the contents of two S3 buckets.
- Identifies extra files in the target bucket, missing files in the target bucket, and files with size or ETag mismatches.
- Allows different levels of comparison based on user preference.

## Comparison Options

1. **Compare Filename, Filesize, and ETag**: This is the default and fastest mode, comparing the filename, filesize, and ETag of objects in both buckets.
2. **Compare Filename and Filename**: A basic comparison that only checks for the presence of filenames in both buckets.
3. **Compare Filename and Calculate MD5 Hash for Each File**: This mode performs an MD5 hash comparison of the file contents for a thorough and accurate comparison. Note that this option is network-intensive as it requires downloading each file's content.

## Prerequisites

- Python 3.6 or higher.
- AWS credentials must be configured correctly to have access to the S3 buckets being used.

## Installation

Clone the repository and navigate into the project directory:

```bash
git clone <repository-url>
cd s3-compuare
```


## Usage

To run the tool, execute the following command in your terminal:

```bash
uv run main.py <source_bucket> <target_bucket> [options]
```

### Options

- `<source_bucket>`: Name of the source S3 bucket.
- `<target_bucket>`: Name of the target S3 bucket.
- `--source-profile`: (Optional) AWS profile to use for accessing the source bucket.
- `--target-profile`: (Optional) AWS profile to use for accessing the target bucket.
- `--ignore-etags`: Ignore ETag differences between files. This is useful for restored objects where ETags might not match.
- `--compare-hashes`: Performs MD5 hash comparisons of files to determine content differences. This process is slower but provides more accurate results.

### Example Command


#### Compare 1

```bash
uv run main.py my-source-bucket my-target-bucket 
```

#### Compare 2

```bash
uv run main.py my-source-bucket my-target-bucket --ignore-etags 
```


#### Compare 3

```bash
uv run main.py my-source-bucket my-target-bucket --ignore-etags --compare-hashes
```

## Output

The tool will log the results of the comparison, including:

- Total files processed.
- Matched files.
- Differences categorized under extra files in target, missing files in target, size mismatches, ETag mismatches, and content mismatches.

If there are no differences, a message indicating that the buckets are identical will be displayed.

