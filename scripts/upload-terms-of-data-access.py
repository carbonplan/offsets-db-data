"""Upload TERMS_OF_DATA_ACCESS from GitHub to S3."""

import urllib.request

import s3fs

GITHUB_RAW_URL = (
    'https://raw.githubusercontent.com/carbonplan/offsets-db-data/main/TERMS_OF_DATA_ACCESS'
)
S3_DEST = 's3://carbonplan-offsets-db/TERMS_OF_DATA_ACCESS.txt'


def main():
    print(f'Fetching {GITHUB_RAW_URL}')
    with urllib.request.urlopen(GITHUB_RAW_URL) as response:  # noqa: S310
        content = response.read()

    print(f'Uploading to {S3_DEST}')
    fs = s3fs.S3FileSystem()
    with fs.open(S3_DEST, 'wb') as f:
        f.write(content)

    print('Done.')


if __name__ == '__main__':
    main()
