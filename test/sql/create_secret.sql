SELECT mooncake.create_secret('mooncake-test-secret', 'S3', 'ASDfExaMPLEKeyID', 'ASDfExaMPLESECRET', '{"ENDPOINT":"s3.us-east-1.amazonaws.com", "REGION":"us-east-1"}');
TRUNCATE TABLE mooncake.secrets;

SELECT mooncake.create_secret('mooncake-test-secret', 'S3', 'ASDfExaMPLEKeyID', 'ASDfExaMPLESECRET', '{"ENDPOINT":"s3.us-east-1.amazonaws.com", "REGION":"us-east-1", "SCOPE":"s3://mooncakebucket/folder"}');
TRUNCATE TABLE mooncake.secrets;

SELECT mooncake.create_secret('mooncake-test-secret', 'S3', 'ASDfExaMPLEKeyID', 'ASDfExaMPLESECRET', '{"ENDPOINT":"s3.us-east-1.amazonaws.com", "REGION":"us-east-1", "USE_SSL":"FALSE"}');
TRUNCATE TABLE mooncake.secrets;

SELECT mooncake.create_secret('mooncake-test-secret', 'S3', 'ASDfExaMPLEKeyID', 'ASDfExaMPLESECRET', '{"ENDPOINT":"s3.us-east-1.amazonaws.com", "REGION":"us-east-1", "URL_STYLE":"path"}');
TRUNCATE TABLE mooncake.secrets;

SELECT mooncake.create_secret('mooncake-test-secret', 'S3', 'ASDfExaMPLEKeyID', 'ASDfExaMPLESECRET', '{"ENDPOINT":"s3express-use1-az6.us-east-1.amazonaws.com", "REGION":"us-east-1"}');
TRUNCATE TABLE mooncake.secrets;

SELECT mooncake.create_secret('mooncake-test-secret', 'S3', 'ASDfExaMPLEKeyID', 'ASDfExaMPLESECRET', '{"ENDPOINT":"s3.us-east-1.amazonaws.com", "REGION":"us-east-1", "URL":"path"}');
TRUNCATE TABLE mooncake.secrets;

SELECT mooncake.create_secret('mooncake-test-secret', 'S3', 'ASDfExaMPLEKeyID', 'ASDfExaMPLESECRET', '{"ENDPOINT":"https://s3.us-east-1.amazonaws.com", "REGION":"us-east-1"}');
TRUNCATE TABLE mooncake.secrets;

SELECT mooncake.create_secret('mooncake-test-secret', 'S3', 'ASDfExaMPLEKeyID', 'ASDfExaMPLESECRET', '{"ENDPOINT":"s3.us-east-1.amazonaws.com", "REGION":"us-east-1", "URL_STYLE":"xpath"}');
TRUNCATE TABLE mooncake.secrets;

SELECT mooncake.create_secret('mooncake-test-secret', 'XYZ', 'ASDfExaMPLEKeyID', 'ASDfExaMPLESECRET', '{"ENDPOINT":"s3.us-east-1.amazonaws.com", "REGION":"us-east-1"}');
TRUNCATE TABLE mooncake.secrets;
