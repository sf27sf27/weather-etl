# AWS Lambda Layer Setup

Your Lambda function requires NumPy, Pandas, Requests, and psycopg2.

**Package size:** 7.87 MB (includes Lambda-compatible psycopg2 and all dependencies)
**Layer needed:** Only AWSSDKPandas (for NumPy, Pandas, Requests)

## Required Lambda Layer:

### AWSSDKPandas (for NumPy, Pandas, Requests)
Includes: NumPy, Pandas, Requests, and many other scientific packages

**Note:** psycopg2 is now included directly in your deployment package (Lambda-compatible version).

## Steps to Add Layer:

### Using AWS Console:
1. Go to your Lambda function in AWS Console
2. Scroll down to the "Layers" section
3. Click "Add a layer"
4. Select "AWS layers"
5. Choose: **AWSSDKPandas-Python310**
6. Select the latest version
7. Click "Add"

### Using AWS CLI (for us-east-2 / Ohio):
```bash
aws lambda update-function-configuration \
  --function-name YOUR_FUNCTION_NAME \
  --layers arn:aws:lambda:us-east-2:336392948345:layer:AWSSDKPandas-Python310:15
```

## Lambda Layer ARN for Your Region (us-east-2 / Ohio):

### AWSSDKPandas (NumPy, Pandas, Requests):
```
arn:aws:lambda:us-east-2:336392948345:layer:AWSSDKPandas-Python310:15
```
## Final Steps:

1. Upload the new `lambda_function.zip` (7.87 MB) to your Lambda function
2. Add the AWSSDKPandas layer using the ARN above
3. Test your function - it should now work!

**Total size:** ~8 MB package + AWSSDKPandas layer = well under Lambda's 250 MB limit.
