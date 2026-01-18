#!/bin/bash
# Script to package Lambda function for deployment

set -e

echo "Creating Lambda deployment package..."

# Clean up any existing package
rm -rf lambda_package
rm -f lambda_function.zip

# Create package directory
mkdir -p lambda_package

# Install dependencies into package directory
pip install -r requirements.txt -t lambda_package/ --quiet

# Copy Lambda function
cp lambda_function.py lambda_package/

# Create zip file
cd lambda_package
zip -r ../lambda_function.zip . -x "*.pyc" -x "__pycache__/*" -x "*.dist-info/*"
cd ..

echo "Deployment package created: lambda_function.zip"
echo ""
echo "Next steps:"
echo "1. Upload lambda_function.zip to AWS Lambda"
echo "2. Set environment variables in Lambda configuration:"
echo "   - DB_HOST"
echo "   - DB_PORT"
echo "   - DB_NAME"
echo "   - DB_USER"
echo "   - DB_PASSWORD"
echo "3. Create a CloudWatch Events rule to trigger every 5 minutes:"
echo "   Schedule expression: rate(5 minutes)"
echo "4. Ensure Lambda has VPC access if your PostgreSQL is in a VPC"
echo "5. Set Lambda timeout to at least 30 seconds"
echo "6. Set Lambda memory to at least 256MB"
