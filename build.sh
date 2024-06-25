docker build --platform linux/amd64 -t orderbot-sync .
docker tag orderbot-sync:latest ${AWS_ECR_REPO}/orderbot-sync:latest
docker push ${AWS_ECR_REPO}/orderbot-sync:latest

