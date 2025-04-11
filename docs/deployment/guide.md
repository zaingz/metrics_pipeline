# Deployment Guide

This guide provides instructions for deploying the Metrics Pipeline in various environments.

## Local Development Deployment

### Prerequisites

- Docker and Docker Compose
- Python 3.8+
- Git

### Steps

1. Clone the repository:
   ```bash
   git clone https://github.com/zaingz/metrics-pipeline.git
   cd metrics-pipeline
   ```

2. Install dependencies:
   ```bash
   pip install -e ".[dev]"
   ```

3. Start the local development environment:
   ```bash
   make dev
   ```

   This will start the following services:
   - LocalStack (for AWS services simulation)
   - ClickHouse (for metrics storage)
   - Metabase (for visualization)
   - API service (for metrics ingestion)
   - Worker service (for processing metrics)

4. Initialize the local environment:
   ```bash
   make init-local
   ```

5. Access the services:
   - Metrics API: http://localhost:8000
   - ClickHouse HTTP interface: http://localhost:8123
   - Metabase: http://localhost:3000

## AWS Deployment

### Prerequisites

- AWS account with appropriate permissions
- Pulumi CLI
- AWS CLI configured with credentials
- Python 3.8+

### Steps

1. Clone the repository:
   ```bash
   git clone https://github.com/zaingz/metrics-pipeline.git
   cd metrics-pipeline
   ```

2. Install dependencies:
   ```bash
   pip install -e ".[dev]"
   ```

3. Configure Pulumi:
   ```bash
   cd deployment/aws
   pulumi stack init dev
   ```

4. Configure deployment parameters:
   ```bash
   pulumi config set aws:region us-east-1
   pulumi config set metrics-pipeline:environment dev
   pulumi config set metrics-pipeline:instance_type t3.medium
   ```

5. Deploy the infrastructure:
   ```bash
   pulumi up
   ```

   This will deploy:
   - API Gateway for metrics ingestion
   - SQS queue for buffering metrics
   - EC2 instance with ClickHouse and Metabase
   - IAM roles and security groups

6. Access the deployed services:
   - The API Gateway URL will be displayed in the Pulumi output
   - The EC2 instance public IP will be displayed in the Pulumi output
   - Metabase will be available at http://<ec2-public-ip>:3000
   - ClickHouse HTTP interface will be available at http://<ec2-public-ip>:8123

## Docker Deployment

### Prerequisites

- Docker and Docker Compose
- Docker registry access (optional, for production deployment)

### Steps

1. Clone the repository:
   ```bash
   git clone https://github.com/zaingz/metrics-pipeline.git
   cd metrics-pipeline
   ```

2. Build the Docker images:
   ```bash
   docker-compose build
   ```

3. Start the services:
   ```bash
   docker-compose up -d
   ```

4. Access the services:
   - Metrics API: http://localhost:8000
   - ClickHouse HTTP interface: http://localhost:8123
   - Metabase: http://localhost:3000

## Production Considerations

### Security

1. **API Authentication**: Implement API Gateway authentication for the metrics ingestion endpoint
2. **Database Security**: Use strong passwords and restrict network access to ClickHouse
3. **Metabase Security**: Configure Metabase with proper authentication and user permissions
4. **Encryption**: Enable encryption in transit (HTTPS) and at rest

### Scaling

1. **Horizontal Scaling**: Deploy multiple worker instances for processing metrics
2. **Queue Scaling**: Configure SQS with appropriate throughput settings
3. **Database Scaling**: Consider ClickHouse cluster deployment for high-volume metrics
4. **Load Balancing**: Use load balancers for API endpoints

### Monitoring

1. **Health Checks**: Implement regular health checks for all components
2. **Alerting**: Set up alerts for service disruptions
3. **Logging**: Configure centralized logging
4. **Metrics**: Monitor the metrics pipeline itself with CloudWatch or other monitoring tools

### Backup and Recovery

1. **ClickHouse Backups**: Configure regular backups of ClickHouse data
2. **Metabase Backups**: Back up Metabase configuration and dashboards
3. **Disaster Recovery**: Implement a disaster recovery plan

## Troubleshooting

### Common Issues

1. **Connection Issues**:
   - Check network connectivity between components
   - Verify security group settings in AWS
   - Check that all services are running

2. **Data Ingestion Issues**:
   - Verify API Gateway configuration
   - Check SQS queue permissions
   - Inspect worker logs for processing errors

3. **Visualization Issues**:
   - Verify Metabase connection to ClickHouse
   - Check that data is being properly stored in ClickHouse
   - Inspect Metabase logs for errors

### Logs

- API service logs: `/var/log/metrics-api.log`
- Worker service logs: `/var/log/metrics-worker.log`
- ClickHouse logs: `/var/log/clickhouse-server/clickhouse-server.log`
- Metabase logs: Available in the Metabase Admin interface

### Support

For additional support:
- Check the [GitHub issues](https://github.com/zaingz/metrics-pipeline/issues)
- Join the community discussion on [Discord/Slack]
- Contact the maintainers at [email]
