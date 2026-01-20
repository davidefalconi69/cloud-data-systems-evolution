"""
FinOps AutoStop Automation - AWS Lambda Function

This module implements an automated cost-optimization solution for AWS EC2 instances.
It analyzes CloudWatch metrics to identify idle resources and stops them based on
configurable thresholds, implementing FinOps best practices for cloud cost management.

Key Features:
    - Multi-dimensional idle detection (CPU, Network, Disk I/O)
    - ABAC (Attribute-Based Access Control) for security
    - Fail-open reliability pattern
    - Structured JSON logging for observability
    - Thread-based parallel processing for scalability
"""

import boto3
import os
import logging
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.config import Config
from botocore.exceptions import ClientError

# --- CONFIGURATION ---
# Configuration parameters are retrieved from environment variables to facilitate 
# deployment across distinct environments (e.g., Development vs. Production) 
# without necessitating codebase modifications.
REGION = os.environ.get('AWS_REGION', 'eu-central-1')
DRY_RUN = os.environ.get('DRY_RUN', 'false').lower() == 'true'
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN', '')

# Thresholds
# Note: Default thresholds are configured conservatively. The logic prioritizes 
# service availability, preferring to allow an idle instance to persist rather 
# than erroneously stopping a legitimately active one.
CPU_THRESHOLD = float(os.environ.get('CPU_THRESHOLD', 2.0)) 
NETWORK_BYTES_THRESHOLD = int(os.environ.get('NETWORK_BYTES_THRESHOLD', 5 * 1024 * 1024))
DISK_BYTES_THRESHOLD = int(os.environ.get('DISK_BYTES_THRESHOLD', 1 * 1024 * 1024))

# Time window settings
# LOOKBACK_MINUTES defines the temporal analysis window. Shorter windows increase 
# the risk of false positives due to transient idleness, whereas longer windows 
# delay cost realization.
LOOKBACK_MINUTES = int(os.environ.get('LOOKBACK_MINUTES', 60))
METRIC_DELAY_MINUTES = int(os.environ.get('METRIC_DELAY_MINUTES', 10)) # Buffer for CloudWatch ingestion latency

# Performance tuning
# AWS Lambda typically allocates CPU resources in proportion to memory (approx. 1 vCPU per 1.7GB). 
# Since the workload is I/O bound (waiting for API responses), a multithreaded execution model 
# is implemented to process multiple instances in parallel within the Lambda timeout constraints.
MAX_WORKERS = int(os.environ.get('MAX_WORKERS', 10))

# --- LOGGING ---
# A JSON formatter is utilized to structure log output, enabling automated ingestion 
# and analysis by log aggregation systems (e.g., CloudWatch Insights or Splunk).
class JsonFormatter(logging.Formatter):
    """
    JSON Formatter that automatically includes all 'extra' attributes passed
    to the logger, ensuring no data is lost when using LoggerAdapters.
    """
    def format(self, record):
        log_obj = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        
        # Standard LogRecord attributes to exclude from the custom output
        reserved = {
            'args', 'asctime', 'created', 'exc_info', 'exc_text', 'filename',
            'funcName', 'levelname', 'levelno', 'lineno', 'module', 'message',
            'msg', 'name', 'pathname', 'process', 'processName', 'relativeCreated',
            'stack_info', 'thread', 'threadName', 'msecs'
        }
        
        # Automatically include all other attributes (e.g., aws_request_id, summary, instance_id)
        for key, value in record.__dict__.items():
            if key not in reserved and not key.startswith('_'):
                # Extract dictionary contents if using our 'extra_tags' convention, 
                # or just add the key directly.
                if key == 'extra_tags' and isinstance(value, dict):
                    log_obj.update(value)
                else:
                    log_obj[key] = value
                    
        return json.dumps(log_obj)

logger = logging.getLogger("FinOpsAutoStop")
# Dynamic log level from environment variable (default: INFO)
log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
logger.setLevel(getattr(logging, log_level, logging.INFO))
# Handlers are reset to prevent duplicate log entries during Lambda execution environment reuse.
if logger.hasHandlers():
    logger.handlers.clear()
handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logger.addHandler(handler)

# --- CLIENTS ---
# The standard retry mode is configured to automatically manage basic throttling exceptions (HTTP 429).
# Thread Safety Note: max_pool_connections is set to MAX_WORKERS + 2 because botocore's default
# urllib3 connection pool size (10) is smaller than our thread pool. Without this, worker threads
# would block waiting for HTTP connections, creating a bottleneck that negates parallelization.
aws_config = Config(
    region_name=REGION,
    retries={'max_attempts': 3, 'mode': 'standard'},
    max_pool_connections=MAX_WORKERS + 2,
    # Explicit timeouts to prevent Lambda timeout exhaustion under high latency conditions
    read_timeout=30,
    connect_timeout=5
)
ec2 = boto3.client('ec2', config=aws_config)
cw = boto3.client('cloudwatch', config=aws_config)
sns = boto3.client('sns', config=aws_config)

def get_aggregated_metrics(instance_id: str) -> Dict[str, float]:
    """
    Retrieves critical resource utilization metrics from CloudWatch.

    Queries multiple metric dimensions (CPU, Network I/O, Disk I/O) to build
    a comprehensive activity profile for idle detection.

    Args:
        instance_id: The EC2 instance ID to query metrics for.

    Returns:
        Dict[str, float]: A dictionary containing:
            - 'cpu': Maximum CPU utilization percentage
            - 'net_in', 'net_out': Sum of network bytes
            - 'disk_read', 'disk_write': Sum of instance-store disk bytes
            - 'ebs_read', 'ebs_write': Sum of EBS volume bytes
        Returns an empty dict on API failure (fail-open strategy).

    Note:
        Empty return triggers SKIP_METRIC_ERROR in process_instance,
        implementing the fail-open reliability pattern.
    """
    now = datetime.now(timezone.utc)
    end_time = now - timedelta(minutes=METRIC_DELAY_MINUTES)
    start_time = end_time - timedelta(minutes=LOOKBACK_MINUTES)

    # Multiple metric dimensions are queried to distinguish between true idle states 
    # and "processing" nodes (e.g., high I/O with low CPU).
    # Note: For larger scale deployments (>10k instances), migrating to BatchGetMetricData 
    # would reduce API call volume; however, per-instance querying is sufficient for this scope.
    metric_queries = [
        {"Id": "cpu", "MetricStat": {"Metric": {"Namespace": "AWS/EC2", "MetricName": "CPUUtilization", "Dimensions": [{"Name": "InstanceId", "Value": instance_id}]}, "Period": 300, "Stat": "Maximum"}},
        {"Id": "net_in", "MetricStat": {"Metric": {"Namespace": "AWS/EC2", "MetricName": "NetworkIn", "Dimensions": [{"Name": "InstanceId", "Value": instance_id}]}, "Period": 300, "Stat": "Sum"}},
        {"Id": "net_out", "MetricStat": {"Metric": {"Namespace": "AWS/EC2", "MetricName": "NetworkOut", "Dimensions": [{"Name": "InstanceId", "Value": instance_id}]}, "Period": 300, "Stat": "Sum"}},
        {"Id": "disk_read", "MetricStat": {"Metric": {"Namespace": "AWS/EC2", "MetricName": "DiskReadBytes", "Dimensions": [{"Name": "InstanceId", "Value": instance_id}]}, "Period": 300, "Stat": "Sum"}},
        {"Id": "disk_write", "MetricStat": {"Metric": {"Namespace": "AWS/EC2", "MetricName": "DiskWriteBytes", "Dimensions": [{"Name": "InstanceId", "Value": instance_id}]}, "Period": 300, "Stat": "Sum"}},
        # EBS metrics are included as certain modern instance generations report these independently.
        {"Id": "ebs_read", "MetricStat": {"Metric": {"Namespace": "AWS/EC2", "MetricName": "EBSReadBytes", "Dimensions": [{"Name": "InstanceId", "Value": instance_id}]}, "Period": 300, "Stat": "Sum"}},
        {"Id": "ebs_write", "MetricStat": {"Metric": {"Namespace": "AWS/EC2", "MetricName": "EBSWriteBytes", "Dimensions": [{"Name": "InstanceId", "Value": instance_id}]}, "Period": 300, "Stat": "Sum"}}
    ]

    try:
        response = cw.get_metric_data(
            MetricDataQueries=metric_queries,
            StartTime=start_time,
            EndTime=end_time
        )
    except ClientError as e:
        logger.error(f"Failed to fetch metrics: {e}", extra={"extra_tags": {"instance_id": instance_id}})
        return {}

    # Legacy disk metrics (DiskReadBytes, DiskWriteBytes) are only available for instances
    # with instance-store volumes. Modern EBS-only instances (t3, m5, etc.) don't report these.
    # We treat them as optional to avoid false SKIP_METRIC_ERROR results.
    optional_metrics = {'disk_read', 'disk_write'}
    # Critical metrics that must exist for accurate idle detection
    critical_metrics = {'cpu', 'net_in', 'net_out', 'ebs_read', 'ebs_write'}
    
    results = {}
    for res in response.get('MetricDataResults', []):
        metric_id = res['Id']
        
        if not res.get('Values'):
            if metric_id in optional_metrics:
                # Optional metrics: assume zero activity if unavailable (EBS-only instances)
                logger.debug(f"Optional metric {metric_id} unavailable (likely EBS-only instance)",
                             extra={"extra_tags": {"instance_id": instance_id}})
                results[metric_id] = 0
                continue
            
            # Critical metric missing: fail-open strategy - assume instance is active
            logger.warning(f"Critical metric {metric_id} missing. Aborting for safety (fail-open).", 
                           extra={"extra_tags": {"instance_id": instance_id}})
            return {} # Empty return triggers "SKIP_METRIC_ERROR" in process_instance
        
        if metric_id == 'cpu':
            results[metric_id] = max(res['Values'])
        else:
            results[metric_id] = sum(res['Values'])
    return results

def notify_action(instance_id: str, instance_name: str, owner: str, reason: str, action_type: str) -> None:
    """
    Publishes an alert to SNS for audit purposes and visibility.

    Args:
        instance_id: The EC2 instance ID.
        instance_name: Human-readable name from the 'Name' tag.
        owner: Owner identifier from the 'Owner' tag.
        reason: Detailed explanation of why the action was taken.
        action_type: The action performed (e.g., 'STOPPED', 'SIMULATION_STOP').
    """
    if not SNS_TOPIC_ARN:
        logger.warning("SNS_TOPIC_ARN not configured - notification skipped",
                       extra={"extra_tags": {"instance_id": instance_id, "action": action_type}})
        return
    
    msg = {
        "source": "FinOps-AutoStop",
        "action": action_type,
        "instance_id": instance_id,
        "instance_name": instance_name,
        "owner": owner,
        "reason": reason,
        "region": REGION,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    
    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=json.dumps(msg, indent=2),
            Subject=f"FinOps: {action_type} - {instance_name}"
        )
    except Exception as e:
        logger.error(f"SNS Publish failed: {e}")

def process_instance(instance: Dict[str, Any], request_id: str = "N/A") -> str:
    """
    Evaluates a single instance.
    Returns the action taken (e.g., 'STOPPED', 'SKIP_SPOT', 'ACTIVE').
    
    Implements Fail-Open strategy: defaults to ACTIVE state on metric failure.
    """
    inst_id = instance['InstanceId']
    tags = {t['Key']: t['Value'] for t in instance.get('Tags', [])}
    name = tags.get('Name', inst_id)
    owner = tags.get('Owner', 'Unknown') 
    
    # Context for all logs in this thread
    ctx = {"aws_request_id": request_id, "instance_id": inst_id}

    # --- GUARDRAILS ---
    # 1. Spot Instances: Spot instances are excluded as they cannot be stopped, only terminated.
    if instance.get('InstanceLifecycle') == 'spot':
        logger.info("Skipping spot instance", extra={"extra_tags": ctx})
        return "SKIP_SPOT"

    # 2. ASG Members: Instances managed by AutoScaling groups are excluded,
    # as manual stops cause the group to provision replacement instances.
    if 'aws:autoscaling:groupName' in tags:
        logger.info("Skipping ASG member", extra={"extra_tags": ctx})
        return "SKIP_ASG"

    # 3. Manual Override: A "Break glass" mechanism is provided for critical tasks.
    if tags.get('KeepAlive', '').lower() == 'true':
        logger.info("Skipping instance (KeepAlive override)", extra={"extra_tags": ctx})
        return "SKIP_OVERRIDE"

    # 4. Warm-up Period: Instances launched very recently are protected 
    # to allow for initialization. Reduced to 5 minutes for thesis testing purposes.
    launch_time = instance['LaunchTime']
    if (datetime.now(timezone.utc) - launch_time).total_seconds() < 300: # 5 minutes
        logger.info("Instance in warm-up period", extra={"extra_tags": {**ctx, "launch_time": launch_time.isoformat()}})
        return "SKIP_RECENTLY_LAUNCHED"

    # --- METRIC ANALYSIS ---
    metrics = get_aggregated_metrics(inst_id)
    if not metrics:
        # This often occurs for very new instances (CloudWatch delay) or 
        # during AWS service interruptions.
        logger.info("Skipping instance (Metrics unavailable)", 
                    extra={"extra_tags": {**ctx, "note": "Likely CloudWatch ingestion delay"}})
        return "SKIP_METRIC_ERROR"

    cpu_max = metrics.get('cpu', 100.0)
    # Note: If individual network metrics are missing but CPU exists, we default to 0.
    # This is a conscious trade-off: we prioritize stopping truly idle instances over
    # edge cases where network-only activity might occur without CPU usage.
    net_total = metrics.get('net_in', 0) + metrics.get('net_out', 0)
    disk_total = (metrics.get('disk_read', 0) + metrics.get('disk_write', 0) + 
                  metrics.get('ebs_read', 0) + metrics.get('ebs_write', 0))

    is_idle = (cpu_max < CPU_THRESHOLD) and \
              (net_total < NETWORK_BYTES_THRESHOLD) and \
              (disk_total < DISK_BYTES_THRESHOLD)

    if is_idle:
        reason = (f"Idle detected ({LOOKBACK_MINUTES}m avg): CPU Max {cpu_max:.2f}%, "
                  f"Net {net_total/1024/1024:.1f}MB, Disk {disk_total/1024/1024:.1f}MB")
        
        logger.info("Instance identified as idle", extra={"extra_tags": {**ctx, "reason": reason}})

        if DRY_RUN:
            notify_action(inst_id, name, owner, reason, "SIMULATION_STOP")
            return "DRY_RUN_STOP"
        
        try:
            # Audit Trail: The resource is tagged prior to stopping to enable forensic analysis.
            ec2.create_tags(
                Resources=[inst_id],
                Tags=[
                    {'Key': 'FinOps_LastAction', 'Value': 'AutoStop'},
                    {'Key': 'FinOps_StopReason', 'Value': reason[:250]}, 
                    {'Key': 'FinOps_Date', 'Value': datetime.now(timezone.utc).strftime('%Y-%m-%d')}
                ]
            )
            
            ec2.stop_instances(InstanceIds=[inst_id])
            notify_action(inst_id, name, owner, reason, "STOPPED")
            return "STOPPED"
            
        except ClientError as e:
            logger.error(f"Stop command failed: {e}", extra={"extra_tags": ctx})
            return "ERROR_STOPPING"

    # Log metrics even when active to help with threshold tuning
    metrics_summary = {
        "cpu_max": f"{cpu_max:.2f}%",
        "net_mb": f"{net_total/1024/1024:.2f}",
        "disk_mb": f"{disk_total/1024/1024:.2f}"
    }
    logger.info("Instance is active (Not idle)", extra={"extra_tags": {**ctx, "metrics": metrics_summary}})
    return "ACTIVE"

def lambda_handler(event, context):
    """
    Entry point for the Lambda function.
    """
    # The request ID is injected into the logger to facilitate distributed tracing.
    log_adapter = logging.LoggerAdapter(logger, {'aws_request_id': context.aws_request_id})
    
    log_adapter.info("Starting FinOps Idle Check", extra={"extra_tags": {
        "region": REGION, 
        "dry_run": DRY_RUN,
        "threshold_cpu": CPU_THRESHOLD,
        "lookback_window": LOOKBACK_MINUTES
    }})

    # Discovery: Server-side filtering is employed to conserve bandwidth.
    # The search is strictly limited to instances tagged with AutoStop=true.
    paginator = ec2.get_paginator('describe_instances')
    iterator = paginator.paginate(
        Filters=[
            {'Name': 'instance-state-name', 'Values': ['running']},
            {'Name': 'tag:AutoStop', 'Values': ['true']}
        ]
    )

    candidates = []
    for page in iterator:
        for r in page['Reservations']:
            candidates.extend(r['Instances'])

    if not candidates:
        log_adapter.info("No candidates found (No running instances with tag AutoStop=true).")
        return {"status": "skipped", "count": 0}

    log_adapter.info(f"Processing {len(candidates)} candidates.")

    stats = {}
    # ThreadPoolExecutor is utilized for efficiency. As API calls are I/O bound,
    # sequential processing incurs a risk of exceeding the Lambda timeout (15m) at scale.
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_instance, inst, context.aws_request_id): inst['InstanceId'] for inst in candidates}
        
        for future in as_completed(futures):
            inst_id = futures[future]
            try:
                result = future.result()
                stats[result] = stats.get(result, 0) + 1
            except Exception as e:
                logger.error("Worker thread exception", extra={"extra_tags": {"instance_id": inst_id, "error": str(e)}})
                stats['ERROR'] = stats.get('ERROR', 0) + 1

    stats['mode'] = "DRY_RUN" if DRY_RUN else "LIVE"
    log_adapter.info("Execution complete", extra={"extra_tags": {"summary": stats}})
    
    return {"status": "success", "summary": stats}