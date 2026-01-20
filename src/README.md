# AWS FinOps AutoStop Automation

## Overview
This project implements a **FinOps cost-optimization tool** designed to automatically stop idle EC2 instances in development and test environments. By leveraging AWS Serverless technologies, it ensures that you only pay for compute resources when they are actually being used, strictly following the **FinOps principles**.

## Architecture
The solution is built using a **Serverless** architecture to minimize maintenance and operational costs:

*   **AWS Lambda**: Executes the core logic. It is stateless and triggered periodically, making it far more cost-efficient than a dedicated tracking server.
*   **Amazon EventBridge**: Acts as the scheduler, triggering the Lambda function (default: every hour) to perform idle checks.
*   **Tagging Strategy (Source of Truth)**: The system relies entirely on EC2 tags. It does not use an external database (like DynamoDB), simplifying the architecture.
    *   **Logic**: The automation *only* targets instances tagged with `AutoStop: true`.

## Deployment

Follow these exact steps to deploy and test the solution:

### 1. Infrastructure Setup (EC2)
*   Launch an EC2 instance (e.g., `t3.micro` for Free Tier).
*   **Crucial Step**: Add the following Tag to the instance:
    *   **Key**: `AutoStop`
    *   **Value**: `true`
*   *Note: Without this tag, the automation allows the instance to run indefinitely (Safety First).*

### 2. Deploy Code (AWS SAM)
1.  Navigate to the source directory: `cd src`
2.  Build the application:
    ```bash
    sam build
    ```
3.  Deploy to AWS:
    ```bash
    sam deploy --guided
    ```
    Follow the interactive prompts to define your stack name and parameter overrides.

### 3. Activate Notifications
*   Check your email inbox immediately after deployment.
*   Look for an email from "AWS Notifications".
*   Click the **Confirm subscription** link. *If you skip this, you will not receive alerts.*

### 4. Verification
*   Wait approx. 15 minutes for CloudWatch metrics to stabilize.
*   The Lambda runs automatically based on the schedule (default: hourly).
*   To test immediately, go to the AWS Lambda Console -> Test tab -> Click "Test".

## Security & Compliance
Security is a core component of this thesis project, featuring a "Security-First" design:

### Attribute-Based Access Control (ABAC)
The Lambda function does **not** have blanket permission to stop all instances. It uses a strict IAM Condition:
```json
"Condition": { "StringEquals": { "ec2:ResourceTag/AutoStop": "true" } }
```
This ensures that even if the Lambda's credentials were compromised, an attacker **cannot stop critical production workloads** (like databases) unless they are explicitly tagged.

### Privilege Escalation Prevention
The function is also restricted from adding the `AutoStop` tag to untagged resources. It can only modify tags on instances that *already* have the tag, preventing it from "tagging to target" arbitrary resources.

### Fail-Open Strategy
Reliability is prioritized over cost in failure scenarios. If CloudWatch metrics are unavailable or the API fails, the system defaults to assuming the instance is **busy** (CPU = 100%), ensuring no false-positive stops occur during outages.
