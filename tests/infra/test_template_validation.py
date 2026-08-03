"""Infrastructure validation tests for template.yaml.

Validates SAM/CloudFormation template correctness, security settings,
and compliance with infrastructure requirements.

Requirements: 12.1, 12.2, 12.3, 12.4, 12.5, 12.6, 12.7
"""

import subprocess
from pathlib import Path

import pytest
import yaml


# ---------------------------------------------------------------------------
# Helpers: Load CloudFormation template with intrinsic function support
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEMPLATE_PATH = PROJECT_ROOT / "template.yaml"


def _cfn_constructor(loader, tag_suffix, node):
    """Handle CloudFormation intrinsic functions (!Ref, !Sub, !GetAtt, etc.)."""
    if isinstance(node, yaml.ScalarNode):
        return loader.construct_scalar(node)
    elif isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node)
    elif isinstance(node, yaml.MappingNode):
        return loader.construct_mapping(node)


yaml.add_multi_constructor("", _cfn_constructor, Loader=yaml.SafeLoader)


@pytest.fixture(scope="module")
def template():
    """Load and parse the CloudFormation template."""
    with open(TEMPLATE_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="module")
def resources(template):
    """Extract the Resources section from the template."""
    return template.get("Resources", {})


# ---------------------------------------------------------------------------
# Helpers: Resource discovery
# ---------------------------------------------------------------------------


def _find_resources_by_type(resources: dict, resource_type: str) -> dict:
    """Return a dict of logical_id -> properties for resources of a given type."""
    return {
        logical_id: resource.get("Properties", {})
        for logical_id, resource in resources.items()
        if resource.get("Type") == resource_type
    }


def _find_iam_policies(resources: dict) -> list:
    """Extract all IAM policy statements from IAM roles in the template."""
    statements = []
    roles = _find_resources_by_type(resources, "AWS::IAM::Role")
    for _role_id, props in roles.items():
        policies = props.get("Policies", [])
        for policy in policies:
            policy_doc = policy.get("PolicyDocument", {})
            for statement in policy_doc.get("Statement", []):
                statements.append(statement)
    return statements


# ---------------------------------------------------------------------------
# Test: SAM validate --lint (Requirement 12.1)
# ---------------------------------------------------------------------------


class TestSamValidate:
    """Validate SAM template using sam validate --lint."""

    def test_sam_validate_lint_passes(self):
        """sam validate --lint should pass without errors for template.yaml."""
        result = subprocess.run(
            ["sam", "validate", "--lint", "--template-file", str(TEMPLATE_PATH)],
            capture_output=True,
            text=True,
            cwd=str(PROJECT_ROOT),
        )
        assert result.returncode == 0, (
            f"sam validate --lint failed:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"
        )


# ---------------------------------------------------------------------------
# Test: cfn-lint (Requirement 12.2)
# ---------------------------------------------------------------------------


class TestCfnLint:
    """Validate CloudFormation template using cfn-lint."""

    def test_cfn_lint_passes(self):
        """cfn-lint should pass without errors for template.yaml."""
        result = subprocess.run(
            ["cfn-lint", str(TEMPLATE_PATH)],
            capture_output=True,
            text=True,
            cwd=str(PROJECT_ROOT),
        )
        assert result.returncode == 0, (
            f"cfn-lint failed:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"
        )


# ---------------------------------------------------------------------------
# Test: S3 PublicAccessBlockConfiguration (Requirement 12.3)
# ---------------------------------------------------------------------------


class TestS3PublicAccessBlock:
    """Verify S3 buckets have all 4 PublicAccessBlock settings set to true."""

    def test_all_s3_buckets_have_public_access_block(self, resources):
        """Every S3 bucket must define PublicAccessBlockConfiguration with all 4 settings true."""
        buckets = _find_resources_by_type(resources, "AWS::S3::Bucket")
        assert buckets, "No S3 bucket resources found in template"

        required_settings = [
            "BlockPublicAcls",
            "BlockPublicPolicy",
            "IgnorePublicAcls",
            "RestrictPublicBuckets",
        ]

        for logical_id, props in buckets.items():
            public_access_config = props.get("PublicAccessBlockConfiguration", {})
            assert public_access_config, (
                f"S3 bucket '{logical_id}' is missing PublicAccessBlockConfiguration"
            )

            for setting in required_settings:
                value = public_access_config.get(setting)
                assert value is True, (
                    f"S3 bucket '{logical_id}' has {setting}={value}, expected true"
                )


# ---------------------------------------------------------------------------
# Test: S3 and Firehose encryption (Requirement 12.4)
# ---------------------------------------------------------------------------


class TestEncryption:
    """Verify S3 buckets and Firehose streams have encryption enabled."""

    def test_s3_buckets_have_encryption(self, resources):
        """Every S3 bucket must have server-side encryption enabled."""
        buckets = _find_resources_by_type(resources, "AWS::S3::Bucket")
        assert buckets, "No S3 bucket resources found in template"

        for logical_id, props in buckets.items():
            encryption = props.get("BucketEncryption", {})
            assert encryption, (
                f"S3 bucket '{logical_id}' is missing BucketEncryption"
            )

            sse_rules = encryption.get("ServerSideEncryptionConfiguration", [])
            assert sse_rules, (
                f"S3 bucket '{logical_id}' has no ServerSideEncryptionConfiguration rules"
            )

            # Verify at least one rule has SSEAlgorithm defined
            has_sse = any(
                rule.get("ServerSideEncryptionByDefault", {}).get("SSEAlgorithm")
                for rule in sse_rules
            )
            assert has_sse, (
                f"S3 bucket '{logical_id}' does not define SSEAlgorithm in encryption config"
            )

    def test_firehose_streams_have_encryption(self, resources):
        """Every Firehose delivery stream must have encryption enabled."""
        streams = _find_resources_by_type(
            resources, "AWS::KinesisFirehose::DeliveryStream"
        )
        assert streams, "No Firehose delivery stream resources found in template"

        for logical_id, props in streams.items():
            encryption_config = props.get("DeliveryStreamEncryptionConfiguration", {})
            assert encryption_config, (
                f"Firehose stream '{logical_id}' is missing "
                "DeliveryStreamEncryptionConfiguration"
            )

            key_type = encryption_config.get("KeyType")
            assert key_type in ("AWS_OWNED_CMK", "CUSTOMER_MANAGED_CMK"), (
                f"Firehose stream '{logical_id}' has invalid KeyType='{key_type}', "
                "expected AWS_OWNED_CMK or CUSTOMER_MANAGED_CMK"
            )


# ---------------------------------------------------------------------------
# Test: S3 bucket policy denies non-TLS (Requirement 12.5)
# ---------------------------------------------------------------------------


class TestS3BucketPolicyTLS:
    """Verify S3 bucket policies deny non-TLS requests."""

    def test_bucket_policy_denies_non_tls(self, resources):
        """Every S3 bucket must have a policy denying requests where aws:SecureTransport is false."""
        bucket_policies = _find_resources_by_type(resources, "AWS::S3::BucketPolicy")
        assert bucket_policies, "No S3 BucketPolicy resources found in template"

        for logical_id, props in bucket_policies.items():
            policy_doc = props.get("PolicyDocument", {})
            statements = policy_doc.get("Statement", [])

            has_deny_insecure = False
            for statement in statements:
                effect = statement.get("Effect", "")
                condition = statement.get("Condition", {})
                bool_condition = condition.get("Bool", {})
                secure_transport = bool_condition.get("aws:SecureTransport")

                if effect == "Deny" and secure_transport == "false":
                    has_deny_insecure = True
                    break

            assert has_deny_insecure, (
                f"BucketPolicy '{logical_id}' does not have a Deny statement "
                "for aws:SecureTransport=false"
            )


# ---------------------------------------------------------------------------
# Test: No wildcard IAM actions or resources (Requirement 12.6)
# ---------------------------------------------------------------------------


class TestIAMNoWildcards:
    """Verify no IAM policy uses wildcard actions or resources with Allow effect."""

    def test_no_wildcard_actions_with_allow(self, resources):
        """No IAM policy statement should use wildcard actions (e.g., 's3:*', '*') with Allow."""
        statements = _find_iam_policies(resources)
        assert statements, "No IAM policy statements found in template"

        for statement in statements:
            effect = statement.get("Effect", "")
            if effect != "Allow":
                continue

            actions = statement.get("Action", [])
            if isinstance(actions, str):
                actions = [actions]

            for action in actions:
                # Check for full wildcard '*' or service wildcard like 's3:*'
                assert action != "*", (
                    f"IAM policy has wildcard Action '*' with Allow effect"
                )
                assert not action.endswith(":*"), (
                    f"IAM policy has wildcard Action '{action}' with Allow effect"
                )

    def test_no_wildcard_resources_with_allow(self, resources):
        """No IAM policy statement should use wildcard Resource ('*') with Allow effect."""
        statements = _find_iam_policies(resources)
        assert statements, "No IAM policy statements found in template"

        for statement in statements:
            effect = statement.get("Effect", "")
            if effect != "Allow":
                continue

            resource = statement.get("Resource", [])
            if isinstance(resource, str):
                resource = [resource]

            for res in resource:
                assert res != "*", (
                    f"IAM policy has wildcard Resource '*' with Allow effect"
                )


# ---------------------------------------------------------------------------
# Test: Firehose DataFormatConversionConfiguration (Requirement 12.7)
# ---------------------------------------------------------------------------


class TestFirehoseFormatConversion:
    """Verify Firehose has DataFormatConversionConfiguration enabled."""

    def test_firehose_data_format_conversion_enabled(self, resources):
        """Firehose delivery stream must have DataFormatConversionConfiguration Enabled=true."""
        streams = _find_resources_by_type(
            resources, "AWS::KinesisFirehose::DeliveryStream"
        )
        assert streams, "No Firehose delivery stream resources found in template"

        for logical_id, props in streams.items():
            # DataFormatConversionConfiguration can be nested under
            # ExtendedS3DestinationConfiguration
            s3_config = props.get("ExtendedS3DestinationConfiguration", {})
            conversion_config = s3_config.get("DataFormatConversionConfiguration", {})

            assert conversion_config, (
                f"Firehose stream '{logical_id}' is missing "
                "DataFormatConversionConfiguration in ExtendedS3DestinationConfiguration"
            )

            enabled = conversion_config.get("Enabled")
            assert enabled is True, (
                f"Firehose stream '{logical_id}' has "
                f"DataFormatConversionConfiguration.Enabled={enabled}, expected true"
            )
