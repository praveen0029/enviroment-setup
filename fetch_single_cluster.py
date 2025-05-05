import requests
import yaml
import os

# Configuration for source and target workspaces
SOURCE_HOST  = "https://xxxxxxxxxxxxxxxxxxx.cloud.databricks.com"
TARGET_HOST  = "https://xxxxxxxxxxxxxxxxxxx.cloud.databricks.com"
SOURCE_TOKEN = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
TARGET_TOKEN = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

# The specific cluster ID to migrate
CLUSTER_ID = "cluster_Id"

# Output folder for YAML files
OUTPUT_DIR = "environment/single_cluster_details"

# Headers for API requests
SOURCE_HEADERS = {
    "Authorization": f"Bearer {SOURCE_TOKEN}",
    "Content-Type": "application/json"
}
TARGET_HEADERS = {
    "Authorization": f"Bearer {TARGET_TOKEN}",
    "Content-Type": "application/json"
}

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)


def api_request(host, endpoint, headers, method="GET", data=None):
    """Make an API request to Databricks"""
    url = f"{host}/api/{endpoint}"
    response = requests.request(method, url, headers=headers, json=data)
    if 200 <= response.status_code < 300:
        return response.json() if response.content else {}
    print(f"API request failed: {response.status_code} - {response.text}")
    return None


def write_yaml(filename, obj):
    """Write object as YAML into OUTPUT_DIR/filename"""
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w") as f:
        yaml.safe_dump(obj, f, sort_keys=False)
    print(f"Wrote YAML file: {path}")


def get_cluster_details(cluster_id):
    """Fetch full cluster details from source workspace"""
    print(f"Fetching details for cluster ID: {cluster_id}")
    details = api_request(
        SOURCE_HOST,
        f"2.0/clusters/get?cluster_id={cluster_id}",
        SOURCE_HEADERS,
        method="GET"
    )
    if not details:
        raise RuntimeError("Cannot fetch cluster details.")
    return details


def build_create_spec(details):
    """Filter out runtime fields to produce create-cluster spec"""
    spec = {
        "cluster_name": details["cluster_name"],
        "spark_version": details["spark_version"],
        "node_type_id": details["node_type_id"]
    }
    # Explicit optional fields
    if "num_workers" in details:
        spec["num_workers"] = details["num_workers"]
    if "autoscale" in details:
        spec["autoscale"] = details["autoscale"]
    if "spark_conf" in details:
        spec["spark_conf"] = details["spark_conf"]
    if "aws_attributes" in details:
        spec["aws_attributes"] = details["aws_attributes"]
    if "azure_attributes" in details:
        spec["azure_attributes"] = details["azure_attributes"]
    if "gcp_attributes" in details:
        spec["gcp_attributes"] = details["gcp_attributes"]
    if "driver_node_type_id" in details:
        spec["driver_node_type_id"] = details["driver_node_type_id"]
    if "ssh_public_keys" in details:
        spec["ssh_public_keys"] = details["ssh_public_keys"]
    if "custom_tags" in details:
        spec["custom_tags"] = details["custom_tags"]
    if "cluster_log_conf" in details:
        spec["cluster_log_conf"] = details["cluster_log_conf"]
    if "init_scripts" in details:
        spec["init_scripts"] = details["init_scripts"]
    if "spark_env_vars" in details:
        spec["spark_env_vars"] = details["spark_env_vars"]
    if "instance_pool_id" in details:
        spec["instance_pool_id"] = details["instance_pool_id"]
        print(f"NOTE: Using instance pool ID from source: {details['instance_pool_id']}")
    if "enable_elastic_disk" in details:
        spec["enable_elastic_disk"] = details["enable_elastic_disk"]
    if "cluster_source" in details:
        spec["cluster_source"] = details["cluster_source"]
    if "enable_local_disk_encryption" in details:
        spec["enable_local_disk_encryption"] = details["enable_local_disk_encryption"]
    if "runtime_engine" in details:
        spec["runtime_engine"] = details["runtime_engine"]
    if "idempotency_token" in details:
        spec["idempotency_token"] = details["idempotency_token"]
    if "single_user_name" in details:
        spec["single_user_name"] = details["single_user_name"]
    if "data_security_mode" in details:
        spec["data_security_mode"] = details["data_security_mode"]
    return spec


def create_cluster_in_target(spec):
    """Create cluster in target workspace using spec"""
    print(f"Creating cluster '{spec['cluster_name']}' in target workspace...")
    result = api_request(
        TARGET_HOST,
        "2.0/clusters/create",
        TARGET_HEADERS,
        method="POST",
        data=spec
    )
    if result and "cluster_id" in result:
        print(f"Cluster created with ID: {result['cluster_id']}")
    else:
        print("Failed to create cluster in target.")
    return result


def main():
    # Step 1: Fetch and write original details
    original = get_cluster_details(CLUSTER_ID)
    write_yaml("original_cluster_details.yml", original)

    # Step 2: Build create spec and write
    spec = build_create_spec(original)
    write_yaml("cluster_config.yml", spec)

    # Step 3: Create cluster in target
    create_cluster_in_target(spec)


if __name__ == "__main__":
    main()
