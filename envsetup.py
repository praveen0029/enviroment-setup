import requests
import json
import os
import base64
from datetime import datetime

# Configuration for source and target workspaces
SOURCE_HOST  = "https://xxxxxxxxxxxxxxxxxxx.cloud.databricks.com"
TARGET_HOST  = "https://xxxxxxxxxxxxxxxxxxx.cloud.databricks.com"
SOURCE_TOKEN = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
TARGET_TOKEN = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

# Headers for API requests
SOURCE_HEADERS = {
    "Authorization": f"Bearer {SOURCE_TOKEN}",
    "Content-Type": "application/json"
}

TARGET_HEADERS = {
    "Authorization": f"Bearer {TARGET_TOKEN}",
    "Content-Type": "application/json"
}

# Create a directory to store extracted configuration
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_dir = f"environment/databricks_migration_{timestamp}"
os.makedirs(output_dir, exist_ok=True)

def api_request(host, endpoint, headers, method="GET", data=None):
    """Make an API request to Databricks"""
    url = f"{host}/api/{endpoint}"
    
    if method == "GET":
        response = requests.get(url, headers=headers)
    elif method == "POST":
        response = requests.post(url, headers=headers, json=data)
    elif method == "PUT":
        response = requests.put(url, headers=headers, json=data)
    
    if response.status_code >= 200 and response.status_code < 300:
        return response.json() if response.content else None
    else:
        print(f"API request failed: {response.status_code} {response.text}")
        return None

def extract_clusters():
    """Extract cluster configurations"""
    print("Extracting cluster configurations...")
    clusters = api_request(SOURCE_HOST, "2.0/clusters/list", SOURCE_HEADERS)
    
    if clusters:
        with open(f"{output_dir}/clusters.json", "w") as f:
            json.dump(clusters, f, indent=2)
        
        return clusters
    return None

def extract_jobs():
    """Extract job configurations"""
    print("Extracting job configurations...")
    jobs = api_request(SOURCE_HOST, "2.1/jobs/list", SOURCE_HEADERS)
    
    if jobs:
        # For each job, get the full job details
        detailed_jobs = []
        for job in jobs.get("jobs", []):
            job_id = job.get("job_id")
            job_details = api_request(SOURCE_HOST, f"2.1/jobs/get?job_id={job_id}", SOURCE_HEADERS)
            if job_details:
                detailed_jobs.append(job_details)
        
        with open(f"{output_dir}/jobs.json", "w") as f:
            json.dump(detailed_jobs, f, indent=2)
        
        return detailed_jobs
    return None

def extract_dbfs_files():
    """Extract DBFS file paths"""
    print("Extracting DBFS file list...")
    dbfs_files = api_request(SOURCE_HOST, "2.0/dbfs/list?path=/", SOURCE_HEADERS)
    
    if dbfs_files:
        with open(f"{output_dir}/dbfs_files.json", "w") as f:
            json.dump(dbfs_files, f, indent=2)
        
        # Create a function to recursively list files
        all_files = []
        
        def list_recursive(path):
            files = api_request(SOURCE_HOST, f"2.0/dbfs/list?path={path}", SOURCE_HEADERS)
            if files and "files" in files:
                for file in files["files"]:
                    all_files.append(file)
                    if file["is_dir"]:
                        list_recursive(file["path"])
        
        list_recursive("/")
        
        with open(f"{output_dir}/dbfs_files_recursive.json", "w") as f:
            json.dump(all_files, f, indent=2)
        
        return all_files
    return None

def extract_notebooks():
    """Extract notebooks from workspace"""
    print("Extracting notebooks...")
    notebooks = api_request(SOURCE_HOST, "2.0/workspace/list?path=/", SOURCE_HEADERS)
    
    if notebooks:
        with open(f"{output_dir}/notebooks.json", "w") as f:
            json.dump(notebooks, f, indent=2)
        
        # Create a function to recursively list notebooks and download their content
        all_notebooks = []
        
        def list_notebooks_recursive(path):
            items = api_request(SOURCE_HOST, f"2.0/workspace/list?path={path}", SOURCE_HEADERS)
            if items and "objects" in items:
                for item in items["objects"]:
                    all_notebooks.append(item)
                    
                    # Download notebook content if it's a notebook
                    if item["object_type"] == "NOTEBOOK":
                        notebook_path = item["path"]
                        export_format = "SOURCE"  # DBC, SOURCE, HTML
                        notebook_content = api_request(
                            SOURCE_HOST, 
                            f"2.0/workspace/export?path={notebook_path}&format={export_format}",
                            SOURCE_HEADERS
                        )
                        
                        if notebook_content and "content" in notebook_content:
                            # Create directory structure
                            notebook_dir = f"{output_dir}/notebooks{os.path.dirname(notebook_path)}"
                            os.makedirs(notebook_dir, exist_ok=True)
                            
                            # Save notebook content
                            with open(f"{notebook_dir}/{os.path.basename(notebook_path)}.py", "w") as nb_file:
                                nb_file.write(base64.b64decode(notebook_content["content"]).decode("utf-8"))
                    
                    # Recursively process directories
                    if item["object_type"] == "DIRECTORY":
                        list_notebooks_recursive(item["path"])
        
        list_notebooks_recursive("/")
        
        with open(f"{output_dir}/notebooks_recursive.json", "w") as f:
            json.dump(all_notebooks, f, indent=2)
        
        return all_notebooks
    return None

def extract_instance_pools():
    """Extract instance pool configurations"""
    print("Extracting instance pool configurations...")
    pools = api_request(SOURCE_HOST, "2.0/instance-pools/list", SOURCE_HEADERS)
    
    if pools:
        with open(f"{output_dir}/instance_pools.json", "w") as f:
            json.dump(pools, f, indent=2)
        
        return pools
    return None

def extract_secrets():
    """Extract secret scopes (not the secret values)"""
    print("Extracting secret scope configurations...")
    scopes = api_request(SOURCE_HOST, "2.0/secrets/scopes/list", SOURCE_HEADERS)
    
    if scopes:
        secret_scopes = []
        for scope in scopes.get("scopes", []):
            scope_name = scope.get("name")
            # Get ACLs for each scope
            acls = api_request(SOURCE_HOST, f"2.0/secrets/acls/list?scope={scope_name}", SOURCE_HEADERS)
            scope["acls"] = acls.get("items", []) if acls else []
            secret_scopes.append(scope)
        
        with open(f"{output_dir}/secret_scopes.json", "w") as f:
            json.dump(secret_scopes, f, indent=2)
        
        return secret_scopes
    return None

def extract_users_groups():
    """Extract users and groups"""
    print("Extracting users and groups...")
    users = api_request(SOURCE_HOST, "2.0/preview/scim/v2/Users", SOURCE_HEADERS)
    groups = api_request(SOURCE_HOST, "2.0/preview/scim/v2/Groups", SOURCE_HEADERS)
    
    if users:
        with open(f"{output_dir}/users.json", "w") as f:
            json.dump(users, f, indent=2)
    
    if groups:
        with open(f"{output_dir}/groups.json", "w") as f:
            json.dump(groups, f, indent=2)
    
    return users, groups

def extract_permissions():
    """Extract permissions for various objects"""
    print("Extracting permissions...")
    permissions = {}
    
    # Clusters permissions
    clusters = api_request(SOURCE_HOST, "2.0/clusters/list", SOURCE_HEADERS)
    if clusters and "clusters" in clusters:
        cluster_permissions = []
        for cluster in clusters["clusters"]:
            cluster_id = cluster.get("cluster_id")
            if cluster_id:
                perms = api_request(SOURCE_HOST, f"2.0/permissions/clusters/{cluster_id}", SOURCE_HEADERS)
                if perms:
                    cluster_permissions.append({
                        "cluster_id": cluster_id,
                        "permissions": perms
                    })
        permissions["clusters"] = cluster_permissions
    
    # Jobs permissions
    jobs = api_request(SOURCE_HOST, "2.1/jobs/list", SOURCE_HEADERS)
    if jobs and "jobs" in jobs:
        job_permissions = []
        for job in jobs["jobs"]:
            job_id = job.get("job_id")
            if job_id:
                perms = api_request(SOURCE_HOST, f"2.0/permissions/jobs/{job_id}", SOURCE_HEADERS)
                if perms:
                    job_permissions.append({
                        "job_id": job_id,
                        "permissions": perms
                    })
        permissions["jobs"] = job_permissions
    
    # Instance pool permissions
    pools = api_request(SOURCE_HOST, "2.0/instance-pools/list", SOURCE_HEADERS)
    if pools and "instance_pools" in pools:
        pool_permissions = []
        for pool in pools["instance_pools"]:
            pool_id = pool.get("instance_pool_id")
            if pool_id:
                perms = api_request(SOURCE_HOST, f"2.0/permissions/instance-pools/{pool_id}", SOURCE_HEADERS)
                if perms:
                    pool_permissions.append({
                        "pool_id": pool_id,
                        "permissions": perms
                    })
        permissions["instance_pools"] = pool_permissions
    
    with open(f"{output_dir}/permissions.json", "w") as f:
        json.dump(permissions, f, indent=2)
    
    return permissions

def extract_workspace_conf():
    """Extract workspace configurations"""
    print("Extracting workspace configurations...")
    workspace_conf = api_request(SOURCE_HOST, "2.0/workspace-conf", SOURCE_HEADERS)
    
    if workspace_conf:
        with open(f"{output_dir}/workspace_conf.json", "w") as f:
            json.dump(workspace_conf, f, indent=2)
        
        return workspace_conf
    return None

def extract_all():
    """Extract all configurations"""
    print("Starting extraction of Databricks environment...")
    
    extract_clusters()
    extract_jobs()
    extract_dbfs_files()
    extract_notebooks()
    extract_instance_pools()
    extract_secrets()
    extract_users_groups()
    extract_permissions()
    extract_workspace_conf()
    
    print(f"Extraction complete. Configuration saved to '{output_dir}' directory.")

def create_target_environment():
    """Create environment in target workspace"""
    print("Creating environment in target workspace...")
    
    # Load configuration files
    try:
        with open(f"{output_dir}/instance_pools.json", "r") as f:
            instance_pools = json.load(f)
        
        with open(f"{output_dir}/clusters.json", "r") as f:
            clusters = json.load(f)
        
        with open(f"{output_dir}/secret_scopes.json", "r") as f:
            secret_scopes = json.load(f)
        
        with open(f"{output_dir}/jobs.json", "r") as f:
            jobs = json.load(f)
    except FileNotFoundError as e:
        print(f"Error loading configuration: {e}")
        return
    
    # Create instance pools first
    if instance_pools and "instance_pools" in instance_pools:
        for pool in instance_pools["instance_pools"]:
            # Remove instance pool ID and other fields that should not be included in create request
            create_pool = {
                "instance_pool_name": pool["instance_pool_name"],
                "node_type_id": pool["node_type_id"],
                "min_idle_instances": pool["min_idle_instances"],
                "max_capacity": pool["max_capacity"],
                "preloaded_spark_versions": pool.get("preloaded_spark_versions", []),
                "idle_instance_autotermination_minutes": pool.get("idle_instance_autotermination_minutes", 60)
            }
            
            # Add custom tags if present
            if "custom_tags" in pool:
                create_pool["custom_tags"] = pool["custom_tags"]
            
            result = api_request(TARGET_HOST, "2.0/instance-pools/create", TARGET_HEADERS, method="POST", data=create_pool)
            if result:
                print(f"Created instance pool: {pool['instance_pool_name']}")
            else:
                print(f"Failed to create instance pool: {pool['instance_pool_name']}")
    
    # Create secret scopes
    for scope in secret_scopes:
        scope_name = scope["name"]
        
        # Create scope
        create_scope = {
            "scope": scope_name
        }
        if "backend_type" in scope and scope["backend_type"] == "DATABRICKS":
            create_scope["scope_backend_type"] = "DATABRICKS"
        
        result = api_request(TARGET_HOST, "2.0/secrets/scopes/create", TARGET_HEADERS, method="POST", data=create_scope)
        if result is not None:
            print(f"Created secret scope: {scope_name}")
            
            # Create ACLs
            for acl in scope.get("acls", []):
                create_acl = {
                    "scope": scope_name,
                    "principal": acl["principal"],
                    "permission": acl["permission"]
                }
                acl_result = api_request(
                    TARGET_HOST, 
                    "2.0/secrets/acls/put", 
                    TARGET_HEADERS, 
                    method="POST", 
                    data=create_acl
                )
                if acl_result is not None:
                    print(f"  Added ACL for {acl['principal']} on scope {scope_name}")
                else:
                    print(f"  Failed to add ACL for {acl['principal']} on scope {scope_name}")
        else:
            print(f"Failed to create secret scope: {scope_name}")
    
    # Create clusters
    if clusters and "clusters" in clusters:
        for cluster in clusters["clusters"]:
            # Skip terminated clusters that are not meant to be persistent
            if cluster.get("state") == "TERMINATED" and not cluster.get("is_pinned", False):
                continue
            
            # Create a simplified cluster configuration
            create_cluster = {
                "cluster_name": cluster["cluster_name"],
                "spark_version": cluster["spark_version"],
                "node_type_id": cluster["node_type_id"],
                "autoscale": cluster.get("autoscale", {}),
                "num_workers": cluster.get("num_workers", 0),
                "spark_conf": cluster.get("spark_conf", {}),
                "ssh_public_keys": cluster.get("ssh_public_keys", []),
                "custom_tags": cluster.get("custom_tags", {}),
                "init_scripts": cluster.get("init_scripts", []),
                "spark_env_vars": cluster.get("spark_env_vars", {})
            }
            
            # Add instance pool ID if specified
            if "instance_pool_id" in cluster:
                # You may need to map old instance pool IDs to new ones
                create_cluster["instance_pool_id"] = cluster["instance_pool_id"]
            
            # Add other optional fields
            for field in ["driver_node_type_id", "enable_elastic_disk", "cluster_log_conf", 
                          "cluster_source", "enable_local_disk_encryption", "runtime_engine"]:
                if field in cluster:
                    create_cluster[field] = cluster[field]
            
            result = api_request(TARGET_HOST, "2.0/clusters/create", TARGET_HEADERS, method="POST", data=create_cluster)
            if result:
                print(f"Created cluster: {cluster['cluster_name']}")
            else:
                print(f"Failed to create cluster: {cluster['cluster_name']}")
    
    # Import notebooks
    notebook_dir = f"{output_dir}/notebooks"
    if os.path.exists(notebook_dir):
        for root, dirs, files in os.walk(notebook_dir):
            for file in files:
                if file.endswith(".py"):
                    source_path = os.path.join(root, file)
                    
                    # Calculate the relative path for the target workspace
                    rel_path = os.path.relpath(source_path, notebook_dir)
                    target_path = os.path.splitext(rel_path)[0]  # Remove .py extension
                    
                    with open(source_path, "r") as f:
                        content = f.read()
                        
                    # Import notebook
                    import_data = {
                        "path": target_path,
                        "format": "SOURCE",
                        "language": "PYTHON",
                        "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
                        "overwrite": True
                    }
                    
                    result = api_request(
                        TARGET_HOST, 
                        "2.0/workspace/import", 
                        TARGET_HEADERS, 
                        method="POST", 
                        data=import_data
                    )
                    
                    if result is not None:
                        print(f"Imported notebook: {target_path}")
                    else:
                        print(f"Failed to import notebook: {target_path}")
    
    # Create jobs
    for job in jobs:
        # Remove job ID and other fields that should not be included in create request
        job_settings = job["settings"]
        
        result = api_request(TARGET_HOST, "2.1/jobs/create", TARGET_HEADERS, method="POST", data={"name": job_settings["name"], "settings": job_settings})
        if result:
            print(f"Created job: {job_settings['name']}")
        else:
            print(f"Failed to create job: {job_settings['name']}")
    
    print("Environment creation completed in target workspace.")

# Main execution
if __name__ == "__main__":
    print("Databricks Environment Migration Tool")
    print("1. Extract environment from source workspace")
    print("2. Create environment in target workspace")
    print("3. Do both")
    
    choice = input("Choose an option (1-3): ")
    
    if choice == "1":
        extract_all()
    elif choice == "2":
        create_target_environment()
    elif choice == "3":
        extract_all()
        create_target_environment()
    else:
        print("Invalid choice. Exiting.")