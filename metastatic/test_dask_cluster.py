# test_dask_simple.py
from dask.distributed import Client
from dask_cloudprovider.gcp import GCPCluster

cluster = GCPCluster(
    projectid="end-to-end-ml-course-466603",
    zone="us-east1-b",
    machine_type="n1-standard-1",
    docker_image="us-east1-docker.pkg.dev/end-to-end-ml-course-466603/metastatic-server/metas-dask-worker:latest",
    n_workers=3,
    env_vars={
        'GOOGLE_APPLICATION_CREDENTIALS': '/app/creds/metas-project-8b1a906b4b86.json',
        'MLFLOW_TRACKING_URI': 'http://10.142.0.47:6100'
    }
)

print("Connecting client...")
client = Client(cluster, timeout='180s')
print(f"Client: {client}")
print(f"Dashboard: {client.dashboard_link}")

def square(x):
    return x ** 2

futures = client.map(square, range(10))
results = client.gather(futures)
print(f"Results: {results}")

input("Press Enter to close...")
client.close()
cluster.close()