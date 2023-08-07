import os
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

os.environ["DATABRICKS_HOST"]
os.environ["DATABRICKS_TOKEN"]
os.environ["DATABRICKS_CLUSTER_ID"]

w = WorkspaceClient()

notebook_path = f"/Users/{w.current_user.me().user_name}/sdk-demo"

cluster_id = os.environ["DATABRICKS_CLUSTER_ID"]

# CREATE
py_on_dbfs = f"/home/{w.current_user.me().user_name}/sample.py"
with w.dbfs.open(py_on_dbfs, write=True, overwrite=True) as f:
    f.write(b'import time; time.sleep(10); print("Hello, World!")')

created_job = w.jobs.create(
    name=f"sdk-demo-python-task",  # metadata
    tasks=[
        jobs.Task(
            description="test",  # metadata
            existing_cluster_id=cluster_id,  # metadata
            spark_python_task=jobs.SparkPythonTask(
                python_file=f"dbfs:{py_on_dbfs}"
            ),  # metadata
            # notebook_task=jobs.NotebookTask(
            #     notebook_path=notebook_path  # metadata
            # ),  # metadata
            task_key="test",  # metadata
            timeout_seconds=0,  # metadata
        )
    ],
)

# GET
run_by_id = w.jobs.run_now(job_id=created_job.job_id).result()

# DELETE
# w.jobs.cancel_all_runs(job_id=created_job.job_id)
# w.jobs.delete(job_id=created_job.job_id)

"""
Problem: Don't know why submit doesn't work
"""
