import boto3
import json

CLUSTER = "foo"
SUBNETS = ["foo", "bar"]
SECURITY_GROUPS = ["baz"]


def run_ecs_task(cluster, task, container, subnets, security_groups, item_name, env):
    print(f"starting task {task} cluster {cluster} item_name={item_name}")
    client = boto3.client("ecs", region_name="eu-central-1")
    response = client.run_task(
        cluster=cluster,
        launchType="FARGATE",
        taskDefinition=task,
        count=1,
        platformVersion="LATEST",
        networkConfiguration={
            "awsvpcConfiguration": {
                "subnets": subnets,
                "securityGroups": security_groups,
                "assignPublicIp": "DISABLED",
            }
        },
        overrides={
            "containerOverrides": [
                {
                    "name": container,
                    "environment": env,
                },
            ]
        },
    )
    print(f"ecs run task response={response} item_name={item_name}")


def run_delta_convertor(
    item_name: str,
    bucket_source: str,
    bucket_target: str,
    prefix_source: str,
    prefix_target: str,
    workers: int,
    mode: str,
    partition_columns: list = None,
    chunk_size: int = None,
    checkpoint: int = None,
    debug: bool = False,
) -> None:
    task = "foo"
    container = "bar"
    args = json.dumps(
        {
            "partition_columns": partition_columns,
            "workers": workers,
            "mode": mode,
            "chunk_size": chunk_size,
            "checkpoint": checkpoint,
            "debug": debug,
        }
    )

    env = [
        {"name": "bucket_source", "value": bucket_source},
        {"name": "bucket_target", "value": bucket_target},
        {"name": "prefix_source", "value": prefix_source},
        {"name": "prefix_target", "value": prefix_target},
        {"name": "item_name", "value": item_name},
        {"name": "args", "value": args},
    ]
    run_ecs_task(CLUSTER, task, container, SUBNETS,
                 SECURITY_GROUPS, item_name, env)


if __name__ == "__main__":
    run_delta_convertor(
        item_name="foo",
        bucket_source="source",
        bucket_target="target",
        prefix_source="source",
        prefix_target="target",
        workers=5,
        mode="init",  # init | append,
        chunk_size=5000,  # rows per batch to write - target file size in Delta Lake
        checkpoint=100,  # create checkpoint after n count of versions
        # partition_columns=["id"],
        debug=True,
    )
