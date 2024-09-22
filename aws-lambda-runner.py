import boto3
import json


def run_lambda(lambda_name: str, item: str, payload: dict[str, str]) -> None:
    print(f"launching aws lambda {lambda_name} for item: {item}")
    client = boto3.client('lambda', region_name="region")
    resp = client.invoke(
        FunctionName=lambda_name,
        InvocationType='Event',
        Payload=json.dumps(payload),
    )
    print(f"aws lambda {lambda_name} for item: {item} resp:{resp}")


def run_some_lambda(item: str, foo: str, bar: str, baz: str) -> None:
    lambda_name = "foo"
    payload = {
      "foo": foo,
      "bar": bar,
      "baz": baz,
    }
    run_lambda(lambda_name, item, foo, bar, baz, payload)



if __name__ == "__main__":
    run_some_lambda("item", "foo", "bar", "baz")

