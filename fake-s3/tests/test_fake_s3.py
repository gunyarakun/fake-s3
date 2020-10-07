import time
import boto3
import pytest
import socket
import uvicorn
import contextlib
from botocore.client import Config
from multiprocessing import Process
from botocore.exceptions import ClientError

from app import app

def launch(port):
    uvicorn.run(app, host='0.0.0.0', port=port)

@pytest.fixture(scope='module', autouse=True)
def available_port():
    with contextlib.closing(socket.socket()) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]

@pytest.fixture(scope='module', autouse=True)
def server(available_port):
    p = Process(target=launch, args=(available_port,))
    p.start()
    yield server
    p.terminate()

@pytest.fixture(scope='module', autouse=True)
def s3_client(available_port, server):
    return boto3.client(
        's3',
        aws_access_key_id='dummy access key id',
        aws_secret_access_key='dummy secret access key',
        endpoint_url=f'http://localhost:{available_port}',
        config=Config(s3={'addressing_style': 'path'}) # Include a bucket name in the path
    )

def test_put_get_and_delete(s3_client):
    epoch = int(time.time() * 1000)
    bucket = f'bucket-{epoch}'
    key = f'test-put-{epoch}'
    body = f'test body {epoch}'
    s3_client.put_object(Bucket=bucket, Key=key, Body=body)

    response = s3_client.get_object(Bucket=bucket, Key=key)
    resp_body = response['Body'].read().decode('utf-8')
    assert body == resp_body

    response = s3_client.delete_object(Bucket=bucket, Key=key)
    assert response['ResponseMetadata']['HTTPStatusCode'] == 204

    with pytest.raises(ClientError):
        response = s3_client.get_object(Bucket=bucket, Key=key)
