import os
import time
import boto3
import pytest
import shutil
import socket
import uvicorn
import contextlib
from botocore.client import Config
from multiprocessing import Process
from botocore.exceptions import ClientError

from app import app, ROOT_PATH

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

@pytest.fixture(autouse=True)
def clear_data():
    if os.path.isdir(ROOT_PATH):
        shutil.rmtree(ROOT_PATH)

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


def test_get_not_exist(s3_client):
    with pytest.raises(ClientError) as ex:
        response = s3_client.get_object(Bucket='dont_exist', Key='key')
    print(ex.value.response['Error'])
    assert ex.value.response['Error']['Code'] == 'NoSuchKey'

    s3_client.put_object(Bucket='new_bucket', Key='key', Body='body')

    with pytest.raises(ClientError) as ex:
        response = s3_client.get_object(Bucket='new_bucket', Key='dont_exist')
    assert ex.value.response['Error']['Code'] == 'NoSuchKey'


def test_list(s3_client):
    bucket = f'bucket-list'
    prefix = 'list'

    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix + '/')
    assert 'Contents' not in response

    for i in range(0, 100): # TODO: test over 1,000
        key = f'{prefix}/test-list-{i}'
        body = f'test body {i}'
        s3_client.put_object(Bucket=bucket, Key=key, Body=body)

    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    assert len(response['Contents']) == 100

    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix + '/')
    assert len(response['Contents']) == 100

def test_list_with_delimiter(s3_client):
    bucket = f'bucket-list-with-delimiter'
    body = 'body'

    response = s3_client.list_objects_v2(Bucket=bucket, Prefix='', Delimiter='/')
    assert 'Contents' not in response

    s3_client.put_object(Bucket=bucket, Key='file1', Body=body)
    s3_client.put_object(Bucket=bucket, Key='file2', Body=body)
    s3_client.put_object(Bucket=bucket, Key='dir1/file3', Body=body)
    s3_client.put_object(Bucket=bucket, Key='dir1/file4', Body=body)
    s3_client.put_object(Bucket=bucket, Key='dir1/dir2/file5', Body=body)
    s3_client.put_object(Bucket=bucket, Key='dir1/dir2/dir3/file6', Body=body)
    s3_client.put_object(Bucket=bucket, Key='dir1/dir4/file7', Body=body)
    s3_client.put_object(Bucket=bucket, Key='file8', Body=body)

    # non '/' delimiter
    with pytest.raises(ClientError):
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix='dir1/', Delimiter='f')

    response = s3_client.list_objects_v2(Bucket=bucket, Prefix='', Delimiter='/')
    assert set([x['Key'] for x in response['Contents']]) == {'file1', 'file2', 'file8'}
    assert set([x['Prefix'] for x in response['CommonPrefixes']]) == {'dir1/'}

    response = s3_client.list_objects_v2(Bucket=bucket, Prefix='dir1/', Delimiter='/')
    assert set([x['Key'] for x in response['Contents']]) == {'dir1/file3', 'dir1/file4'}
    assert set([x['Prefix'] for x in response['CommonPrefixes']]) == {'dir1/dir2/', 'dir1/dir4/'}

    response = s3_client.list_objects_v2(Bucket=bucket, Prefix='dir1/dir2/', Delimiter='/')
    assert set([x['Key'] for x in response['Contents']]) == {'dir1/dir2/file5'}
    assert set([x['Prefix'] for x in response['CommonPrefixes']]) == {'dir1/dir2/dir3/'}


def test_list_not_exists(s3_client):
    response = s3_client.list_objects_v2(Bucket='dont_exist', Prefix='')
    assert 'Contents' not in response
