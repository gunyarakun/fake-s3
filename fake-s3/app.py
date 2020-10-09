import os
import urllib
import hashlib
import aiofiles
from datetime import datetime
import xml.etree.cElementTree as ET

ROOT_PATH='/data'

async def read_file(path):
    async with aiofiles.open(path, mode='rb') as f:
        return await f.read()

async def write_file(path, receive):
    async with aiofiles.open(path, mode='wb') as f:
        more_body = True
        while more_body:
            message = await receive()
            body = message.get('body', b'')
            await f.write(body)
            more_body = message.get('more_body', False)

def resolve_path(path):
    return ROOT_PATH + os.path.abspath(path)

async def send_response(send, status, headers=[], body=b''):
    await send({
        'type': 'http.response.start',
        'status': status,
        'headers': headers,
    })
    await send({
        'type': 'http.response.body',
        'body': body,
    })

def find_all_files(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            yield os.path.join(root, file)

def generate_contents_element(root_elem, path, bucket_path):
    stat = os.stat(path)
    mtime = datetime.utcfromtimestamp(stat.st_mtime)
    iso_mtime = mtime.strftime('%Y-%m-%dT%H:%M:%SZ')
    etag_key = (iso_mtime + path).encode('utf-8') # FIXME: wow, isn't from the content!
    etag = '"' + hashlib.sha1(etag_key).hexdigest() + '"'
    rel_path = os.path.relpath(path, bucket_path)

    contents = ET.SubElement(root_elem, 'Contents')
    ET.SubElement(contents, 'Key').text = rel_path
    ET.SubElement(contents, 'LastModified').text = iso_mtime
    ET.SubElement(contents, 'ETag').text = etag
    ET.SubElement(contents, 'Size').text = str(stat.st_size)
    ET.SubElement(contents, 'StorageClass').text = 'STANDARD'


async def send_list(bucket_path, prefix, delimiter, send):
    # Check trailing '/' on prefix
    if prefix[-1:] != '/':
        return await send_response(send, 400)
    # Remove heading '/' on prefix
    if prefix[:1] == '/':
        prefix = prefix[1:]
    prefix_path = os.path.join(bucket_path, prefix)

    root_elem = ET.Element('ListBucketResult', xmlns='http://s3.amazonaws.com/doc/2006-03-01/')
    if not os.path.isdir(prefix_path):
        return await send_response(send, 400)
    elif delimiter is None:
        for path in find_all_files(prefix_path):
            generate_contents_element(root_elem, path, bucket_path)
    elif delimiter == '/':
        for f in os.listdir(prefix_path):
            path = os.path.join(prefix_path, f)
            if os.path.isfile(path):
                generate_contents_element(root_elem, path, bucket_path)
            else:
                common_prefixes = ET.SubElement(root_elem, 'CommonPrefixes')
                ET.SubElement(common_prefixes, 'Prefix').text = prefix + f + '/'
    else:
        # Now supports only '/'
        return await send_response(send, 400)

    await send_response(send, 200, [
            [b'content-type', b'application/xml'],
            [b'access-control-allow-origin', b'*'],
        ], ET.tostring(root_elem, encoding='utf-8'))

async def get(scope, send):
    url_path = scope['path']
    path = resolve_path(url_path)

    if os.path.isdir(path):
        # TODO: Check the url_path is only a bucket name
        qs = dict(urllib.parse.parse_qsl(scope.get('query_string', b'').decode('utf-8')))
        await send_list(
                path,
                qs.get('prefix', '/'),
                qs.get('delimiter'),
                send)
    elif os.path.isfile(path):
        await send_response(send, 200, [
                [b'content-type', b'binary/octet-stream'],
                [b'access-control-allow-origin', b'*'],
            ], await read_file(path))
    else:
        await send_response(send, 404)

async def put(scope, receive, send):
    abspath = os.path.abspath(scope['path'])

    # Do not put directory
    if abspath[-1] == '/':
        await send_response(send, 400)
        return

    path = resolve_path(abspath)

    dirname = os.path.dirname(path)
    if not os.path.exists(dirname):
        os.makedirs(dirname, exist_ok=True)

    await write_file(path, receive)
    await send_response(send, 200)

async def delete(scope, send):
    path = resolve_path(scope['path'])

    if not os.path.isfile(path):
        await send_response(send, 404)
        return

    os.remove(path)

    await send_response(send, 204)

async def app(scope, receive, send):
    assert scope['type'] == 'http'

    if scope['method'] == 'GET':
        await get(scope, send)
    elif scope['method'] == 'PUT':
        await put(scope, receive, send)
    elif scope['method'] == 'DELETE':
        await delete(scope, send)
