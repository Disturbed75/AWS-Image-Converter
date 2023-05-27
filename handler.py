import base64
import json
import os

import boto3
import urllib3

PNG_BUCKET_NAME = 'png-img-test-bucket'
GIF_BUCKET_NAME = 'gif-img-test-bucket'
API_KEY = os.environ.get('API_KEY')

s3 = boto3.client('s3')
http = urllib3.PoolManager()


def is_jpg(key):
    arr = key.split('.')
    file_format = arr[1]
    return file_format == 'jpg'


def get_file_name(key):
    arr = key.split('/')
    name_and_format = arr[len(arr) - 1]
    name = name_and_format.split('.')[0]
    return name


def get_image_base64(bucket_name, key):
    try:
        obj = s3.get_object(
            Bucket=bucket_name,
            Key=key,
            ResponseContentEncoding='base64'
        )
        body = obj['Body'].read()
        byte_base_64 = base64.b64encode(body)
        res = byte_base_64.decode('utf-8')
        return res
    except Exception as e:
        print(e)
        return None


def put_object(bucket_name, key, body):
    try:
        s3.put_object(Bucket=bucket_name, Key=key, Body=body)
    except Exception as e:
        print(e)


def get_converted_image_url(response):
    response_body = json.loads(response.data)
    download_url = response_body['Files'][0]['Url']
    response = http.urlopen('GET', download_url)
    return response.data


def convert_image_to_gif(base_64_input):
    url = 'https://v2.convertapi.com/convert/jpg/to/gif?Secret=' + API_KEY
    request = {
        'Parameters': [
            {
                'Name': 'Files',
                'FileValues': [
                    {
                        'Name': 'my_file.jpg',
                        'Data': base_64_input
                    }
                ]
            },
            {
                'Name': 'StoreFile',
                'Value': True
            }
        ]
    }
    encoded_request = json.dumps(request)
    response = http.request('POST', url, headers={'Content-Type': 'application/json'}, body=encoded_request)
    return get_converted_image_url(response)


def convert_image_to_png(base_64_input):
    url = 'https://v2.convertapi.com/convert/jpg/to/png?Secret=' + API_KEY
    request = {
        'Parameters': [
            {
                'Name': 'File',
                'FileValue': {
                    'Name': 'my_file.jpg',
                    'Data': base_64_input
                }
            },
            {
                'Name': 'StoreFile',
                'Value': True
            }
        ],
    }
    encoded_request = json.dumps(request)
    response = http.request('POST', url, headers={'Content-Type': 'application/json'}, body=encoded_request)
    return get_converted_image_url(response)


def handler(event):
    s3_event = event['Records'][0]['s3']
    bucket_name = s3_event['bucket']['name']
    key = s3_event['object']['key']

    is_jpeg = is_jpg(key)

    if is_jpeg:
        file_name = get_file_name(key)
        base_64_image = get_image_base64(bucket_name, key)

        if base_64_image is not None:
            png_bytes = convert_image_to_png(base_64_image)
            put_object(PNG_BUCKET_NAME, file_name + '.png', png_bytes)

            gif_bytes = convert_image_to_gif(base_64_image)
            put_object(GIF_BUCKET_NAME, file_name + '.gif', gif_bytes)

    return {'statusCode': 200}
