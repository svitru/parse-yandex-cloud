#!/usr/bin/env python3
import argparse
import json
import logging
import time
import jwt
import grpc

from prettytable import PrettyTable
import tempfile
import os
from kubernetes import client, config

import yandexcloud
from yandex.cloud.resourcemanager.v1.folder_service_pb2 import ListFoldersRequest
from yandex.cloud.resourcemanager.v1.folder_service_pb2_grpc import FolderServiceStub
from yandex.cloud.k8s.v1.cluster_service_pb2 import ListClustersRequest
from yandex.cloud.k8s.v1.cluster_service_pb2_grpc import ClusterServiceStub
from yandex.cloud.iam.v1.iam_token_service_pb2 import CreateIamTokenRequest
from yandex.cloud.iam.v1.iam_token_service_pb2_grpc import IamTokenServiceStub


def list_folder(sdk, cloud_id):
    folder_service = sdk.client(FolderServiceStub)
    listOfFolders = []
    page_token = None
    while True:
        response = folder_service.List(ListFoldersRequest(cloud_id=cloud_id, page_token=page_token))
        for folder in response.folders:
            listOfFolders.append(folder)
        page_token = response.next_page_token
        if not page_token:
            break
    return listOfFolders

def list_cluster_k8s(sdk, folder_id):
    cluster_k8s_service = sdk.client(ClusterServiceStub)
    listOfClusters = []
    page_token = None
    while True:
        response = cluster_k8s_service.List(ListClustersRequest(folder_id=folder_id, page_token=page_token))
        for cluster in response.clusters:
            listOfClusters.append(cluster)
        page_token = response.next_page_token
        if not page_token:
            break
    return listOfClusters

def get_token_k8s(sdk, service_account_json):
    iam_service = sdk.client(IamTokenServiceStub)

    now = int(time.time())
    payload = {
        'aud': 'https://iam.api.cloud.yandex.net/iam/v1/tokens',
        'iss': service_account_json["service_account_id"],
        'iat': now,
        'exp': now + 360}
    encoded_token = jwt.encode(
        payload,
        service_account_json["private_key"],
        algorithm='PS256',
        headers={'kid': service_account_json["id"]})
    response = iam_service.Create(CreateIamTokenRequest(jwt=encoded_token))
    return response

def get_k8s_api(endpoint, certificate, iam_token):
    aConfiguration = client.Configuration()
    aConfiguration.host = endpoint
    handler, nameFileCert = tempfile.mkstemp()
    os.write(handler, str.encode(certificate))
    os.close(handler)
    aConfiguration.verify_ssl = True
    aConfiguration.ssl_ca_cert = nameFileCert
    aConfiguration.api_key = {"authorization": "Bearer " + iam_token}
    aApiClient = client.ApiClient(aConfiguration)
    v1 = client.AppsV1Api(aApiClient)
    return v1

def main():
    logging.basicConfig(level=logging.INFO)
    arguments = parse_args()
    interceptor = yandexcloud.RetryInterceptor(max_retry_count=5, retriable_codes=[grpc.StatusCode.UNAVAILABLE])
    with open(arguments.sa_json_path) as infile:
        service_account_key=json.load(infile)
        sdk = yandexcloud.SDK(interceptor=interceptor, service_account_key=service_account_key)
    k8sToken = get_token_k8s(sdk, service_account_key)
    operation = list_folder(sdk, arguments.cloud_id)

    for folder in operation:
        if arguments.filter_folder in folder.name:
            print(folder.name + ":")
            listClusters = list_cluster_k8s(sdk, folder.id)
            for cluster in listClusters:
                print("  " + cluster.name)
                print("    internal endpoint: " + cluster.master.endpoints.internal_v4_endpoint)
                v1 = get_k8s_api(cluster.master.endpoints.internal_v4_endpoint, cluster.master.master_auth.cluster_ca_certificate, k8sToken.iam_token)
                print("Listing deployments:")
                ret = v1.list_deployment_for_all_namespaces(watch=False)
                t = PrettyTable(['Name', 'Namespace'])
                t.align['Name'] = "l"
                t.align['Namespace'] = "c"
                for i in ret.items:
                    t.add_row([i.metadata.name, i.metadata.namespace])
                print(t)
                print("")

def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument(
        '--sa-json-path',
        help='Path to the service account key JSON file.\nThis file can be created using YC CLI:\n'
             'yc iam key create --output sa.json --service-account-id <id>', required=True
    )
    parser.add_argument('--cloud-id', help='Your Yandex.Cloud cloud id', required=True)
    parser.add_argument('--filter-folder', help='Substring in Folder name', default='')

    return parser.parse_args()


if __name__ == '__main__':
    main()

