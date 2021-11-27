#!/usr/bin/env python3
import argparse
import json
import logging

import grpc

import yandexcloud
from yandex.cloud.resourcemanager.v1.folder_service_pb2 import ListFoldersRequest
from yandex.cloud.resourcemanager.v1.folder_service_pb2_grpc import FolderServiceStub
from yandex.cloud.k8s.v1.cluster_service_pb2 import ListClustersRequest
from yandex.cloud.k8s.v1.cluster_service_pb2_grpc import ClusterServiceStub

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

def main():
    logging.basicConfig(level=logging.INFO)
    arguments = parse_args()
    interceptor = yandexcloud.RetryInterceptor(max_retry_count=5, retriable_codes=[grpc.StatusCode.UNAVAILABLE])
    if arguments.token:
        sdk = yandexcloud.SDK(interceptor=interceptor, token=arguments.token)
    else:
        with open(arguments.sa_json_path) as infile:
            sdk = yandexcloud.SDK(interceptor=interceptor, service_account_key=json.load(infile))

    operation = list_folder(sdk, arguments.cloud_id)

    for folder in operation:
        print(folder.name + ":")
        listClusters = list_cluster_k8s(sdk, folder.id)
        for cluster in listClusters:
            print("  " + cluster.name)


def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter)

    auth = parser.add_mutually_exclusive_group(required=True)
    auth.add_argument(
        '--sa-json-path',
        help='Path to the service account key JSON file.\nThis file can be created using YC CLI:\n'
             'yc iam key create --output sa.json --service-account-id <id>',
    )
    auth.add_argument('--token', help='OAuth token')
    parser.add_argument('--cloud-id', help='Your Yandex.Cloud cloud id', required=True)

    return parser.parse_args()


if __name__ == '__main__':
    main()

