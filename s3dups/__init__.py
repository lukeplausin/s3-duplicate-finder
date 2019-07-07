#!/usr/bin/env python

import boto3
import difflib
import os
import json
from collections import Counter


def build_hashmap(Bucket, Prefix="", client=boto3.client('s3')):
    # Calculate disk usage within S3 and report back to parent
    hashes = {}
    null_items = []
    paginator = client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=Bucket, Prefix=Prefix)

    for page in page_iterator:
        for item in page.get('Contents', []):
            if item['Size'] > 0:
                items = hashes.get(item['ETag'], [])
                items.append(item)
                hashes[item['ETag']] = items
            else:
                null_items.append(item)
    # TODO: Something with null items?
    return hashes


def s3_duplicates( 
            Bucket, Prefix="", client=boto3.client('s3')
        ):
    full_hash_list = build_hashmap(Bucket=Bucket, Prefix=Prefix, client=client)
    duplicate_hashes = {
        key: value
        for key, value in full_hash_list.items()
        if len(value) > 1
    }
    return duplicate_hashes


def analyse_duplicate_sets(duplicate_hashes, Delimiter="/"):
    full_key_list = []
    key_to_object = {}
    common_prefixes = {}
    for value in duplicate_hashes.values():
        for duplicate in value:
            full_key_list.append(duplicate['Key'])
            key_to_object[duplicate['Key']] = duplicate

    full_key_list.sort()

    for index, value in enumerate(full_key_list[0:-1]):
        comparison = full_key_list[index+1]
        seq_matcher = difflib.SequenceMatcher(None, value, comparison)
        match = seq_matcher.find_longest_match(0, len(value), 0, len(comparison))
        prefix = value[match.a:(match.a+match.size)].rsplit(Delimiter,1)[0]
        if Delimiter in prefix:
            common_prefix_items = common_prefixes.get(prefix, set([]))
            common_prefix_items.add(value)
            common_prefix_items.add(comparison)
            common_prefixes[prefix] = common_prefix_items
            key_to_object[value]['CommonPrefix'] = prefix

    duplicate_sets = {}
    for prefix, keys in common_prefixes.items():
        linked_objects = {}
        for key in keys:
            obj = key_to_object[key]
            linked_objects[key] = [
                linked_obj
                for linked_obj in duplicate_hashes[obj['ETag']]
                if linked_obj['Key'] != obj['Key']
            ]
            
        linked_prefixes = [
            linked_obj['CommonPrefix']
            for linked_obj_by_key in linked_objects.values()
            for linked_obj in linked_obj_by_key
            if 'CommonPrefix' in linked_obj
        ]
        count = Counter(linked_prefixes)
        stats = count.most_common()
        duplicate_sets[prefix] = {
            "statistics": stats,
            "keys": keys,
            "objects": [key_to_object[key] for key in keys],
            "duplicates": linked_objects
        }
    return duplicate_sets


def main():
    try:
        import argparse 
    except ImportError:
        print("ERROR: You are running Python < 2.7. Please use pip to install argparse:   pip install argparse")

    parser = argparse.ArgumentParser(add_help=True, description="Display S3 usage by storage tier")
    parser.add_argument("--bucket", required=True, type=str, help="S3 bucket name")
    parser.add_argument("--prefix", type=str, help="S3 bucket prefix", default="")
    parser.add_argument("--delimiter", type=str, help="S3 bucket delimiter", default="/")
    parser.add_argument("--file", "-f", type=str, help="File name to output data to", default="")
    parser.add_argument("--simple", help="If set, then the output will only be duplicates by hash", action='store_true')

    args = parser.parse_args()
    client = boto3.client('s3')

    duplicate_hashes = s3_duplicates(Bucket=args.bucket, Prefix=args.prefix, client=client)
    data = {"duplicates": duplicate_hashes}
    if args.simple:
        for key, value in duplicate_hashes.items():
            print("{}:".format(key))
            for duplicate in value:
                print("  {Key}: LastModified: {LastModified} Size: {Size}".format(**duplicate))
    else:
        # Analyse for patterns
        duplicate_sets = analyse_duplicate_sets(duplicate_hashes, Delimiter=args.delimiter)
        data['sets'] = duplicate_sets
        for prefix, data in duplicate_sets.items():
            sizes = [obj['Size'] for obj in data['objects']]
            print("\"{}\":".format(prefix))
            print("  objects: {}".format(len(data['keys'])))
            print("  size: {}".format(sum(sizes)))
            # print("  Keys:")
            # for key in data['keys']:
            #     print("    - {}".format(key))
            print("  duplicates:")
            for statistic in data['statistics']:
                print("    - {} [{} objects]".format(statistic[0], statistic[1]))
                # for linked_obj in data['duplicates'].values():
                #     for key_obj in linked_obj:
                #         if statistic[0] == key_obj['CommonPrefix']:
                #             print("      - {}".format(key_obj['Key'].replace(statistic[0], "")))

    if args.file:
        with open(args.file, 'w') as f:
            json.dump(data, f, indent=2, default=str)
    


if __name__ == '__main__':
    main()
