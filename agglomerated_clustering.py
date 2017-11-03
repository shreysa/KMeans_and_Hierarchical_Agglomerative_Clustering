#!/usr/bin/env python3

# Agglomerated Hierarchical Clustering
# Author: Shreysa
# 31 October, 2017
# References: 
# - https://github.com/ZwEin27/Hierarchical-Clustering

import csv
import math
import heapq
import matplotlib
import numpy as np
from pprint import pprint
import matplotlib.pyplot as plt

# MODIFY THIS
INPUT_FILE_NAME = 'data/song_info.csv'

loudness = []
with open(INPUT_FILE_NAME) as csvfile:
    reader = csv.DictReader(csvfile, delimiter=';')
    for row in reader:
        loudness.append(float(row['loudness']))

# MODIFY THIS
input_data = loudness[6000:6500]

def euclidean_distance(pt1, pt2):
    return math.sqrt((pt1 - pt2)**2)

def pairwise_distances(dataset):
    num_entries = len(dataset)
    results = []
    for i in range(num_entries):
        for j in range(i + 1, num_entries):
            dist = euclidean_distance(dataset[i]['data'], dataset[j]['data'])
            results.append((dist, [dist, [[i], [j]]]))
    return results

def compute_centroid(dataset, data_points_index):
    size = len(data_points_index)
    centroid = 0.0
    for idx in data_points_index:
        dim_data = dataset[idx]['data']
        centroid += float(dim_data)
    centroid /= size
    return centroid

def create_dataset(input_data):
    dataset = []
    clusters = {}
    
    id = 0
    for entry in input_data:
        dataset.append({'data': entry, 'id': id})
        cluster_key = str([id])
        clusters.setdefault(cluster_key, {})
        clusters[cluster_key]['centroid'] = entry
        clusters[cluster_key]['elements'] = [id]
        id += 1
    return dataset, clusters

def get_cluster_data(clusters, input_data):
    data = []
    for cluster in clusters.values():
        elements = cluster['elements']
        
        cluster_elements = []
        for elem in elements:
            cluster_elements.append(input_data[elem])
            
        data.append(cluster_elements)
    return data

def plot_data(clusters, input_data):
    plot_data = get_cluster_data(clusters, input_data)
    fig = plt.figure()
    ax1 = fig.add_subplot(111)
    plt.legend(loc='upper left');
    colors = ['r', 'g', 'b']
    for i, cluster_data in enumerate(plot_data):
        x = range(0, len(cluster_data))
        y = cluster_data
        ax1.scatter(x, y, s=10, c=colors[i], label='cluster-{}'.format(i))
    plt.show()    

dataset, clusters = create_dataset(input_data)
distances = pairwise_distances(dataset)
heapq.heapify(distances)
heap = distances
current_clusters = clusters
required_clusters = 3
old_clusters = []
i = 0

while len(current_clusters) > required_clusters:
    print('[{}] Num clusters: {}'.format(i, len(current_clusters))) 
    dist, min_item = heapq.heappop(heap)
    pair_data = min_item[1]
    
    invalid_heap_node = False
    for old_cluster in old_clusters:
        if old_cluster in pair_data:
            invalid_heap_node = True
            break
    
    if invalid_heap_node:
        continue
        
    #print('[{}] cluster: {}, distance: {:f}'.format(i, pair_data, dist))
    
    new_cluster = {}
    new_cluster_elements = sum(pair_data, [])
    new_cluster_centroid = compute_centroid(dataset, new_cluster_elements)
    new_cluster_elements.sort()
    new_cluster.setdefault('centroid', new_cluster_centroid)
    new_cluster.setdefault('elements', new_cluster_elements)
    
    for pair_item in pair_data:
        old_clusters.append(pair_item)
        del current_clusters[str(pair_item)]
        
    # Calculate distances between existing cluster and new cluster
    for ex_cluster in current_clusters.values():
        new_heap_entry = []
        dist = euclidean_distance(ex_cluster['centroid'], new_cluster['centroid'])
        new_heap_entry.append(dist)
        new_heap_entry.append([new_cluster['elements'], ex_cluster['elements']])
        heapq.heappush(heap, (dist, new_heap_entry))
        
    current_clusters[str(new_cluster_elements)] = new_cluster
    i += 1

plot_data(current_clusters, input_data)
