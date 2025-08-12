#!/usr/bin/env python3
"""
Hosted to ES3 Estimator - Python Version (No External Dependencies)
Uses only built-in Python libraries
"""

import urllib.request
import urllib.parse
import json
import argparse
import os
import sys
from datetime import datetime, timezone, timedelta

class ES3Estimator:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://overview-elastic-cloud-com.es.us-east-1.aws.found.io"

    def _make_api_request(self, endpoint, query_data=None, method='POST'):
        """
        Common method to make API requests to Elastic Cloud
        """
        try:
            # Prepare request data
            if query_data:
                data = json.dumps(query_data).encode('utf-8')
            else:
                data = None
            
            # Create request
            req = urllib.request.Request(
                endpoint,
                data=data,
                headers={
                    'Content-Type': 'application/json',
                    'Authorization': f'ApiKey {self.api_key}'
                },
                method=method
            )
            
            # Make request
            with urllib.request.urlopen(req, timeout=30) as response:
                if response.status != 200:
                    print(f"HTTP Error: {response.status} - {response.reason}")
                    return None
                
                response_data = response.read().decode('utf-8')
                return json.loads(response_data)
                
        except Exception as e:
            print(f"API Request Error: {str(e)} (Type: {type(e).__name__})")
            return None

    def fetch_cluster_environment_data(self, cluster_id):
        """
        Fetch environment data for a specific cluster from logging index
        """
        # Search query to find cluster data
        search_query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "ece.cluster": cluster_id
                            }
                        }
                    ]
                }
            },
            "size": 100,
            "sort": [
                {
                    "@timestamp": {
                        "order": "desc"
                    }
                }
            ],
            "_source": ["environment.*", "attributes.*", "@timestamp", "cluster_uuid"]
        }
        
        endpoint = f"{self.base_url}/logging-*:elasticsearch-2*/_search"
        
        data = self._make_api_request(endpoint, search_query)
        if not data:
            return None
            
        hits = data.get('hits', {}).get('hits', [])
        
        if not hits:
            return None
        
        return self.process_environment_data(hits)
    
    def process_environment_data(self, hits):
        """
        Process environment data from search results
        """
        environment_data = []
        
        for i, hit in enumerate(hits):
            try:
                source = hit.get('_source', {})
                environment = source.get('environment', {})
                attributes = source.get('attributes', {})
                timestamp = source.get('@timestamp')
                
                if environment or attributes:
                    record_data = {
                        'timestamp': timestamp,
                        'environment': environment,
                        'attributes': attributes
                    }
                    environment_data.append(record_data)
                    
            except Exception as e:
                continue
        
        return environment_data if environment_data else None
    
    def analyze_cluster_infrastructure(self, environment_data):
        """
        Analyze cluster infrastructure: availability zones, regions, instance types
        """
        if not environment_data:
            return None
        
        # Collect all infrastructure data from all records
        infrastructure_info = {
            'availability_zones': set(),
            'regions': set(),
            'instance_types': set(),
            'instance_ids': set(),
            'ami_ids': set(),
            'hostnames': set(),
            'cloud_providers': set(),
            'all_fields': set(),
            'field_values': {}
        }
        
        for record in environment_data:
            env = record['environment']
            attrs = record['attributes']
            
            # Search through environment fields
            for key, value in env.items():
                key_lower = key.lower()
                infrastructure_info['all_fields'].add(f"environment.{key}")
                
                # Store all unique values for each field
                field_name = f"environment.{key}"
                if field_name not in infrastructure_info['field_values']:
                    infrastructure_info['field_values'][field_name] = set()
                infrastructure_info['field_values'][field_name].add(str(value))
                
                # Categorize environment fields
                if 'zone' in key_lower or key_lower == 'availability_zone':
                    infrastructure_info['availability_zones'].add(str(value))
                elif 'region' in key_lower:
                    infrastructure_info['regions'].add(str(value))
                elif 'instance-id' in key_lower or 'instanceid' in key_lower:
                    infrastructure_info['instance_ids'].add(str(value))
                elif 'ami-id' in key_lower or 'amiid' in key_lower:
                    infrastructure_info['ami_ids'].add(str(value))
                elif 'hostname' in key_lower:
                    infrastructure_info['hostnames'].add(str(value))
            
            # Search through attributes fields
            for key, value in attrs.items():
                key_lower = key.lower()
                infrastructure_info['all_fields'].add(f"attributes.{key}")
                
                # Store all unique values for each field
                field_name = f"attributes.{key}"
                if field_name not in infrastructure_info['field_values']:
                    infrastructure_info['field_values'][field_name] = set()
                infrastructure_info['field_values'][field_name].add(str(value))
                
                # Categorize attributes fields
                if 'zone' in key_lower or key_lower == 'availability_zone':
                    infrastructure_info['availability_zones'].add(str(value))
                elif 'region' in key_lower:
                    infrastructure_info['regions'].add(str(value))
                elif 'instance_configuration' in key_lower:
                    infrastructure_info['instance_types'].add(str(value))
                elif any(field in key_lower for field in ['provider', 'platform', 'cloud']):
                    infrastructure_info['cloud_providers'].add(str(value))
        
        return infrastructure_info

    def fetch_cluster_stats(self, cluster_id):
        """
        Fetch cluster statistics from metrics index
        """
        # Search query for metrics index
        stats_query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "ece.cluster": cluster_id
                            }
                        },
                        {
                            "range": {
                                "elasticsearch.cluster.stats.indices.docs.total": {
                                    "gt": 0
                                }
                            }
                        }
                    ]
                }
            },
            "size": 1,
            "sort": [
                {
                    "@timestamp": {
                        "order": "desc"
                    }
                }
            ],
            "_source": ["elasticsearch.cluster.stats.*", "@timestamp", "ece.cluster"]
        }
        
        endpoint = f"{self.base_url}/metrics-*:cluster-elasticsearch-*/_search"
        
        data = self._make_api_request(endpoint, stats_query)
        if not data:
            return None
            
        hits = data.get('hits', {}).get('hits', [])
        
        if not hits:
            return None
        
        return self.process_cluster_stats(hits)
    
    def process_cluster_stats(self, hits):
        """
        Process cluster statistics from search results
        """
        stats_data = []
        
        for i, hit in enumerate(hits):
            try:
                source = hit.get('_source', {})
                cluster_stats = source.get('elasticsearch', {}).get('cluster', {}).get('stats', {})
                timestamp = source.get('@timestamp')
                
                # Extract basic stats
                indices_stats = cluster_stats.get('indices', {})
                doc_total = indices_stats.get('docs', {}).get('total')
                storage_bytes = indices_stats.get('store', {}).get('size', {}).get('bytes')
                shards_total = indices_stats.get('shards', {}).get('count')
                shards_primary = indices_stats.get('shards', {}).get('primaries')
                
                if doc_total is not None:
                    record_data = {
                        'timestamp': timestamp,
                        'total_docs': doc_total,
                        'storage_bytes': storage_bytes,
                        'shards_total': shards_total,
                        'shards_primary': shards_primary
                    }
                    stats_data.append(record_data)
                    
            except Exception as e:
                continue
        
        return stats_data if stats_data else None
    
    def analyze_cluster_stats(self, stats_data):
        """
        Analyze cluster document statistics
        """
        if not stats_data:
            return None
        
        # Get latest stats
        latest_stats = stats_data[0]
        total_docs = latest_stats['total_docs']
        storage_bytes = latest_stats['storage_bytes']
        shards_total = latest_stats['shards_total']
        shards_primary = latest_stats['shards_primary']
        latest_timestamp = latest_stats['timestamp']
        
        # Calculate primary vs replica breakdown using shard ratio
        if shards_total and shards_primary and shards_total > 0:
            primary_ratio = shards_primary / shards_total
            replica_ratio = 1 - primary_ratio
            
            primary_docs = int(total_docs * primary_ratio)
            replica_docs = total_docs - primary_docs
            
            if storage_bytes:
                primary_storage_bytes = int(storage_bytes * primary_ratio)
                replica_storage_bytes = storage_bytes - primary_storage_bytes
                primary_storage_gb = primary_storage_bytes / (1024**3)
                replica_storage_gb = replica_storage_bytes / (1024**3)
                total_storage_gb = storage_bytes / (1024**3)
            else:
                primary_storage_bytes = replica_storage_bytes = None
                primary_storage_gb = replica_storage_gb = total_storage_gb = None
        else:
            primary_docs = replica_docs = None
            primary_storage_bytes = replica_storage_bytes = None
            primary_storage_gb = replica_storage_gb = total_storage_gb = None
        
        return {
            'latest_total_docs': total_docs,
            'latest_primary_docs': primary_docs,
            'latest_replica_docs': replica_docs,
            'latest_storage_bytes': storage_bytes,
            'latest_primary_storage_bytes': primary_storage_bytes,
            'latest_replica_storage_bytes': replica_storage_bytes,
            'latest_storage_gb': total_storage_gb,
            'latest_primary_storage_gb': primary_storage_gb,
            'latest_replica_storage_gb': replica_storage_gb,
            'latest_shards_total': shards_total,
            'latest_shards_primary': shards_primary,
            'latest_timestamp': latest_timestamp,
            'primary_ratio': primary_ratio if shards_total else None,
            'historical_data': stats_data
        }

    def fetch_indexing_metrics(self, cluster_id):
        """
        Fetch bulk ingest rate metrics for the last 7 days
        """
        # Calculate 7 days ago timestamp
        now = datetime.now(timezone.utc)
        seven_days_ago = now - timedelta(days=7)
        
        # Query metrics-*:cluster-elasticsearch-* for bulk total size bytes over last 7 days
        indexing_query = {
            "size": 0,
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "ece.cluster": cluster_id
                            }
                        },
                        {
                            "range": {
                                "@timestamp": {
                                    "gte": seven_days_ago.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                                    "lte": now.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                                    "format": "strict_date_optional_time",
                                }
                            }
                        },
                        {
                            "term": {
                                "event.dataset": "elasticsearch.index"
                            }
                        },
                        {
                            "exists": {
                                "field": "elasticsearch.index.total.bulk.total_size_in_bytes"
                            }
                        },
                    ],
                }
            },
            "aggs": {
                "nodes": {
                    "terms": {
                        "field": "ece.instance",
                        "size": 200,
                        "order": {"_count": "desc"},
                    },
                    "aggs": {
                        "timeseries": {
                            "date_histogram": {
                                "field": "@timestamp",
                                "min_doc_count": 0,
                                "time_zone": "UTC",
                                "extended_bounds": {
                                    "min": int(seven_days_ago.timestamp() * 1000),
                                    "max": int(now.timestamp() * 1000),
                                },
                                # 1 hour buckets for smoother derivative
                                "fixed_interval": "3600s",
                            },
                            "aggs": {
                                "bulk_total_max": {
                                    "max": {
                                        "field": "elasticsearch.index.total.bulk.total_size_in_bytes"
                                    }
                                },
                                "bulk_total_deriv": {
                                    "derivative": {
                                        "buckets_path": "bulk_total_max",
                                        "gap_policy": "skip",
                                        # normalize derivative to per-second
                                        "unit": "1s",
                                    }
                                },
                                "bulk_rate_sec": {
                                    "bucket_script": {
                                        "buckets_path": {
                                            "value": "bulk_total_deriv[normalized_value]"
                                        },
                                        "script": {"source": "params.value > 0.0 ? params.value : 0.0", "lang": "painless"},
                                        "gap_policy": "skip",
                                    }
                                },
                            },
                        }
                    },
                }
            },
            "runtime_mappings": {},
        }
        
        endpoint = f"{self.base_url}/metrics-*:cluster-elasticsearch-*/_search"
        
        data = self._make_api_request(endpoint, indexing_query)
        if not data:
            return None
            
        return self.process_indexing_metrics(data)
    
    def process_indexing_metrics(self, data):
        """
        Process bulk ingest metrics and calculate min, max, average rates in bytes/sec and MB/sec
        """
        try:
            nodes_agg = data.get('aggregations', {}).get('nodes', {}).get('buckets', [])
            
            if not nodes_agg:
                return None
            
            # Collect all time buckets from all nodes
            time_buckets = {}
            node_stats = {}
            
            for node_bucket in nodes_agg:
                node_name = node_bucket['key']
                timeseries = node_bucket.get('timeseries', {}).get('buckets', [])
                
                node_rates = []
                for time_bucket in timeseries:
                    timestamp = time_bucket['key']
                    rate_value = time_bucket.get('bulk_rate_sec', {}).get('value')
                    
                    if rate_value is not None and rate_value > 0:
                        node_rates.append(rate_value)
                        
                        # Add to time bucket for cluster total calculation
                        if timestamp not in time_buckets:
                            time_buckets[timestamp] = []
                        time_buckets[timestamp].append(rate_value)
                
                if node_rates:
                    node_stats[node_name] = {
                        'min_rate': min(node_rates),
                        'max_rate': max(node_rates),
                        'avg_rate': sum(node_rates) / len(node_rates),
                        'data_points': len(node_rates)
                    }
            
            # Calculate cluster totals for each time period
            cluster_totals = []
            for timestamp, rates in time_buckets.items():
                if rates:  # Only include time periods with data
                    cluster_total = sum(rates)
                    cluster_totals.append(cluster_total)
            
            if not cluster_totals:
                return None
            
            # Calculate stats in bytes/sec
            min_bps = min(cluster_totals)
            max_bps = max(cluster_totals)
            avg_bps = sum(cluster_totals) / len(cluster_totals)
            
            # Convert to MB/sec
            mib_div = 1024.0 * 1024.0
            min_mbps = min_bps / mib_div
            max_mbps = max_bps / mib_div
            avg_mbps = avg_bps / mib_div
            
            cluster_stats = {
                'min_rate': min_bps,
                'max_rate': max_bps,
                'avg_rate': avg_bps,
                'min_rate_mbps': min_mbps,
                'max_rate_mbps': max_mbps,
                'avg_rate_mbps': avg_mbps,
                'total_data_points': len(cluster_totals),
                'node_count': len(node_stats)
            }
            
            return {
                'cluster_stats': cluster_stats,
                'node_stats': node_stats
            }
            
        except Exception as e:
            return None

    def identify_inactive_nodes(self, cpu_metrics):
        """
        Identify inactive nodes based on CPU usage patterns using statistical analysis
        """
        if not cpu_metrics or 'node_stats' not in cpu_metrics:
            return set()
        
        node_stats = cpu_metrics['node_stats']
        inactive_nodes = set()
        
        if len(node_stats) < 2:
            return set()
        
        # Calculate statistics for all nodes
        all_avg_usages = [stats['avg_usage'] for stats in node_stats.values()]
        all_max_usages = [stats['max_usage'] for stats in node_stats.values()]
        
        # Calculate mean and standard deviation
        mean_avg = sum(all_avg_usages) / len(all_avg_usages)
        mean_max = sum(all_max_usages) / len(all_max_usages)
        
        # Calculate standard deviation
        variance_avg = sum((x - mean_avg) ** 2 for x in all_avg_usages) / len(all_avg_usages)
        std_dev_avg = variance_avg ** 0.5
        
        variance_max = sum((x - mean_max) ** 2 for x in all_max_usages) / len(all_max_usages)
        std_dev_max = variance_max ** 0.5
        
        # Define threshold as 2 standard deviations below the mean
        threshold_avg = mean_avg - (2 * std_dev_avg)
        threshold_max = mean_max - (2 * std_dev_max)
        
        # Ensure minimum reasonable thresholds
        threshold_avg = max(threshold_avg, 1.0)  # At least 1% average
        threshold_max = max(threshold_max, 2.0)  # At least 2% max
        
        for node_name, stats in node_stats.items():
            avg_usage = stats['avg_usage']
            max_usage = stats['max_usage']
            
            # Consider node inactive if both average and max are significantly below the cluster average
            if avg_usage < threshold_avg and max_usage < threshold_max:
                inactive_nodes.add(node_name)
        
        return inactive_nodes

    def fetch_cpu_utilization_metrics(self, cluster_id, inactive_nodes=None):
        """
        Fetch CPU utilization metrics for the last 7 days, excluding inactive nodes
        """
        # Calculate 7 days ago timestamp
        now = datetime.now(timezone.utc)
        seven_days_ago = now - timedelta(days=7)
        
        # Query logging-*:elasticsearch-2* for container CPU usage over last 7 days
        cpu_query = {
            "size": 0,
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "@timestamp": {
                                    "gte": seven_days_ago.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                                    "lte": now.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                                    "format": "strict_date_optional_time"
                                }
                            }
                        },
                        {
                            "bool": {
                                "must": [],
                                "filter": [],
                                "should": [],
                                "must_not": []
                            }
                        },
                        {
                            "bool": {
                                "must": [],
                                "filter": [],
                                "should": [],
                                "must_not": []
                            }
                        }
                    ],
                    "filter": [
                        {
                            "match_phrase": {
                                "ece.cluster": cluster_id
                            }
                        },
                        {
                            "exists": {
                                "field": "ece.cluster"
                            }
                        }
                    ],
                    "should": [],
                    "must_not": []
                }
            },
            "aggs": {
                "nodes": {
                    "terms": {
                        "field": "node_name.keyword",
                        "size": 200,
                        "order": {"_count": "desc"},
                    },
                    "aggs": {
                        "timeseries": {
                            "date_histogram": {
                                "field": "@timestamp",
                                "min_doc_count": 0,
                                "time_zone": "UTC",
                                "extended_bounds": {
                                    "min": int(seven_days_ago.timestamp() * 1000),
                                    "max": int(now.timestamp() * 1000),
                                },
                                # 1 hour buckets for smoother analysis
                                "fixed_interval": "3600s",
                            },
                            "aggs": {
                                "cpu_usage_max": {
                                    "max": {
                                        "field": "container.cpu.usage_in_thousands"
                                    }
                                },
                                "cpu_usage_avg": {
                                    "avg": {
                                        "field": "container.cpu.usage_in_thousands"
                                    }
                                },
                                "cpu_usage_min": {
                                    "min": {
                                        "field": "container.cpu.usage_in_thousands"
                                    }
                                },
                            },
                        }
                    },
                }
            },
            "runtime_mappings": {},
        }
        
        endpoint = f"{self.base_url}/logging-*:elasticsearch-2*/_search"
        
        data = self._make_api_request(endpoint, cpu_query)
        return self.process_cpu_utilization_metrics(data, inactive_nodes) if data else None

    def process_cpu_utilization_metrics(self, data, inactive_nodes=None):
        """
        Process CPU utilization metrics and calculate min, max, average CPU usage percentages
        Excludes inactive nodes from the calculation
        """
        try:
            nodes_agg = data.get('aggregations', {}).get('nodes', {}).get('buckets', [])
            
            if not nodes_agg:
                return None
            
            # Convert inactive_nodes to set for efficient lookup
            inactive_nodes = set(inactive_nodes) if inactive_nodes else set()
            
            # Collect all time buckets from all nodes (excluding inactive nodes)
            time_buckets = {}
            node_stats = {}
            excluded_inactive_nodes = []
            
            for node_bucket in nodes_agg:
                node_name = node_bucket['key']
                
                # Skip inactive nodes
                if node_name in inactive_nodes:
                    excluded_inactive_nodes.append(node_name)
                    continue
                
                timeseries = node_bucket.get('timeseries', {}).get('buckets', [])
                
                node_usage = []
                for time_bucket in timeseries:
                    timestamp = time_bucket['key']
                    avg_usage = time_bucket.get('cpu_usage_avg', {}).get('value')
                    max_usage = time_bucket.get('cpu_usage_max', {}).get('value')
                    min_usage = time_bucket.get('cpu_usage_min', {}).get('value')
                    
                    if avg_usage is not None and avg_usage > 0:
                        # Convert from percentage format (e.g., 680 = 68.0%)
                        avg_usage_pct = avg_usage / 10.0
                        node_usage.append(avg_usage_pct)
                        
                        # Add to time bucket for cluster total calculation
                        if timestamp not in time_buckets:
                            time_buckets[timestamp] = []
                        time_buckets[timestamp].append(avg_usage_pct)
                
                if node_usage:
                    node_stats[node_name] = {
                        'min_usage': min(node_usage),
                        'max_usage': max(node_usage),
                        'avg_usage': sum(node_usage) / len(node_usage),
                        'data_points': len(node_usage)
                    }
            
            # Calculate cluster totals for each time period
            cluster_totals = []
            for timestamp, usage in time_buckets.items():
                if usage:  # Only include time periods with data
                    cluster_avg = sum(usage) / len(usage)  # Average across nodes
                    cluster_totals.append(cluster_avg)
            
            if not cluster_totals:
                return None
            
            # Calculate stats
            min_usage = min(cluster_totals)
            max_usage = max(cluster_totals)
            avg_usage = sum(cluster_totals) / len(cluster_totals)
            
            cluster_stats = {
                'min_usage': min_usage,
                'max_usage': max_usage,
                'avg_usage': avg_usage,
                'total_data_points': len(cluster_totals),
                'node_count': len(node_stats),
                'excluded_inactive_nodes': excluded_inactive_nodes
            }
            
            return {
                'cluster_stats': cluster_stats,
                'node_stats': node_stats
            }
            
        except Exception as e:
            return None

    def fetch_search_metrics(self, cluster_id):
        """
        Fetch search request rate metrics for the last 7 days
        """
        now = datetime.now(timezone.utc)
        seven_days_ago = now - timedelta(days=7)
        
        search_query = {
            "size": 0,
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "@timestamp": {
                                    "gte": seven_days_ago.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                                    "lte": now.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                                    "format": "strict_date_optional_time"
                                }
                            }
                        },
                        {
                            "bool": {
                                "must": [],
                                "filter": [],
                                "should": [],
                                "must_not": []
                            }
                        },
                        {
                            "bool": {
                                "must": [],
                                "filter": [],
                                "should": [],
                                "must_not": []
                            }
                        }
                    ],
                    "filter": [
                        {
                            "match_phrase": {
                                "ece.cluster": cluster_id
                            }
                        },
                        {
                            "exists": {
                                "field": "ece.cluster"
                            }
                        }
                    ],
                    "should": [],
                    "must_not": []
                }
            },
            "aggs": {
                "19c4a3e0-dab3-11ea-867c-914a6de0ed8c": {
                    "terms": {
                        "field": "node_name.keyword",
                        "size": "150",
                        "order": {
                            "_count": "desc"
                        }
                    },
                    "aggs": {
                        "timeseries": {
                            "date_histogram": {
                                "field": "@timestamp",
                                "min_doc_count": 0,
                                "time_zone": "UTC",
                                "extended_bounds": {
                                    "min": int(seven_days_ago.timestamp() * 1000),
                                    "max": int(now.timestamp() * 1000)
                                },
                                "fixed_interval": "30240s"
                            },
                            "aggs": {
                                "4e850600-dd34-11ea-b07c-2b3403cecbb7": {
                                    "max": {
                                        "field": "indices.search.fetch_total"
                                    }
                                },
                                "fa3e6620-dd35-11ea-b07c-2b3403cecbb7": {
                                    "derivative": {
                                        "buckets_path": "4e850600-dd34-11ea-b07c-2b3403cecbb7",
                                        "gap_policy": "skip",
                                        "unit": "1s"
                                    }
                                },
                                "044fe9e0-dd36-11ea-b07c-2b3403cecbb7": {
                                    "bucket_script": {
                                        "buckets_path": {
                                            "value": "fa3e6620-dd35-11ea-b07c-2b3403cecbb7[normalized_value]"
                                        },
                                        "script": {
                                            "source": "params.value > 0.0 ? params.value : 0.0",
                                            "lang": "painless"
                                        },
                                        "gap_policy": "skip"
                                    }
                                }
                            }
                        }
                    },
                    "meta": {
                        "timeField": "@timestamp",
                        "panelId": "61ca57f0-469d-11e7-af02-69e470af7417",
                        "seriesId": "19c4a3e0-dab3-11ea-867c-914a6de0ed8c",
                        "intervalString": "30240s",
                        "indexPatternString": "elasticsearch-2*"
                    }
                }
            },
            "runtime_mappings": {}
        }
        
        endpoint = f"{self.base_url}/logging-*:elasticsearch-2*/_search"
        
        data = self._make_api_request(endpoint, search_query)
        if not data:
            return None
            
        return self.process_search_metrics(data)
    
    def process_search_metrics(self, data):
        """
        Process search metrics and calculate min, max, average rates
        """
        try:
            nodes_agg = data.get('aggregations', {}).get('19c4a3e0-dab3-11ea-867c-914a6de0ed8c', {}).get('buckets', [])
            
            if not nodes_agg:
                return None
            
            # Collect all time buckets from all nodes
            time_buckets = {}
            node_stats = {}
            
            for node_bucket in nodes_agg:
                node_name = node_bucket['key']
                timeseries = node_bucket.get('timeseries', {}).get('buckets', [])
                
                node_rates = []
                for time_bucket in timeseries:
                    timestamp = time_bucket['key']
                    rate_value = time_bucket.get('044fe9e0-dd36-11ea-b07c-2b3403cecbb7', {}).get('value')
                    
                    if rate_value is not None and rate_value > 0:
                        node_rates.append(rate_value)
                        
                        # Add to time bucket for cluster total calculation
                        if timestamp not in time_buckets:
                            time_buckets[timestamp] = []
                        time_buckets[timestamp].append(rate_value)
                
                if node_rates:
                    node_stats[node_name] = {
                        'min_rate': min(node_rates),
                        'max_rate': max(node_rates),
                        'avg_rate': sum(node_rates) / len(node_rates),
                        'data_points': len(node_rates)
                    }
            
            # Calculate cluster totals for each time period
            cluster_totals = []
            for timestamp, rates in time_buckets.items():
                if rates:  # Only include time periods with data
                    cluster_total = sum(rates)
                    cluster_totals.append(cluster_total)
            
            if not cluster_totals:
                return None
            
            cluster_stats = {
                'min_rate': min(cluster_totals),
                'max_rate': max(cluster_totals),
                'avg_rate': sum(cluster_totals) / len(cluster_totals),
                'total_data_points': len(cluster_totals),
                'node_count': len(node_stats)
            }
            
            return {
                'cluster_stats': cluster_stats,
                'node_stats': node_stats
            }
            
        except Exception as e:
            return None

    def fetch_document_size_analysis(self, cluster_id, cluster_stats=None):
        """
        Analyze document size patterns using histogram aggregation on total index data set size
        """
        # Query to get document size distribution using histogram aggregation
        size_query = {
            "size": 0,
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "@timestamp": {
                                    "gte": "now-7d",
                                    "lte": "now",
                                    "format": "strict_date_optional_time"
                                }
                            }
                        }
                    ],
                    "filter": [
                        {
                            "match_phrase": {
                                "ece.cluster": cluster_id
                            }
                        },
                        {
                            "exists": {
                                "field": "ece.cluster"
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "indices": {
                    "terms": {
                        "field": "_index",
                        "size": 20,
                        "order": {
                            "_count": "desc"
                        }
                    },
                    "aggs": {
                        "doc_count": {
                            "value_count": {
                                "field": "_id"
                            }
                        }
                    }
                }
            }
        }
        
        endpoint = f"{self.base_url}/_all/_search"
        
        data = self._make_api_request(endpoint, size_query)
        if not data:
            return None
            
        return self.process_document_size_analysis(data, cluster_stats)

    def process_document_size_analysis(self, data, cluster_stats=None):
        """
        Process document size analysis using index-based aggregation results
        """
        try:
            indices_agg = data.get('aggregations', {}).get('indices', {}).get('buckets', [])

            if not indices_agg:
                return None

            index_stats = {}
            total_docs = 0

            for index_bucket in indices_agg:
                index_name = index_bucket['key']
                doc_count = index_bucket['doc_count']

                if doc_count > 0:
                    index_stats[index_name] = {
                        'doc_count': doc_count
                    }
                    total_docs += doc_count

            if not index_stats:
                return None

            # Calculate average document size based on primary storage and primary document count
            # We should use primary documents only, not including replicas
            if cluster_stats:
                primary_storage_bytes = cluster_stats.get('latest_primary_storage_bytes', 0)
                primary_docs = cluster_stats.get('latest_primary_docs', 0)
            else:
                return None
            
            avg_doc_size_bytes = primary_storage_bytes / primary_docs if primary_docs > 0 else 0
            avg_doc_size_kb = avg_doc_size_bytes / 1024

            # Categorize document size patterns
            if avg_doc_size_kb < 1:
                size_category = "📄 Very Small (< 1KB)"
            elif avg_doc_size_kb < 10:
                size_category = "📄 Small (1-10KB)"
            elif avg_doc_size_kb < 100:
                size_category = "📄 Medium (10-100KB)"
            elif avg_doc_size_kb < 1000:
                size_category = "📄 Large (100KB-1MB)"
            else:
                size_category = "📄 Very Large (> 1MB)"

            return {
                'total_docs': total_docs,
                'avg_size_kb': avg_doc_size_kb,
                'size_category': size_category,
                'index_count': len(index_stats),
                'index_stats': index_stats
            }

        except Exception as e:
            return None

    def fetch_ingest_to_query_ratio(self, cluster_id):
        """
        Fetch the ratio of resources used at ingest time vs. query time using ESQL
        This is critical for ES3 capacity planning
        """
        # Calculate 7 days ago timestamp
        now = datetime.now(timezone.utc)
        seven_days_ago = now - timedelta(days=7)
        
        esql_query = f"""
FROM metrics-*:cluster-elasticsearch-* 
| WHERE ece.cluster:"{cluster_id}" AND event.dataset:"elasticsearch.node.stats" AND @timestamp >= "{seven_days_ago.strftime('%Y-%m-%dT%H:%M:%S.%fZ')}"
| STATS max_node_query_time = MAX(elasticsearch.node.stats.indices.search.fetch_time.ms + elasticsearch.node.stats.indices.search.fetch_time.ms),max_node_index_time = MAX(elasticsearch.node.stats.indices.indexing.index_time.ms) BY elasticsearch.node.name 
| STATS total_query_time = SUM(max_node_query_time), total_index_time = SUM(max_node_index_time) 
| EVAL ingest_ratio = CONCAT(TO_STRING(CEIL(total_index_time::double / total_query_time::double * 100.0)), "%")
| KEEP ingest_ratio
| LIMIT 10
"""
        
        endpoint = f"{self.base_url}/_query"
        
        data = self._make_api_request(endpoint, {"query": esql_query})
        if not data:
            return None
            
        return self.process_ingest_to_query_ratio(data)

    def process_ingest_to_query_ratio(self, data):
        """
        Process the ingest-to-query ratio from ESQL response
        """
        try:
            # ESQL response structure
            columns = data.get('columns', [])
            values = data.get('values', [])
            
            if not values or len(values) == 0:
                return None
            
            # Get the ingest_ratio value from the first row
            ingest_ratio = values[0][0] if values[0] else None
            
            if not ingest_ratio:
                return None
            
            # Extract the numeric value from the percentage string
            # Format is like "150%" - extract the number
            try:
                numeric_ratio = float(ingest_ratio.replace('%', ''))
            except (ValueError, AttributeError):
                return None
            
            return {
                'ingest_ratio': ingest_ratio,
                'numeric_ratio': numeric_ratio,
                'interpretation': self._interpret_ingest_ratio(numeric_ratio)
            }
            
        except Exception as e:
            return None

    def _interpret_ingest_ratio(self, ratio):
        """
        Interpret the ingest-to-query ratio for ES3 capacity planning
        """
        if ratio < 50:
            return "Query-heavy workload - prioritize search performance"
        elif ratio < 100:
            return "Balanced workload - moderate ingest, moderate queries"
        elif ratio < 200:
            return "Ingest-heavy workload - prioritize indexing performance"
        elif ratio < 500:
            return "Very ingest-heavy workload - high indexing throughput needed"
        else:
            return "Extremely ingest-heavy workload - maximum indexing capacity required"

    def fetch_total_cluster_memory(self, cluster_id):
        """
        Fetch total cluster memory using ESQL query
        """
        # Calculate 7 days ago timestamp
        now = datetime.now(timezone.utc)
        seven_days_ago = now - timedelta(days=7)
        
        esql_query = f"""
FROM metrics-*:cluster-elasticsearch-* 
| WHERE ece.cluster:"{cluster_id}" AND event.dataset:"elasticsearch.node.stats" AND ece.elasticsearch_roles: "ingest" AND @timestamp >= "{seven_days_ago.strftime('%Y-%m-%dT%H:%M:%S.%fZ')}"
| STATS max_memory_node = MAX(elasticsearch.node.stats.os.cgroup.memory.limit.bytes::long) BY elasticsearch.node.name 
| STATS total_memory = SUM(max_memory_node)
| EVAL total_memory_gb = total_memory / 1000 / 1000 / 1000
| KEEP total_memory_gb
| LIMIT 10
"""
        
        endpoint = f"{self.base_url}/_query"
        
        data = self._make_api_request(endpoint, {"query": esql_query})
        if not data:
            return None
            
        return self.process_total_cluster_memory(data)

    def process_total_cluster_memory(self, data):
        """
        Process the total cluster memory from ESQL response
        """
        try:
            # ESQL response structure
            columns = data.get('columns', [])
            values = data.get('values', [])
            
            if not values or len(values) == 0:
                return None
            
            # Get the total_memory_gb value from the first row
            total_memory_gb = values[0][0] if values[0] else None
            
            if not total_memory_gb:
                return None
            
            # Convert to float
            try:
                numeric_memory_gb = float(total_memory_gb)
            except (ValueError, AttributeError):
                return None
            
            return {
                'total_memory_gb': total_memory_gb,
                'numeric_memory_gb': numeric_memory_gb
            }
            
        except Exception as e:
            return None

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="ES3 Cost Estimator - Analyze Elasticsearch cluster and estimate ES3 costs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --cluster-id 1a86373f5628470f8841946a610855d9 --api-key your_api_key_here
  %(prog)s --cluster-id 1a86373f5628470f8841946a610855d9 --api-key-file ~/.es_api_key
  
Environment Variables:
  ES_CLUSTER_ID    Elasticsearch cluster ID
  ES_API_KEY       API key for Elastic Cloud access
        """
    )
    
    # Cluster ID (required)
    parser.add_argument(
        '--cluster-id',
        default=os.environ.get('ES_CLUSTER_ID'),
        help='Elasticsearch cluster ID (required, or set ES_CLUSTER_ID env var)'
    )
    
    # API key options (mutually exclusive)
    api_group = parser.add_mutually_exclusive_group()
    api_group.add_argument(
        '--api-key',
        default=os.environ.get('ES_API_KEY'),
        help='API key for Elastic Cloud access (or set ES_API_KEY env var)'
    )
    api_group.add_argument(
        '--api-key-file',
        help='File containing the API key (reads first line, e.g., ~/.es_api_key)'
    )
    
    # Optional settings
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output'
    )
    
    parser.add_argument(
        '--version',
        action='version',
        version='ES3 Cost Estimator v1.0'
    )
    
    return parser.parse_args()

def get_api_key(args):
    """Get API key from arguments, file, or environment"""
    if args.api_key:
        return args.api_key
    elif args.api_key_file:
        try:
            with open(args.api_key_file, 'r') as f:
                api_key = f.readline().strip()
                if not api_key:
                    print("❌ API key file is empty")
                    return None
                return api_key
        except FileNotFoundError:
            print(f"❌ API key file not found: {args.api_key_file}")
            return None
        except Exception as e:
            print(f"❌ Error reading API key file: {e}")
            return None
    else:
        print("❌ No API key provided. Use --api-key, --api-key-file, or set ES_API_KEY environment variable")
        return None

def validate_inputs(cluster_id, api_key):
    """Basic validation of inputs"""
    if not cluster_id:
        print("❌ Cluster ID is required. Use --cluster-id or set ES_CLUSTER_ID environment variable")
        return False
    
    if not api_key:
        print("❌ API key is required")
        return False
    
    # Basic format validation
    if len(cluster_id) < 10:
        print("❌ Cluster ID appears to be invalid (too short)")
        return False
    
    if len(api_key) < 20:
        print("❌ API key appears to be invalid (too short)")
        return False
    
    return True

def main():
    args = parse_arguments()
    
    print("🚀 ES3 Cost Estimator - Production Ready")
    print("="*50)
    
    # Get and validate inputs
    cluster_id = args.cluster_id
    api_key = get_api_key(args)
    
    if not validate_inputs(cluster_id, api_key):
        return 1
    
    if args.verbose:
        print(f"🔧 Configuration:")
        print(f"   Cluster ID: {cluster_id}")
        print(f"   API Key: {'*' * (len(api_key) - 8) + api_key[-8:]}")
        print()
    
    # Initialize estimator
    estimator = ES3Estimator(api_key)
    
    print("📡 Fetching cluster data...")
    # Initialize all variables
    environment_data = None
    cluster_stats = None
    stats_analysis = None
    indexing_metrics = None
    search_metrics = None
    cpu_metrics = None
    search_latency_metrics = None
    document_size_analysis = None
    ingest_to_query_ratio = None
    total_cluster_memory = None
    cost_estimates = None
    
    # Fetch all data silently
    environment_data = estimator.fetch_cluster_environment_data(cluster_id)
    
    if environment_data:
        print("🔧 Analyzing infrastructure...")
        # Analyze infrastructure silently
        infrastructure_info = estimator.analyze_cluster_infrastructure(environment_data)
        
        print("📊 Fetching cluster statistics...")
        # Fetch cluster statistics
        cluster_stats = estimator.fetch_cluster_stats(cluster_id)
        
        if cluster_stats:
            print("⚡ Fetching performance metrics...")
            stats_analysis = estimator.analyze_cluster_stats(cluster_stats)
            
            # Fetch all performance metrics
            indexing_metrics = estimator.fetch_indexing_metrics(cluster_id)
            search_metrics = estimator.fetch_search_metrics(cluster_id)
            
            # Fetch CPU utilization metrics
            cpu_metrics = estimator.fetch_cpu_utilization_metrics(cluster_id)
            
            # Identify inactive nodes and recalculate CPU metrics excluding them
            if cpu_metrics:
                inactive_nodes = estimator.identify_inactive_nodes(cpu_metrics)
                if inactive_nodes:
                    cpu_metrics = estimator.fetch_cpu_utilization_metrics(cluster_id, inactive_nodes)
            
            document_size_analysis = estimator.fetch_document_size_analysis(cluster_id, stats_analysis)
            ingest_to_query_ratio = estimator.fetch_ingest_to_query_ratio(cluster_id)
            total_cluster_memory = estimator.fetch_total_cluster_memory(cluster_id)
        else:
            # Initialize variables if cluster_stats is None
            indexing_metrics = None
            search_metrics = None
            cpu_metrics = None
            document_size_analysis = None
            ingest_to_query_ratio = None
            total_cluster_memory = None
            
            print("💰 Calculating cost estimates...")
            # Generate ES3 cost estimates
            # cost_estimates = estimator.estimate_es3_costs(stats_analysis, infrastructure_info) # This line is removed
    
    print("✅ Analysis complete! Displaying results...")
    # Now display results in organized order
    print("\n" + "="*60)
    print("📊 CLUSTER DOCUMENT STATISTICS")
    print("="*60)
    
    if stats_analysis:
        print(f"📄 Total Documents: {stats_analysis['latest_total_docs']:,}")
        print(f"  └─ Primary: {stats_analysis['latest_primary_docs']:,} docs ({stats_analysis['primary_ratio']:.1%})")
        print(f"  └─ Replica: {stats_analysis['latest_replica_docs']:,} docs ({1-stats_analysis['primary_ratio']:.1%})")
        print(f"💾 Total Storage: {stats_analysis['latest_storage_gb']:.2f} GB")
        print(f"  └─ Primary: {stats_analysis['latest_primary_storage_gb']:.2f} GB")
        print(f"  └─ Replica: {stats_analysis['latest_replica_storage_gb']:.2f} GB")
        print(f"🔗 Total Shards: {stats_analysis['latest_shards_total']:,}")
        print(f"  └─ Primary: {stats_analysis['latest_shards_primary']:,} shards")
        print(f"  └─ Replica: {stats_analysis['latest_shards_total'] - stats_analysis['latest_shards_primary']:,} shards")
        print(f"🕒 Latest Measurement: {stats_analysis['latest_timestamp']}")
    else:
        print("❌ No cluster statistics available")
    
    print("\n" + "="*60)
    print("📈 INGEST PERFORMANCE (Last 7 days)")
    print("="*60)
    
    if indexing_metrics:
        stats = indexing_metrics['cluster_stats']
        
        # Add query configuration description
        print("🔍 Query Configuration:")
        print("  └─ Time range: 7 days (604,800 seconds)")
        print("  └─ Buckets: 168 buckets")
        print("  └─ Bucket duration: 1 hour (3,600 seconds) per bucket")
        print("  └─ Metric: elasticsearch.index.total.bulk.total_size_in_bytes (cumulative)")
        print("  └─ Calculation: Derivative to get bytes/sec rate")
        print("  └─ Aggregation: Max value per bucket, then sum across nodes")
        print("  └─ Data source: metrics-*:cluster-elasticsearch-*")
        
        print(f"📦 Min rate: {stats['min_rate']:.2f} B/s")
        print(f"📦 Max rate: {stats['max_rate']:.2f} B/s")
        print(f"📦 Avg rate: {stats['avg_rate']:.2f} B/s")
        print(f"📊 Data points: {stats['total_data_points']} across {stats['node_count']} nodes")
        
        print(f"💾 Min rate: {stats['min_rate_mbps']:.2f} MB/s")
        print(f"💾 Max rate: {stats['max_rate_mbps']:.2f} MB/s")
        print(f"💾 Avg rate: {stats['avg_rate_mbps']:.2f} MB/s")
        
        # Calculate average to peak ratio
        avg_to_peak_ratio = stats['avg_rate_mbps'] / stats['max_rate_mbps']
        print(f"📊 Avg to Peak ratio: {avg_to_peak_ratio:.3f} ({stats['avg_rate_mbps']:.2f}/{stats['max_rate_mbps']:.2f})")
    else:
        print("❌ No bulk ingest metrics available")
    
    print("\n" + "="*60)
    print("🔍 SEARCH PERFORMANCE (Last 7 days)")
    print("="*60)
    
    if search_metrics:
        stats = search_metrics['cluster_stats']
        
        # Add query configuration description
        print("🔍 Query Configuration:")
        print("  └─ Time range: 7 days (604,800 seconds)")
        print("  └─ Buckets: 20 buckets")
        print("  └─ Bucket duration: 8.4 hours (30,240 seconds) per bucket")
        print("  └─ Metric: indices.search.fetch_total (cumulative count)")
        print("  └─ Calculation: Derivative to get queries/sec rate")
        print("  └─ Aggregation: Max value per bucket, then sum across nodes")
        print("  └─ Data source: All nodes in cluster")
        
        print(f"🔍 Min rate: {stats['min_rate']:.2f} queries/sec")
        print(f"🔍 Max rate: {stats['max_rate']:.2f} queries/sec")
        print(f"🔍 Avg rate: {stats['avg_rate']:.2f} queries/sec")
        print(f"📊 Data points: {stats['total_data_points']} across {stats['node_count']} nodes")
        
        # Calculate average to peak ratio
        avg_to_peak_ratio = stats['avg_rate'] / stats['max_rate']
        print(f"📊 Avg to Peak ratio: {avg_to_peak_ratio:.3f} ({stats['avg_rate']:.2f}/{stats['max_rate']:.2f})")
    else:
        print("❌ No search metrics available")
    
    print("\n" + "="*60)
    print("🖥️  CPU UTILIZATION PERFORMANCE (Last 7 days)")
    print("="*60)
    
    if cpu_metrics:
        stats = cpu_metrics['cluster_stats']
        
        # Add query configuration description
        print("🔍 Query Configuration:")
        print("  └─ Time range: 7 days (604,800 seconds)")
        print("  └─ Buckets: 168 buckets")
        print("  └─ Bucket duration: 1 hour (3,600 seconds) per bucket")
        print("  └─ Metric: container.cpu.usage_in_thousands")
        print("  └─ Calculation: Average usage across nodes per time bucket")
        print("  └─ Aggregation: Average across nodes, then stats across time")
        print("  └─ Data source: logging-*:elasticsearch-2*")
        
        # Show excluded inactive nodes if any
        if stats.get('excluded_inactive_nodes'):
            print(f"  └─ Excluded inactive nodes: {', '.join(stats['excluded_inactive_nodes'])}")
        
        print(f"🖥️  Min usage: {stats['min_usage']:.1f}%")
        print(f"🖥️  Max usage: {stats['max_usage']:.1f}%")
        print(f"🖥️  Avg usage: {stats['avg_usage']:.1f}%")
        print(f"📊 Data points: {stats['total_data_points']} across {stats['node_count']} nodes")
        
        # Calculate average to peak ratio
        avg_to_peak_ratio = stats['avg_usage'] / stats['max_usage']
        print(f"📊 Avg to Peak ratio: {avg_to_peak_ratio:.3f} ({stats['avg_usage']:.1f}%/{stats['max_usage']:.1f}%)")
        
        # Add CPU utilization interpretation
        if stats['avg_usage'] < 30:
            cpu_interpretation = "Low CPU utilization - underutilized resources"
        elif stats['avg_usage'] < 60:
            cpu_interpretation = "Moderate CPU utilization - well-balanced workload"
        elif stats['avg_usage'] < 80:
            cpu_interpretation = "High CPU utilization - consider scaling up"
        else:
            cpu_interpretation = "Very high CPU utilization - immediate scaling recommended"
        
        print(f"💡 CPU Interpretation: {cpu_interpretation}")
    else:
        print("❌ No CPU utilization metrics available")
    
    print("\n" + "="*60)
    print("⚖️  INGEST TO QUERY RATIO")
    print("="*60)
    
    if ingest_to_query_ratio:
        ratio_data = ingest_to_query_ratio
        print("🔍 Query Configuration:")
        print("  └─ Time range: 7 days (NOW() - INTERVAL 7 DAY)")
        print("  └─ Data source: metrics-*:cluster-elasticsearch-*")
        print("  └─ Filter: event.dataset = elasticsearch.node.stats")
        print("  └─ Metrics: elasticsearch.node.stats.indices.indexing.index_time.ms")
        print("  └─ Query metrics: elasticsearch.node.stats.indices.search.fetch_time.ms + query_time.ms")
        print("  └─ Calculation: (total_index_time / total_query_time) * 100")
        print("  └─ Aggregation: Max per node, then sum across cluster")
        
        print(f"⚖️  Ingest to Query Ratio: {ratio_data['ingest_ratio']}")
        print(f"📊 Numeric Ratio: {ratio_data['numeric_ratio']:.1f}")
        print(f"💡 Interpretation: {ratio_data['interpretation']}")
        
        # Calculate Ingest Tier VCUs and cost using average ingest rate
        if total_cluster_memory and total_cluster_memory.get('numeric_memory_gb') and indexing_metrics:
            total_memory_gb = total_cluster_memory['numeric_memory_gb']
            ingest_ratio_percent = ratio_data['numeric_ratio'] / 100.0  # Convert percentage to decimal
            
            # Get average to peak ratio from bulk ingest performance
            indexing_stats = indexing_metrics['cluster_stats']
            avg_to_peak_ratio = indexing_stats['avg_rate_mbps'] / indexing_stats['max_rate_mbps']
            
            # Get CPU utilization factor (use actual average CPU percentage)
            cpu_utilization_factor = 1.0
            if cpu_metrics:
                cpu_stats = cpu_metrics['cluster_stats']
                avg_cpu_usage = cpu_stats['avg_usage']
                # Use actual CPU utilization percentage (e.g., 54.1% = 0.541)
                cpu_utilization_factor = avg_cpu_usage / 100.0 if avg_cpu_usage > 0 else 1.0
            
            # Calculate ingest tier VCUs using average rate and CPU factor: total cluster memory * ingest ratio * avg_to_peak_ratio * cpu_factor
            ingest_tier_vcus = total_memory_gb * ingest_ratio_percent * avg_to_peak_ratio * cpu_utilization_factor
            
            # Cost calculation: $0.14 per VCU per hour
            vcu_hourly_cost = 0.14
            hourly_cost = ingest_tier_vcus * vcu_hourly_cost
            daily_cost = hourly_cost * 24
            monthly_cost = daily_cost * 30
            
            print(f"\n💰 **INGEST TIER ESTIMATION:**")
            print(f"  └─ Total Cluster Memory: {total_memory_gb:.1f} GB")
            print(f"  └─ Ingest Ratio: {ingest_ratio_percent:.3f} ({ratio_data['ingest_ratio']})")
            print(f"  └─ Avg to Peak Ratio: {avg_to_peak_ratio:.3f}")
            print(f"  └─ CPU Utilization Factor: {cpu_utilization_factor:.2f}")
            print(f"  └─ Estimated Ingest VCUs: {ingest_tier_vcus:.1f} VCUs")
            print(f"  └─ VCU Cost: ${vcu_hourly_cost:.2f}/hour")
            print(f"  └─ Hourly Cost: ${hourly_cost:.2f}")
            print(f"  └─ Daily Cost: ${daily_cost:.2f}")
            print(f"  └─ **Monthly Cost: ${monthly_cost:.2f}**")
            print(f"  └─ Note: Includes CPU utilization factor based on {cpu_stats['avg_usage']:.1f}% average CPU usage")
            
            # Calculate Search Tier VCUs and cost using query portion of the ratio
            if search_metrics:
                search_stats = search_metrics['cluster_stats']
                query_ratio_percent = 1.0 - ingest_ratio_percent  # Query portion (76% = 100% - 24%)
                search_avg_to_peak_ratio = search_stats['avg_rate'] / search_stats['max_rate']
                
                # Use the same CPU utilization factor for search tier
                search_cpu_utilization_factor = cpu_utilization_factor
                
                # Calculate search tier VCUs: total cluster memory * query ratio * search avg to peak ratio * cpu_factor
                search_tier_vcus = total_memory_gb * query_ratio_percent * search_avg_to_peak_ratio * search_cpu_utilization_factor
                
                # Cost calculation: $0.14 per VCU per hour
                search_hourly_cost = search_tier_vcus * vcu_hourly_cost
                search_daily_cost = search_hourly_cost * 24
                search_monthly_cost = search_daily_cost * 30
                
                print(f"\n🔍 **SEARCH TIER ESTIMATION:**")
                print(f"  └─ Total Cluster Memory: {total_memory_gb:.1f} GB")
                print(f"  └─ Query Ratio: {query_ratio_percent:.3f} ({query_ratio_percent*100:.1f}%)")
                print(f"  └─ Search Avg to Peak Ratio: {search_avg_to_peak_ratio:.3f}")
                print(f"  └─ CPU Utilization Factor: {search_cpu_utilization_factor:.2f}")
                print(f"  └─ Estimated Search VCUs: {search_tier_vcus:.1f} VCUs")
                print(f"  └─ VCU Cost: ${vcu_hourly_cost:.2f}/hour")
                print(f"  └─ Hourly Cost: ${search_hourly_cost:.2f}")
                print(f"  └─ Daily Cost: ${search_daily_cost:.2f}")
                print(f"  └─ **Monthly Cost: ${search_monthly_cost:.2f}**")
                print(f"  └─ Note: Includes CPU utilization factor based on {cpu_stats['avg_usage']:.1f}% average CPU usage")
                
                # Calculate Storage Tier cost using primary storage
                if stats_analysis and stats_analysis.get('latest_primary_storage_gb'):
                    primary_storage_gb = stats_analysis['latest_primary_storage_gb']
                    storage_cost_per_gb_month = 0.047
                    storage_monthly_cost = primary_storage_gb * storage_cost_per_gb_month
                    
                    print(f"\n💾 **STORAGE TIER ESTIMATION:**")
                    print(f"  └─ Primary Storage: {primary_storage_gb:.1f} GB")
                    print(f"  └─ Storage Cost: ${storage_cost_per_gb_month:.3f}/GB/month")
                    print(f"  └─ **Monthly Cost: ${storage_monthly_cost:.2f}**")
                    
                    # Total cost for all three tiers
                    total_monthly_cost = monthly_cost + search_monthly_cost + storage_monthly_cost
                    print(f"\n💰 **TOTAL MONTHLY COST (Ingest + Search + Storage):**")
                    print(f"  └─ Ingest Tier: ${monthly_cost:.2f}")
                    print(f"  └─ Search Tier: ${search_monthly_cost:.2f}")
                    print(f"  └─ Storage Tier: ${storage_monthly_cost:.2f}")
                    print(f"  └─ **Total: ${total_monthly_cost:.2f}**")
        
        # Add ES3 capacity planning guidance
        print("\n🎯 ES3 Capacity Planning Guidance:")
        if ratio_data['numeric_ratio'] < 50:
            print("  └─ Focus on Search Power for query performance")
            print("  └─ Consider Performant or High-Throughput presets")
        elif ratio_data['numeric_ratio'] < 100:
            print("  └─ Balanced workload - both presets suitable")
            print("  └─ Choose based on latency requirements")
        else:
            print("  └─ Focus on indexing throughput and storage")
            print("  └─ Consider High-Throughput preset for ingest-heavy workloads")
    else:
        print("❌ No ingest-to-query ratio data available")
    
    print("\n" + "="*60)
    print("📊 DOCUMENT SIZE ANALYSIS")
    print("="*60)
    
    if stats_analysis and stats_analysis.get('latest_primary_storage_bytes') and stats_analysis.get('latest_primary_docs'):
        # Calculate average document size from cluster stats
        primary_storage_bytes = stats_analysis['latest_primary_storage_bytes']
        primary_docs = stats_analysis['latest_primary_docs']
        
        if primary_docs > 0:
            avg_doc_size_bytes = primary_storage_bytes / primary_docs
            avg_doc_size_kb = avg_doc_size_bytes / 1024
            
            # Categorize document size patterns
            if avg_doc_size_kb < 1:
                size_category = "📄 Very Small (< 1KB)"
            elif avg_doc_size_kb < 10:
                size_category = "📄 Small (1-10KB)"
            elif avg_doc_size_kb < 100:
                size_category = "📄 Medium (10-100KB)"
            elif avg_doc_size_kb < 1000:
                size_category = "📄 Large (100KB-1MB)"
            else:
                size_category = "📄 Very Large (> 1MB)"
            
            print(f"📄 Total documents: {stats_analysis['latest_total_docs']:,}")
            print(f"📄 Primary documents: {primary_docs:,}")
            print(f"📄 Estimated average size: {avg_doc_size_kb:.2f} KB")
            print(f"📄 Document size category: {size_category}")
            print(f"💾 Primary storage: {stats_analysis['latest_primary_storage_gb']:.2f} GB")
            print(f"📊 Storage efficiency: {avg_doc_size_kb:.2f} KB per document")
            
            # Additional insights based on document size
            if avg_doc_size_kb < 1:
                print(f"💡 **INSIGHT**: Very small documents suggest high-volume logging/monitoring data")
            elif avg_doc_size_kb < 10:
                print(f"💡 **INSIGHT**: Small documents typical of metrics, logs, or structured data")
            elif avg_doc_size_kb < 100:
                print(f"💡 **INSIGHT**: Medium-sized documents common in application data or enriched logs")
            elif avg_doc_size_kb < 1000:
                print(f"💡 **INSIGHT**: Large documents may contain rich content, attachments, or complex objects")
            else:
                print(f"💡 **INSIGHT**: Very large documents suggest binary data, images, or complex documents")
        else:
            print("❌ No valid document count available")
    else:
        print("❌ No document size analysis available")
    
    print("\n" + "="*60)
    print("✅ ANALYSIS SUMMARY")
    print("="*60)
    
    if stats_analysis:
        print(f"📊 Analyzed {len(environment_data)} environment records")
        print(f"📈 Found cluster statistics with {stats_analysis['latest_total_docs']:,} total documents")
        if indexing_metrics:
            avg_rate = indexing_metrics['cluster_stats']['avg_rate']
            print(f"⚡ Average indexing rate: {avg_rate:.2f} docs/sec over last 7 days")
        if search_metrics:
            avg_qps = search_metrics['cluster_stats']['avg_rate']
            print(f"🔍 Average search rate: {avg_qps:.2f} queries/sec over last 7 days")
        if search_latency_metrics:
            avg_latency_ms = search_latency_metrics['cluster_stats']['avg_latency_ms']
            print(f"⏱️  Average search latency: {avg_latency_ms:.2f} ms over last 7 days")
        if document_size_analysis:
            print(f"📄 Average document size: {document_size_analysis['avg_size_kb']:.2f} KB")
        # The cost estimates section is removed, so we cannot display the VCU range here.
    else:
        print("❌ Failed to fetch cluster data")
        print("   Please check your cluster ID and API key")
        return 1
    
    # Check if we got essential data
    if not stats_analysis:
        print("\n❌ Critical Error: Could not fetch essential cluster statistics")
        print("   This may indicate:")
        print("   • Invalid cluster ID")
        print("   • API key lacks sufficient permissions")
        print("   • Cluster is not accessible")
        return 1
    
    print(f"\n🎉 Analysis completed successfully!")
    return 0

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n⚠️  Analysis interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        print("   Please check your inputs and try again")
        sys.exit(1)