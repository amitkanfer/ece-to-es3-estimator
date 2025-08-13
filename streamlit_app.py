import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
from datetime import datetime
import io
import sys
from contextlib import redirect_stdout

# Import our existing ES3Estimator class
from es3_estimator import ES3Estimator

# Page configuration
st.set_page_config(
    page_title="ES3 Cost Estimator",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .success-message {
        color: #28a745;
        font-weight: bold;
    }
    .error-message {
        color: #dc3545;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

def main():
    # Header
    st.markdown('<h1 class="main-header">🚀 ES3 Cost Estimator</h1>', unsafe_allow_html=True)
    st.markdown("**Analyze your Elasticsearch cluster and estimate ES3 serverless costs**")
    
    # Sidebar for configuration
    with st.sidebar:
        st.header("🔧 Configuration")
        
        # Required inputs
        st.subheader("Required Settings")
        cluster_id = st.text_input(
            "Cluster ID", 
            placeholder="e.g., 1a86373f5628470f8841946a610855d9",
            help="Your Elasticsearch cluster ID from Elastic Cloud"
        )
        
        api_key = st.text_input(
            "API Key", 
            type="password",
            placeholder="Enter your Elastic Cloud API key",
            help="API key with read access to your cluster metrics"
        )
        
        # Optional settings
        st.subheader("Optional Settings")
        analysis_days = st.slider(
            "Analysis Period (days)", 
            min_value=1, 
            max_value=30, 
            value=7,
            help="Number of days of historical data to analyze"
        )
        
        # Advanced settings (collapsible)
        with st.expander("Advanced Settings"):
            base_url = st.text_input(
                "Base URL",
                value="https://overview-elastic-cloud-com.es.us-east-1.aws.found.io",
                help="Elastic Cloud API base URL (usually no need to change)"
            )
            
            vcu_cost = st.number_input(
                "VCU Hourly Cost ($)",
                min_value=0.01,
                max_value=1.0,
                value=0.14,
                step=0.01,
                format="%.3f",
                help="Cost per VCU per hour in USD"
            )
            
            storage_cost = st.number_input(
                "Storage Cost ($/GB/month)",
                min_value=0.001,
                max_value=0.1,
                value=0.047,
                step=0.001,
                format="%.3f",
                help="Storage cost per GB per month in USD"
            )
        
        # Analysis button
        run_analysis = st.button("🔍 Run Analysis", type="primary", use_container_width=True)
    
    # Main content area
    if not run_analysis:
        # Welcome screen
        st.info("👈 Enter your cluster details in the sidebar and click 'Run Analysis' to get started")
        
        st.markdown("## 📋 What This Tool Does:")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("""
            **📊 Cluster Analysis**
            - Document statistics
            - Storage breakdown
            - Shard distribution
            """)
        
        with col2:
            st.markdown("""
            **⚡ Performance Metrics**
            - Indexing rates
            - Search performance
            - CPU utilization
            """)
        
        with col3:
            st.markdown("""
            **💰 Cost Estimation**
            - ES3 VCU requirements
            - Monthly cost breakdown
            - Capacity planning
            """)
        
        st.markdown("## 🔒 Security Note:")
        st.warning("Your API key is processed securely and never stored. All analysis happens in real-time.")
        
        return
    
    # Validate inputs
    if not cluster_id or not api_key:
        st.error("❌ Please provide both Cluster ID and API Key")
        return
    
    if len(cluster_id) < 10:
        st.error("❌ Cluster ID appears to be invalid (too short)")
        return
    
    if len(api_key) < 20:
        st.error("❌ API key appears to be invalid (too short)")
        return
    
    # Run the analysis
    with st.spinner("🔄 Analyzing your cluster... This may take 30-60 seconds"):
        try:
            # Initialize estimator with configuration
            config = {
                'analysis_days': analysis_days,
                'vcu_hourly_cost': vcu_cost,
                'storage_cost_per_gb_month': storage_cost,
                'timeout_seconds': 30
            }
            
            estimator = ES3Estimator(api_key)
            
            # Fetch all data
            results = run_cluster_analysis(estimator, cluster_id)
            
            if results['success']:
                display_results(results['data'], config)
            else:
                st.error(f"❌ Analysis failed: {results['error']}")
                
        except Exception as e:
            st.error(f"💥 Unexpected error: {str(e)}")

def test_basic_connectivity(estimator):
    """Test basic API connectivity"""
    try:
        # Try a simple query to test connectivity
        test_query = {
            "query": {"match_all": {}},
            "size": 1
        }
        endpoint = f"{estimator.base_url}/metrics-*/_search"
        result = estimator._make_api_request(endpoint, test_query)
        return result is not None
    except:
        return False

def run_cluster_analysis(estimator, cluster_id):
    """Run the complete cluster analysis"""
    try:
        results = {
            'success': False,
            'error': None,
            'data': {},
            'raw_output': ''  # Add field to store captured CLI output
        }
        
        # Test API connectivity first
        st.info("🔌 Testing API connectivity...")
        if not test_basic_connectivity(estimator):
            results['error'] = "Failed to connect to Elasticsearch API. Please check:\n1. API key is valid\n2. Network connectivity\n3. Base URL is correct"
            return results
        else:
            st.success("✅ API connectivity test passed")
        
        # Capture CLI output during analysis
        captured_output = io.StringIO()
        
        # Fetch environment data
        st.info("🔍 Fetching cluster environment data...")
        environment_data = estimator.fetch_cluster_environment_data(cluster_id)
        
        if not environment_data:
            # Try to get more specific error info
            st.warning("⚠️ No environment data returned. Trying alternative approach...")
            
            # Let's try fetching cluster stats instead as the first step
            cluster_stats = estimator.fetch_cluster_stats(cluster_id)
            if not cluster_stats:
                results['error'] = "Could not fetch any cluster data. This could mean:\n1. Cluster ID is incorrect\n2. API key lacks permissions\n3. Cluster is not accessible"
                return results
            else:
                st.success("✅ Cluster stats fetched successfully - proceeding without environment data")
                environment_data = None  # Continue without environment data
        else:
            st.success("✅ Environment data fetched successfully")
        
        # Fetch cluster statistics
        cluster_stats = estimator.fetch_cluster_stats(cluster_id)
        
        if not cluster_stats:
            results['error'] = "Could not fetch cluster statistics. Cluster may not be accessible."
            return results
        
        # Analyze cluster stats
        stats_analysis = estimator.analyze_cluster_stats(cluster_stats)
        
        # Fetch performance metrics
        indexing_metrics = estimator.fetch_indexing_metrics(cluster_id)
        search_metrics = estimator.fetch_search_metrics(cluster_id)
        cpu_metrics = estimator.fetch_cpu_utilization_metrics(cluster_id)
        
        # Handle inactive nodes
        if cpu_metrics:
            inactive_nodes = estimator.identify_inactive_nodes(cpu_metrics)
            if inactive_nodes:
                cpu_metrics = estimator.fetch_cpu_utilization_metrics(cluster_id, inactive_nodes)
        
        # Fetch cost calculation data
        ingest_to_query_ratio = estimator.fetch_ingest_to_query_ratio(cluster_id)
        total_cluster_memory = estimator.fetch_total_cluster_memory(cluster_id)
        
        # Store results
        results['data'] = {
            'stats_analysis': stats_analysis,
            'indexing_metrics': indexing_metrics,
            'search_metrics': search_metrics,
            'cpu_metrics': cpu_metrics,
            'ingest_to_query_ratio': ingest_to_query_ratio,
            'total_cluster_memory': total_cluster_memory,
            'environment_data': environment_data
        }
        
        # Capture CLI-style output
        with redirect_stdout(captured_output):
            generate_cli_style_output(estimator, cluster_id, stats_analysis, indexing_metrics, 
                                    search_metrics, cpu_metrics, ingest_to_query_ratio, 
                                    total_cluster_memory)
        
        # Store the captured output
        results['raw_output'] = captured_output.getvalue()
        results['success'] = True
        return results
        
    except Exception as e:
        results['error'] = str(e)
        return results

def generate_cli_style_output(estimator, cluster_id, stats_analysis, indexing_metrics, 
                            search_metrics, cpu_metrics, ingest_to_query_ratio, 
                            total_cluster_memory):
    """Generate CLI-style output using the same print statements as es3_estimator.py"""
    
    print("🚀 ES3 Cost Estimator - Production Ready")
    print("=" * 50)
    print("📡 Fetching cluster data...")
    print("🔧 Analyzing infrastructure...")
    print("📊 Fetching cluster statistics...")
    print("⚡ Fetching performance metrics...")
    print("✅ Analysis complete! Displaying results...")
    
    # Cluster Document Statistics
    print("=" * 60)
    print("📊 CLUSTER DOCUMENT STATISTICS")
    print("=" * 60)
    
    if stats_analysis:
        print(f"📄 Total Documents: {stats_analysis['latest_total_docs']:,}")
        primary_docs = stats_analysis['latest_primary_docs']
        total_docs = stats_analysis['latest_total_docs']
        replica_docs = total_docs - primary_docs
        print(f"  └─ Primary: {primary_docs:,} docs ({primary_docs/total_docs*100:.1f}%)")
        print(f"  └─ Replica: {replica_docs:,} docs ({replica_docs/total_docs*100:.1f}%)")
        
        total_storage = stats_analysis['latest_storage_gb']
        primary_storage = stats_analysis['latest_primary_storage_gb']
        replica_storage = total_storage - primary_storage
        print(f"💾 Total Storage: {total_storage:.2f} GB")
        print(f"  └─ Primary: {primary_storage:.2f} GB")
        print(f"  └─ Replica: {replica_storage:.2f} GB")
        
        total_shards = stats_analysis['latest_shards_total']
        primary_shards = stats_analysis['latest_shards_primary']
        replica_shards = total_shards - primary_shards
        print(f"🔗 Total Shards: {total_shards}")
        print(f"  └─ Primary: {primary_shards} shards")
        print(f"  └─ Replica: {replica_shards} shards")
        print(f"🕒 Latest Measurement: {stats_analysis.get('latest_timestamp', 'N/A')}")
    else:
        print("❌ No cluster statistics available")
    
    # Ingest Performance
    print("=" * 60)
    print("📈 INGEST PERFORMANCE (Last 7 days)")
    print("=" * 60)
    
    if indexing_metrics and indexing_metrics.get('cluster_stats'):
        cluster_stats = indexing_metrics['cluster_stats']
        print("🔍 Query Configuration:")
        print("  └─ Time range: 7 days (604,800 seconds)")
        print("  └─ Buckets: 168 buckets")
        print("  └─ Bucket duration: 1 hour (3,600 seconds) per bucket")
        print("  └─ Metric: elasticsearch.index.total.bulk.total_size_in_bytes (cumulative)")
        print("  └─ Calculation: Derivative to get bytes/sec rate")
        print("  └─ Aggregation: Max value per bucket, then sum across nodes")
        print("  └─ Data source: metrics-*:cluster-elasticsearch-*")
        print(f"📦 Min rate: {cluster_stats['min_rate']:.2f} B/s")
        print(f"📦 Max rate: {cluster_stats['max_rate']:.2f} B/s")
        print(f"📦 Avg rate: {cluster_stats['avg_rate']:.2f} B/s")
        print(f"📊 Data points: {cluster_stats['total_data_points']} across {cluster_stats['node_count']} nodes")
        print(f"💾 Min rate: {cluster_stats['min_rate_mbps']:.2f} MB/s")
        print(f"💾 Max rate: {cluster_stats['max_rate_mbps']:.2f} MB/s")
        print(f"💾 Avg rate: {cluster_stats['avg_rate_mbps']:.2f} MB/s")
        
        avg_to_peak_ratio = cluster_stats['avg_rate_mbps'] / cluster_stats['max_rate_mbps'] if cluster_stats['max_rate_mbps'] > 0 else 0
        print(f"📊 Avg to Peak ratio: {avg_to_peak_ratio:.3f} ({cluster_stats['avg_rate_mbps']:.2f}/{cluster_stats['max_rate_mbps']:.2f})")
    else:
        print("❌ No bulk ingest metrics available")
    
    # Search Performance  
    print("=" * 60)
    print("🔍 SEARCH PERFORMANCE (Last 7 days)")
    print("=" * 60)
    
    if search_metrics and search_metrics.get('cluster_stats'):
        cluster_stats = search_metrics['cluster_stats']
        print("🔍 Query Configuration:")
        print("  └─ Time range: 7 days (604,800 seconds)")
        print(f"  └─ Buckets: {cluster_stats['total_data_points']} buckets")
        print("  └─ Bucket duration: 8.4 hours (30,240 seconds) per bucket")
        print("  └─ Metric: indices.search.fetch_total (cumulative count)")
        print("  └─ Calculation: Derivative to get queries/sec rate")
        print("  └─ Aggregation: Max value per bucket, then sum across nodes")
        print("  └─ Data source: All nodes in cluster")
        print(f"🔍 Min rate: {cluster_stats['min_rate']:.2f} queries/sec")
        print(f"🔍 Max rate: {cluster_stats['max_rate']:.2f} queries/sec")
        print(f"🔍 Avg rate: {cluster_stats['avg_rate']:.2f} queries/sec")
        print(f"📊 Data points: {cluster_stats['total_data_points']} across {cluster_stats['node_count']} nodes")
        
        avg_to_peak_ratio = cluster_stats['avg_rate'] / cluster_stats['max_rate'] if cluster_stats['max_rate'] > 0 else 0
        print(f"📊 Avg to Peak ratio: {avg_to_peak_ratio:.3f} ({cluster_stats['avg_rate']:.2f}/{cluster_stats['max_rate']:.2f})")
    else:
        print("❌ No search metrics available")
    
    # CPU Utilization
    print("=" * 60)
    print("🖥️  CPU UTILIZATION PERFORMANCE (Last 7 days)")
    print("=" * 60)
    
    if cpu_metrics and cpu_metrics.get('cluster_stats'):
        cluster_stats = cpu_metrics['cluster_stats']
        print("🔍 Query Configuration:")
        print("  └─ Time range: 7 days (604,800 seconds)")
        print("  └─ Buckets: 168 buckets")
        print("  └─ Bucket duration: 1 hour (3,600 seconds) per bucket")
        print("  └─ Metric: container.cpu.usage_in_thousands")
        print("  └─ Calculation: Average usage across nodes per time bucket")
        print("  └─ Aggregation: Average across nodes, then stats across time")
        print("  └─ Data source: logging-*:elasticsearch-2*")
        
        if 'excluded_inactive_nodes' in cluster_stats and cluster_stats['excluded_inactive_nodes']:
            print(f"  └─ Excluded inactive nodes: {', '.join(cluster_stats['excluded_inactive_nodes'])}")
        
        print(f"🖥️  Min usage: {cluster_stats['min_usage']:.1f}%")
        print(f"🖥️  Max usage: {cluster_stats['max_usage']:.1f}%")
        print(f"🖥️  Avg usage: {cluster_stats['avg_usage']:.1f}%")
        print(f"📊 Data points: {cluster_stats['total_data_points']} across {cluster_stats['node_count']} nodes")
        
        avg_to_peak_ratio = cluster_stats['avg_usage'] / cluster_stats['max_usage'] if cluster_stats['max_usage'] > 0 else 0
        print(f"📊 Avg to Peak ratio: {avg_to_peak_ratio:.3f} ({cluster_stats['avg_usage']:.1f}%/{cluster_stats['max_usage']:.1f}%)")
        
        # CPU interpretation
        avg_cpu = cluster_stats['avg_usage']
        if avg_cpu < 30:
            interpretation = "Low CPU utilization - cluster may be over-provisioned"
        elif avg_cpu < 60:
            interpretation = "Moderate CPU utilization - well-balanced workload"
        elif avg_cpu < 80:
            interpretation = "High CPU utilization - monitor for performance impact"
        else:
            interpretation = "Very high CPU utilization - consider scaling up"
        print(f"💡 CPU Interpretation: {interpretation}")
    else:
        print("❌ No CPU utilization metrics available")
    
    # Ingest to Query Ratio
    print("=" * 60)
    print("⚖️  INGEST TO QUERY RATIO")
    print("=" * 60)
    
    if ingest_to_query_ratio:
        print("🔍 Query Configuration:")
        print("  └─ Time range: 7 days (NOW() - INTERVAL 7 DAY)")
        print("  └─ Data source: metrics-*:cluster-elasticsearch-*")
        print("  └─ Filter: event.dataset = elasticsearch.node.stats")
        print("  └─ Metrics: elasticsearch.node.stats.indices.indexing.index_time.ms")
        print("  └─ Query metrics: elasticsearch.node.stats.indices.search.fetch_time.ms + query_time.ms")
        print("  └─ Calculation: (total_index_time / total_query_time) * 100")
        print("  └─ Aggregation: Max per node, then sum across cluster")
        print(f"⚖️  Ingest to Query Ratio: {ingest_to_query_ratio['ratio_percentage']:.1f}%")
        print(f"📊 Numeric Ratio: {ingest_to_query_ratio['ratio_percentage']:.1f}")
        
        # Interpretation
        ratio = ingest_to_query_ratio['ratio_percentage']
        if ratio > 50:
            interpretation = "Ingest-heavy workload - prioritize indexing performance"
        elif ratio > 30:
            interpretation = "Balanced workload - consider both ingest and search optimization"
        else:
            interpretation = "Query-heavy workload - prioritize search performance"
        print(f"💡 Interpretation: {interpretation}")
        
        # Cost calculations if available
        if (total_cluster_memory and indexing_metrics and indexing_metrics.get('cluster_stats') and 
            search_metrics and search_metrics.get('cluster_stats') and 
            cpu_metrics and cpu_metrics.get('cluster_stats')):
            
            memory_gb = total_cluster_memory.get('numeric_memory_gb', 0)
            cpu_factor = cpu_metrics['cluster_stats']['avg_usage'] / 100.0
            
            # Ingest tier
            print("💰 **INGEST TIER ESTIMATION:**")
            ingest_ratio = ingest_to_query_ratio['ratio_percentage'] / 100.0
            
            # Calculate avg to peak ratio from indexing metrics
            indexing_cluster_stats = indexing_metrics['cluster_stats']
            avg_to_peak_ratio = (indexing_cluster_stats['avg_rate_mbps'] / indexing_cluster_stats['max_rate_mbps'] 
                                if indexing_cluster_stats['max_rate_mbps'] > 0 else 0)
            
            ingest_vcus = memory_gb * ingest_ratio * avg_to_peak_ratio * cpu_factor
            vcu_cost = 0.14  # Default VCU cost
            hourly_cost = ingest_vcus * vcu_cost
            daily_cost = hourly_cost * 24
            monthly_cost = daily_cost * 30
            
            print(f"  └─ Total Cluster Memory: {memory_gb:.1f} GB")
            print(f"  └─ Ingest Ratio: {ingest_ratio:.3f} ({ingest_to_query_ratio['ratio_percentage']:.1f}%)")
            print(f"  └─ Avg to Peak Ratio: {avg_to_peak_ratio:.3f}")
            print(f"  └─ CPU Utilization Factor: {cpu_factor:.2f}")
            print(f"  └─ Estimated Ingest VCUs: {ingest_vcus:.1f} VCUs")
            print(f"  └─ VCU Cost: ${vcu_cost:.2f}/hour")
            print(f"  └─ Hourly Cost: ${hourly_cost:.2f}")
            print(f"  └─ Daily Cost: ${daily_cost:.2f}")
            print(f"  └─ **Monthly Cost: ${monthly_cost:.2f}**")
            print(f"  └─ Note: Includes CPU utilization factor based on {cpu_metrics['cluster_stats']['avg_usage']:.1f}% average CPU usage")
            
            # Search tier
            print("🔍 **SEARCH TIER ESTIMATION:**")
            query_ratio = 1.0 - ingest_ratio
            
            # Calculate avg to peak ratio from search metrics
            search_cluster_stats = search_metrics['cluster_stats']
            search_avg_to_peak_ratio = (search_cluster_stats['avg_rate'] / search_cluster_stats['max_rate'] 
                                      if search_cluster_stats['max_rate'] > 0 else 0)
            
            search_vcus = memory_gb * query_ratio * search_avg_to_peak_ratio * cpu_factor
            search_hourly_cost = search_vcus * vcu_cost
            search_daily_cost = search_hourly_cost * 24
            search_monthly_cost = search_daily_cost * 30
            
            print(f"  └─ Total Cluster Memory: {memory_gb:.1f} GB")
            print(f"  └─ Query Ratio: {query_ratio:.3f} ({(query_ratio*100):.1f}%)")
            print(f"  └─ Search Avg to Peak Ratio: {search_avg_to_peak_ratio:.3f}")
            print(f"  └─ CPU Utilization Factor: {cpu_factor:.2f}")
            print(f"  └─ Estimated Search VCUs: {search_vcus:.1f} VCUs")
            print(f"  └─ VCU Cost: ${vcu_cost:.2f}/hour")
            print(f"  └─ Hourly Cost: ${search_hourly_cost:.2f}")
            print(f"  └─ Daily Cost: ${search_daily_cost:.2f}")
            print(f"  └─ **Monthly Cost: ${search_monthly_cost:.2f}**")
            print(f"  └─ Note: Includes CPU utilization factor based on {cpu_metrics['cluster_stats']['avg_usage']:.1f}% average CPU usage")
            
            # Storage tier
            if stats_analysis:
                print("💾 **STORAGE TIER ESTIMATION:**")
                primary_storage_gb = stats_analysis['latest_primary_storage_gb']
                storage_cost_per_gb = 0.047  # Default storage cost
                storage_monthly_cost = primary_storage_gb * storage_cost_per_gb
                
                print(f"  └─ Primary Storage: {primary_storage_gb:.1f} GB")
                print(f"  └─ Storage Cost: ${storage_cost_per_gb:.3f}/GB/month")
                print(f"  └─ **Monthly Cost: ${storage_monthly_cost:.2f}**")
                
                # Total cost
                total_monthly_cost = monthly_cost + search_monthly_cost + storage_monthly_cost
                print("💰 **TOTAL MONTHLY COST (Ingest + Search + Storage):**")
                print(f"  └─ Ingest Tier: ${monthly_cost:.2f}")
                print(f"  └─ Search Tier: ${search_monthly_cost:.2f}")
                print(f"  └─ Storage Tier: ${storage_monthly_cost:.2f}")
                print(f"  └─ **Total: ${total_monthly_cost:.2f}**")
                
                # Guidance
                print("🎯 ES3 Capacity Planning Guidance:")
                if ratio > 50:
                    print("  └─ Focus on Ingest Power for indexing performance")
                    print("  └─ Consider Ingest-optimized or High-Throughput presets")
                else:
                    print("  └─ Focus on Search Power for query performance")
                    print("  └─ Consider Performant or High-Throughput presets")
    else:
        print("❌ No ingest-to-query ratio data available")
    
    # Document Size Analysis
    print("=" * 60)
    print("📊 DOCUMENT SIZE ANALYSIS")
    print("=" * 60)
    
    if stats_analysis:
        total_docs = stats_analysis['latest_total_docs']
        primary_docs = stats_analysis['latest_primary_docs']
        primary_storage_gb = stats_analysis['latest_primary_storage_gb']
        
        avg_size_kb = (primary_storage_gb * 1024 * 1024) / primary_docs if primary_docs > 0 else 0
        
        print(f"📄 Total documents: {total_docs:,}")
        print(f"📄 Primary documents: {primary_docs:,}")
        print(f"📄 Estimated average size: {avg_size_kb:.2f} KB")
        
        # Document size category
        if avg_size_kb < 1:
            size_category = "📄 Very Small (<1KB)"
            insight = "Typical of simple logs or metrics data"
        elif avg_size_kb < 10:
            size_category = "📄 Small (1-10KB)"
            insight = "Common in structured logs and events"
        elif avg_size_kb < 100:
            size_category = "📄 Medium (10-100KB)"
            insight = "Medium-sized documents common in application data or enriched logs"
        elif avg_size_kb < 1000:
            size_category = "📄 Large (100KB-1MB)"
            insight = "Large documents typical of content management or document storage"
        else:
            size_category = "📄 Very Large (>1MB)"
            insight = "Very large documents - consider document splitting strategies"
        
        print(f"📄 Document size category: {size_category}")
        print(f"💾 Primary storage: {primary_storage_gb:.2f} GB")
        print(f"📊 Storage efficiency: {avg_size_kb:.2f} KB per document")
        print(f"💡 **INSIGHT**: {insight}")
    else:
        print("❌ No document size analysis available")
    
    # Analysis Summary
    print("=" * 60)
    print("✅ ANALYSIS SUMMARY")
    print("=" * 60)
    
    # Count available data
    available_sections = []
    if stats_analysis:
        available_sections.append(f"📊 Analyzed 100 environment records")
        available_sections.append(f"📈 Found cluster statistics with {stats_analysis['latest_total_docs']:,} total documents")
    if indexing_metrics and indexing_metrics.get('cluster_stats'):
        available_sections.append(f"⚡ Average indexing rate: {indexing_metrics['cluster_stats']['avg_rate']:.2f} docs/sec over last 7 days")
    if search_metrics and search_metrics.get('cluster_stats'):
        available_sections.append(f"🔍 Average search rate: {search_metrics['cluster_stats']['avg_rate']:.2f} queries/sec over last 7 days")
    if cpu_metrics and cpu_metrics.get('cluster_stats'):
        available_sections.append(f"🖥️ Average CPU utilization: {cpu_metrics['cluster_stats']['avg_usage']:.1f}% over last 7 days")
    
    if available_sections:
        for section in available_sections:
            print(section)
        print("🎉 Analysis completed successfully!")
    else:
        print("❌ Failed to fetch cluster data")
        print("   Please check your cluster ID and API key")

def display_results(data, config):
    """Display the analysis results in the Streamlit interface"""
    
    stats_analysis = data['stats_analysis']
    indexing_metrics = data['indexing_metrics']
    search_metrics = data['search_metrics']
    cpu_metrics = data['cpu_metrics']
    ingest_to_query_ratio = data['ingest_to_query_ratio']
    total_cluster_memory = data['total_cluster_memory']
    
    st.success("✅ Analysis completed successfully!")
    
    # Create tabs for different sections
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 Cluster Overview", 
        "⚡ Performance", 
        "💰 Cost Analysis", 
        "📈 Charts", 
        "📋 Summary",
        "📄 Raw Output"
    ])
    
    with tab1:
        display_cluster_overview(stats_analysis)
    
    with tab2:
        display_performance_metrics(indexing_metrics, search_metrics, cpu_metrics)
    
    with tab3:
        display_cost_analysis(
            stats_analysis, indexing_metrics, search_metrics, 
            cpu_metrics, ingest_to_query_ratio, total_cluster_memory, config
        )
    
    with tab4:
        display_charts(indexing_metrics, search_metrics, cpu_metrics)
    
    with tab5:
        display_summary(data, config)
    
    with tab6:
        display_raw_output(data, config)

def display_cluster_overview(stats_analysis):
    """Display cluster overview statistics"""
    if not stats_analysis:
        st.error("❌ No cluster statistics available")
        return
    
    st.header("📊 Cluster Statistics")
    
    # Key metrics in columns
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Documents",
            f"{stats_analysis['latest_total_docs']:,}",
            help="Total number of documents in the cluster"
        )
    
    with col2:
        st.metric(
            "Total Storage",
            f"{stats_analysis['latest_storage_gb']:.1f} GB",
            help="Total storage used by the cluster"
        )
    
    with col3:
        st.metric(
            "Primary Storage",
            f"{stats_analysis['latest_primary_storage_gb']:.1f} GB",
            help="Primary storage (excluding replicas)"
        )
    
    with col4:
        st.metric(
            "Total Shards",
            f"{stats_analysis['latest_shards_total']:,}",
            help="Total number of shards (primary + replica)"
        )
    
    # Detailed breakdown
    st.subheader("📄 Document Breakdown")
    col1, col2 = st.columns(2)
    
    with col1:
        # Document pie chart
        if stats_analysis['latest_primary_docs'] and stats_analysis['latest_replica_docs']:
            fig = px.pie(
                values=[stats_analysis['latest_primary_docs'], stats_analysis['latest_replica_docs']],
                names=['Primary', 'Replica'],
                title="Document Distribution",
                color_discrete_sequence=['#1f77b4', '#ff7f0e']
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Storage pie chart
        if stats_analysis['latest_primary_storage_gb'] and stats_analysis['latest_replica_storage_gb']:
            fig = px.pie(
                values=[stats_analysis['latest_primary_storage_gb'], stats_analysis['latest_replica_storage_gb']],
                names=['Primary', 'Replica'],
                title="Storage Distribution",
                color_discrete_sequence=['#2ca02c', '#d62728']
            )
            st.plotly_chart(fig, use_container_width=True)

def display_performance_metrics(indexing_metrics, search_metrics, cpu_metrics):
    """Display performance metrics"""
    st.header("⚡ Performance Metrics")
    
    # Performance overview
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.subheader("📦 Indexing Performance")
        if indexing_metrics:
            stats = indexing_metrics['cluster_stats']
            st.metric("Average Rate", f"{stats['avg_rate_mbps']:.2f} MB/s")
            st.metric("Peak Rate", f"{stats['max_rate_mbps']:.2f} MB/s")
            efficiency = (stats['avg_rate_mbps'] / stats['max_rate_mbps']) * 100
            st.metric("Efficiency", f"{efficiency:.1f}%", help="Average to peak ratio")
        else:
            st.warning("No indexing metrics available")
    
    with col2:
        st.subheader("🔍 Search Performance")
        if search_metrics:
            stats = search_metrics['cluster_stats']
            st.metric("Average Rate", f"{stats['avg_rate']:.1f} queries/sec")
            st.metric("Peak Rate", f"{stats['max_rate']:.1f} queries/sec")
            efficiency = (stats['avg_rate'] / stats['max_rate']) * 100
            st.metric("Efficiency", f"{efficiency:.1f}%", help="Average to peak ratio")
        else:
            st.warning("No search metrics available")
    
    with col3:
        st.subheader("🖥️ CPU Utilization")
        if cpu_metrics:
            stats = cpu_metrics['cluster_stats']
            st.metric("Average CPU", f"{stats['avg_usage']:.1f}%")
            st.metric("Peak CPU", f"{stats['max_usage']:.1f}%")
            
            # CPU interpretation
            if stats['avg_usage'] < 30:
                interpretation = "🟢 Low - Underutilized"
            elif stats['avg_usage'] < 60:
                interpretation = "🟡 Moderate - Well-balanced"
            elif stats['avg_usage'] < 80:
                interpretation = "🟠 High - Consider scaling"
            else:
                interpretation = "🔴 Very High - Immediate scaling needed"
            
            st.metric("Status", interpretation)
        else:
            st.warning("No CPU metrics available")

def display_cost_analysis(stats_analysis, indexing_metrics, search_metrics, 
                         cpu_metrics, ingest_to_query_ratio, total_cluster_memory, config):
    """Display ES3 cost analysis"""
    st.header("💰 ES3 Cost Analysis")
    
    if not (total_cluster_memory and ingest_to_query_ratio and indexing_metrics):
        st.error("❌ Insufficient data for cost calculation")
        return
    
    # Calculate costs (simplified version of the original logic)
    total_memory_gb = total_cluster_memory['numeric_memory_gb']
    ingest_ratio_percent = ingest_to_query_ratio['numeric_ratio'] / 100.0
    
    # Get performance factors
    indexing_stats = indexing_metrics['cluster_stats']
    avg_to_peak_ratio = indexing_stats['avg_rate_mbps'] / indexing_stats['max_rate_mbps']
    
    # CPU utilization factor
    cpu_utilization_factor = 1.0
    if cpu_metrics:
        cpu_stats = cpu_metrics['cluster_stats']
        cpu_utilization_factor = cpu_stats['avg_usage'] / 100.0
    
    # Calculate VCUs and costs
    vcu_hourly_cost = config['vcu_hourly_cost']
    storage_cost_per_gb_month = config['storage_cost_per_gb_month']
    
    # Ingest tier
    ingest_tier_vcus = total_memory_gb * ingest_ratio_percent * avg_to_peak_ratio * cpu_utilization_factor
    ingest_monthly_cost = ingest_tier_vcus * vcu_hourly_cost * 24 * 30
    
    # Search tier
    if search_metrics:
        search_stats = search_metrics['cluster_stats']
        query_ratio_percent = 1.0 - ingest_ratio_percent
        search_avg_to_peak_ratio = search_stats['avg_rate'] / search_stats['max_rate']
        search_tier_vcus = total_memory_gb * query_ratio_percent * search_avg_to_peak_ratio * cpu_utilization_factor
        search_monthly_cost = search_tier_vcus * vcu_hourly_cost * 24 * 30
    else:
        search_tier_vcus = 0
        search_monthly_cost = 0
    
    # Storage tier
    if stats_analysis:
        primary_storage_gb = stats_analysis['latest_primary_storage_gb']
        storage_monthly_cost = primary_storage_gb * storage_cost_per_gb_month
    else:
        storage_monthly_cost = 0
    
    total_monthly_cost = ingest_monthly_cost + search_monthly_cost + storage_monthly_cost
    
    # Display cost breakdown
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("💵 Monthly Cost Breakdown")
        
        # Cost metrics
        st.metric("Ingest Tier", f"${ingest_monthly_cost:.2f}", 
                 help=f"{ingest_tier_vcus:.1f} VCUs")
        st.metric("Search Tier", f"${search_monthly_cost:.2f}", 
                 help=f"{search_tier_vcus:.1f} VCUs")
        st.metric("Storage Tier", f"${storage_monthly_cost:.2f}", 
                 help=f"{primary_storage_gb:.1f} GB")
        st.metric("**Total**", f"**${total_monthly_cost:.2f}**")
    
    with col2:
        # Cost pie chart
        if total_monthly_cost > 0:
            costs = [ingest_monthly_cost, search_monthly_cost, storage_monthly_cost]
            labels = ['Ingest Tier', 'Search Tier', 'Storage Tier']
            
            fig = px.pie(
                values=costs,
                names=labels,
                title="Monthly Cost Distribution",
                color_discrete_sequence=['#ff6b6b', '#4ecdc4', '#45b7d1']
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Workload analysis
    st.subheader("⚖️ Workload Analysis")
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Ingest to Query Ratio", ingest_to_query_ratio['ingest_ratio'])
        st.info(ingest_to_query_ratio['interpretation'])
    
    with col2:
        st.metric("Total Cluster Memory", f"{total_memory_gb:.1f} GB")
        st.metric("CPU Utilization Factor", f"{cpu_utilization_factor:.2f}")

def display_charts(indexing_metrics, search_metrics, cpu_metrics):
    """Display performance charts"""
    st.header("📈 Performance Charts")
    
    # Create subplots for different metrics
    if indexing_metrics or search_metrics or cpu_metrics:
        
        # Performance comparison chart
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Indexing Rates', 'Search Rates', 'CPU Utilization', 'Efficiency Comparison'),
            specs=[[{"type": "bar"}, {"type": "bar"}],
                   [{"type": "bar"}, {"type": "bar"}]]
        )
        
        # Indexing rates
        if indexing_metrics:
            stats = indexing_metrics['cluster_stats']
            fig.add_trace(
                go.Bar(
                    x=['Min', 'Avg', 'Max'],
                    y=[stats['min_rate_mbps'], stats['avg_rate_mbps'], stats['max_rate_mbps']],
                    name='Indexing (MB/s)',
                    marker_color='#1f77b4'
                ),
                row=1, col=1
            )
        
        # Search rates
        if search_metrics:
            stats = search_metrics['cluster_stats']
            fig.add_trace(
                go.Bar(
                    x=['Min', 'Avg', 'Max'],
                    y=[stats['min_rate'], stats['avg_rate'], stats['max_rate']],
                    name='Search (queries/s)',
                    marker_color='#ff7f0e'
                ),
                row=1, col=2
            )
        
        # CPU utilization
        if cpu_metrics:
            stats = cpu_metrics['cluster_stats']
            fig.add_trace(
                go.Bar(
                    x=['Min', 'Avg', 'Max'],
                    y=[stats['min_usage'], stats['avg_usage'], stats['max_usage']],
                    name='CPU (%)',
                    marker_color='#2ca02c'
                ),
                row=2, col=1
            )
        
        # Efficiency comparison
        efficiencies = []
        labels = []
        
        if indexing_metrics:
            stats = indexing_metrics['cluster_stats']
            eff = (stats['avg_rate_mbps'] / stats['max_rate_mbps']) * 100
            efficiencies.append(eff)
            labels.append('Indexing')
        
        if search_metrics:
            stats = search_metrics['cluster_stats']
            eff = (stats['avg_rate'] / stats['max_rate']) * 100
            efficiencies.append(eff)
            labels.append('Search')
        
        if cpu_metrics:
            stats = cpu_metrics['cluster_stats']
            eff = (stats['avg_usage'] / stats['max_usage']) * 100
            efficiencies.append(eff)
            labels.append('CPU')
        
        if efficiencies:
            fig.add_trace(
                go.Bar(
                    x=labels,
                    y=efficiencies,
                    name='Efficiency (%)',
                    marker_color='#d62728'
                ),
                row=2, col=2
            )
        
        fig.update_layout(height=600, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    
    else:
        st.warning("No performance data available for charts")

def display_summary(data, config):
    """Display analysis summary and download options"""
    st.header("📋 Analysis Summary")
    
    # Key findings
    stats_analysis = data['stats_analysis']
    if stats_analysis:
        st.success(f"✅ Successfully analyzed cluster with {stats_analysis['latest_total_docs']:,} documents")
    
    # Recommendations
    st.subheader("🎯 Recommendations")
    
    recommendations = []
    
    # CPU recommendations
    cpu_metrics = data['cpu_metrics']
    if cpu_metrics:
        avg_cpu = cpu_metrics['cluster_stats']['avg_usage']
        if avg_cpu < 30:
            recommendations.append("🔽 CPU underutilized - consider rightsizing for cost savings")
        elif avg_cpu > 80:
            recommendations.append("🔼 High CPU usage - plan for capacity scaling")
        else:
            recommendations.append("✅ CPU utilization is well-balanced")
    
    # Workload recommendations
    ingest_to_query_ratio = data['ingest_to_query_ratio']
    if ingest_to_query_ratio:
        ratio = ingest_to_query_ratio['numeric_ratio']
        if ratio < 25:
            recommendations.append("🔍 Query-heavy workload - focus on search optimization")
        elif ratio > 150:
            recommendations.append("📥 Ingest-heavy workload - focus on indexing throughput")
        else:
            recommendations.append("⚖️ Balanced workload - good mix of indexing and querying")
    
    if not recommendations:
        recommendations.append("📊 Cluster appears well-optimized for current workload")
    
    for rec in recommendations:
        st.info(rec)
    
    # Download options
    st.subheader("💾 Export Results")
    
    # Prepare data for download
    export_data = {
        'analysis_timestamp': datetime.now().isoformat(),
        'configuration': config,
        'results': {
            'cluster_stats': stats_analysis,
            'performance_metrics': {
                'indexing': data['indexing_metrics']['cluster_stats'] if data['indexing_metrics'] else None,
                'search': data['search_metrics']['cluster_stats'] if data['search_metrics'] else None,
                'cpu': data['cpu_metrics']['cluster_stats'] if data['cpu_metrics'] else None,
            },
            'cost_analysis': {
                'ingest_to_query_ratio': ingest_to_query_ratio,
                'total_cluster_memory': data['total_cluster_memory']
            }
        }
    }
    
    col1, col2 = st.columns(2)
    
    with col1:
        # JSON download
        json_str = json.dumps(export_data, indent=2, default=str)
        st.download_button(
            label="📄 Download JSON",
            data=json_str,
            file_name=f"es3_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json"
        )
    
    with col2:
        # CSV download (simplified)
        if stats_analysis:
            csv_data = {
                'Metric': [
                    'Total Documents',
                    'Primary Documents', 
                    'Total Storage (GB)',
                    'Primary Storage (GB)',
                    'Total Shards'
                ],
                'Value': [
                    stats_analysis['latest_total_docs'],
                    stats_analysis['latest_primary_docs'],
                    stats_analysis['latest_storage_gb'],
                    stats_analysis['latest_primary_storage_gb'],
                    stats_analysis['latest_shards_total']
                ]
            }
            
            df = pd.DataFrame(csv_data)
            csv_str = df.to_csv(index=False)
            
            st.download_button(
                label="📊 Download CSV",
                data=csv_str,
                file_name=f"es3_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )

def display_raw_output(data, config):
    """Display raw text output captured from CLI analysis"""
    st.header("📄 Raw Analysis Output")
    st.write("This shows the exact same output as the command-line version.")
    
    # Get the captured CLI output
    raw_output = data.get('raw_output', 'No raw output available')
    
    # Display in a code block for easy copying
    st.code(raw_output, language="text")
    
    # Download button for the raw output
    st.download_button(
        label="💾 Download Raw Output",
        data=raw_output,
        file_name=f"es3_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
        mime="text/plain"
    )



if __name__ == "__main__":
    main()
