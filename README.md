# 🚀 ES3 Cost Estimator

A comprehensive tool for analyzing Elasticsearch clusters and estimating migration costs to Elastic Cloud ES3. This tool provides detailed insights into your cluster's performance, resource utilization, and projected costs.

## ✨ Features

- **📊 Cluster Analysis**: Comprehensive document statistics and storage analysis
- **⚡ Performance Metrics**: Ingest rates, search performance, and CPU utilization
- **💰 Cost Estimation**: Detailed ES3 tier cost calculations (Ingest, Search, Storage)
- **📈 Interactive Dashboard**: Beautiful Streamlit web interface with charts
- **🔧 Production Ready**: Command-line tool with robust error handling
- **📱 Web Interface**: User-friendly web app for easy analysis

## 🎯 What It Analyzes

### Cluster Metrics
- Document counts (primary/replica breakdown)
- Storage utilization (primary/total storage)
- Shard distribution and configuration
- Infrastructure details

### Performance Analysis
- **Ingest Performance**: Bulk indexing rates over time
- **Search Performance**: Query rates and response patterns  
- **CPU Utilization**: Node-level CPU usage with inactive node detection
- **Ingest-to-Query Ratio**: Workload characterization

### Cost Projections
- **Ingest Tier**: VCU requirements based on indexing patterns
- **Search Tier**: VCU requirements based on query patterns
- **Storage Tier**: Primary storage costs
- **Total Monthly Cost**: Complete ES3 migration estimate

## 🚀 Quick Start

### Web Interface (Recommended)

1. **Visit the live app**: [ES3 Cost Estimator](https://your-app-url.streamlit.app) *(coming soon)*

2. **Enter your credentials**:
   - Elastic Cloud Cluster ID
   - API Key with read permissions

3. **Run Analysis** and explore the interactive results!

### Command Line

```bash
# Clone the repository
git clone https://github.com/yourusername/ece-to-es3-estimator.git
cd ece-to-es3-estimator

# Install dependencies
pip install -r requirements.txt

# Run analysis
python es3_estimator.py --cluster-id YOUR_CLUSTER_ID --api-key YOUR_API_KEY
```

## 🔑 Getting Your API Key

1. Go to [Elastic Cloud Console](https://cloud.elastic.co)
2. Navigate to **Stack Management** → **API Keys**
3. Create a new API key with **read permissions**
4. Copy the **encoded** API key (base64 string)

**Required Permissions:**
- Read access to `metrics-*` indices
- Read access to `logging-*` indices

## 📊 Sample Output

```
🚀 ES3 Cost Estimator - Production Ready
==================================================

📊 CLUSTER DOCUMENT STATISTICS
📄 Total Documents: 51,982,995
💾 Total Storage: 820.73 GB
🔗 Total Shards: 218

📈 INGEST PERFORMANCE (Last 7 days)
📦 Avg rate: 5.18 MB/s
📊 Avg to Peak ratio: 0.224

🔍 SEARCH PERFORMANCE (Last 7 days)  
🔍 Avg rate: 605.31 queries/sec
📊 Avg to Peak ratio: 0.780

🖥️ CPU UTILIZATION (Last 7 days)
🖥️ Avg usage: 54.1%
💡 Moderate CPU utilization - well-balanced workload

💰 TOTAL MONTHLY COST ESTIMATE: $13,639.41
  └─ Ingest Tier: $1,134.81
  └─ Search Tier: $12,487.61  
  └─ Storage Tier: $16.99
```

## 🏗️ Architecture

### Data Sources
- **Metrics Index**: `metrics-*:cluster-elasticsearch-*`
- **Logging Index**: `logging-*:elasticsearch-2*`

### Key Calculations
- **Ingest Rate**: Derivative of `elasticsearch.index.total.bulk.total_size_in_bytes`
- **Search Rate**: Derivative of `indices.search.fetch_total`
- **CPU Usage**: Average of `container.cpu.usage_in_thousands`
- **VCU Estimation**: Based on memory, workload ratios, and CPU utilization

## 🛠️ Local Development

### Prerequisites
- Python 3.8+
- Valid Elastic Cloud API key
- Access to Elasticsearch cluster

### Setup
```bash
# Create virtual environment
python -m venv streamlit-env
source streamlit-env/bin/activate  # On Windows: streamlit-env\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run Streamlit app
streamlit run streamlit_app.py

# Run command line tool
python es3_estimator.py --help
```

### Command Line Options
```bash
python es3_estimator.py --help

Options:
  --cluster-id TEXT     Elastic Cloud Cluster ID [required]
  --api-key TEXT        API Key (or use ES_API_KEY env var)
  --api-key-file TEXT   Path to file containing API key
  --verbose             Enable verbose output
  --version             Show version information
```

## 🔍 Troubleshooting

### Common Issues

**401 Unauthorized**
- Verify API key is correct and not expired
- Ensure API key has read permissions for metrics/logging indices
- Check if API key is associated with the correct cluster

**No Data Found**
- Verify cluster ID is correct
- Check if indices `metrics-*` and `logging-*` exist
- Ensure cluster has been running for at least 7 days for historical data

**Connection Errors**
- Verify network connectivity to Elastic Cloud
- Check if base URL is correct for your region

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Built for the Elasticsearch community
- Powered by Streamlit for the web interface
- Uses Elastic Cloud APIs for data collection

---

**⚠️ Important**: This tool provides cost estimates based on historical data and current usage patterns. Actual ES3 costs may vary based on your specific configuration, usage patterns, and Elastic's pricing changes.
