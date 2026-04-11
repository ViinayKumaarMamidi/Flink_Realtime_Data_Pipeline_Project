# Flink_Realtime_Data_Pipeline_Project
This Repo contains details about Flink Real Time processing pipeline with Redpanda, Flink and Postgres Database Tables

**Data Flow Details**

A comprehensive real-time data processing pipeline built with Apache Flink, Redpanda (Kafka-compatible), and PostgreSQL. This project demonstrates streaming data ingestion, real-time processing, aggregation, and persistent storage.

## 📋 Project Overview

This repository contains a production-ready real-time taxi ride data pipeline that:
- **Produces** real-time taxi ride events to Redpanda (Kafka)
- **Processes** streaming data using Apache Flink with windowing and aggregations
- **Sinks** processed data to PostgreSQL for analytics and reporting
- Handles late-arriving events with proper watermarking strategies

### Architecture Components
- **Redpanda**: Kafka-compatible streaming broker (port 9092)
- **Apache Flink**: Stream processing engine (Cluster: JobManager + TaskManager)
- **PostgreSQL**: Data warehouse for sink tables (port 5432)
- **Python**: Event producer and Flink job implementations

---

## 🚀 Quick Start Guide

### Prerequisites

Ensure you have the following installed:
- **Docker** (v20.10+) and **Docker Compose** (v1.29+)
- **Python** (3.12+) - for running producer scripts locally
- **Git** - for cloning the repository
- **4GB RAM minimum** recommended for Docker containers

### Step 1: Clone the Repository

```bash
git clone https://github.com/ViinayKumaarMamidi/Flink_Realtime_Data_Pipeline_Project.git
cd Flink_Realtime_Data_Pipeline_Project
```

### Step 2: Set Up the Development Environment

#### Option A: Using Docker Compose (Recommended)

Start all services in one command:

```bash
docker-compose up -d
```

This will start:
- Redpanda broker
- PostgreSQL database
- Flink JobManager (accessible at http://localhost:8081)
- Flink TaskManager

Verify all services are running:

```bash
docker-compose ps
```

Expected output should show all 4 services in "running" state.

#### Option B: Local Python Environment Setup

If running the producer locally:

```bash
# Create a virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
# or using the project's pyproject.toml
pip install -e .
```

### Step 3: Verify Database Connection

Connect to PostgreSQL and create necessary tables:

```bash
# Access PostgreSQL container
docker-compose exec postgres psql -U postgres -d postgres

# Inside psql, create tables (see Step 4 for schema details)
```

---

## 📊 Step-by-Step Execution Guide

### Step 4: Create Database Tables

Execute the following SQL commands in PostgreSQL:

```sql
-- Raw rides table (sink from Flink data load job)
CREATE TABLE IF NOT EXISTS rides (
    PULocationID INTEGER,
    DOLocationID INTEGER,
    trip_distance FLOAT,
    total_amount FLOAT,
    tpep_pickup_datetime BIGINT
);

-- Aggregated rides table (sink from Flink aggregation job)
CREATE TABLE IF NOT EXISTS rides_aggregated (
    hour_window TIMESTAMP,
    PULocationID INTEGER,
    trip_count INTEGER,
    avg_distance FLOAT,
    total_revenue FLOAT,
    PRIMARY KEY (hour_window, PULocationID)
);
```

### Step 5: Start Producing Data

Launch the real-time taxi ride producer:

```bash
python producer_realtime.py
```

**What this does:**
- Generates synthetic taxi ride events based on NYC yellow taxi data
- Simulates ~80% on-time and ~20% late events (3-10 seconds delayed)
- Publishes events to Redpanda topic: `rides`
- Sends 2 events per second (configurable via `time.sleep()`)

**Expected output:**
```
Sending events (Ctrl+C to stop)...
--------------------------
  ON TIME   -> PU=79 ts=14:32:45
  ON TIME   -> PU=107 ts=14:32:46
  LATE (5s) -> PU=48 ts=14:32:41
...
```

### Step 6: Deploy Flink Jobs

#### 6A: Data Load Job (Producer → Postgres)

This job reads from Redpanda and writes raw ride data to PostgreSQL:

```bash
docker-compose exec jobmanager flink run \
  -py /opt/flink/usrlib/flink_jobs/data_load.py
```

**What this does:**
- Consumes events from `rides` topic
- Applies minimal transformations
- Writes to `rides` PostgreSQL table
- Provides real-time data visibility

#### 6B: Aggregation Job (Time-windowed Processing)

This job performs 1-hour windowed aggregations:

```bash
docker-compose exec jobmanager flink run \
  -py /opt/flink/usrlib/flink_jobs/aggregation.py
```

**What this does:**
- Groups rides by 1-hour windows and pickup location
- Computes metrics:
  - Trip count per location per hour
  - Average distance traveled
  - Total revenue (sum)
- Writes aggregated results to `rides_aggregated` PostgreSQL table
- Handles late events with 10-second grace period

### Step 7: Monitor Processing in Flink UI

Open your browser and navigate to:

```
http://localhost:8081
```

**What to monitor:**
- **Dashboard**: Overall cluster health and job statistics
- **Job Graph**: Visual representation of task pipeline
- **Task Managers**: Resource utilization and slot allocation
- **Logs**: Troubleshooting and debugging information

---

## 🔍 Validation & Testing

### Validate Data in PostgreSQL

```bash
# Access PostgreSQL
docker-compose exec postgres psql -U postgres -d postgres

# Check raw rides
SELECT COUNT(*) as total_rides FROM rides;
SELECT * FROM rides LIMIT 10;

# Check aggregated data
SELECT * FROM rides_aggregated ORDER BY hour_window DESC LIMIT 10;

# Verify counts
SELECT COUNT(*) as raw_count, COUNT(DISTINCT PULocationID) as unique_locations 
FROM rides;
```

### Monitor Redpanda Topics

```bash
# Check topic details
docker-compose exec redpanda rpk topic list

# Consume messages from the rides topic
docker-compose exec redpanda rpk topic consume rides
```

### Check Flink Job Status

From the Flink UI (http://localhost:8081):
- Verify jobs are in "RUNNING" state
- Monitor throughput and backpressure
- Review task logs for errors

---

## 📁 Project Structure

```
Flink_Realtime_Data_Pipeline_Project/
├── docker-compose.yaml           # Multi-container orchestration
├── Dockerfile.flink              # Flink container with dependencies
├── flink-config.yaml             # Flink cluster configuration
├── pyproject.toml                # Python dependencies
├── pyproject.flink.toml          # Flink-specific Python config
├── producer_realtime.py          # Event producer script
├── models.py                     # Data models (Ride dataclass)
├── main.py                       # Entry point
├── consumer.ipynb                # Jupyter notebook: data consumption
├── consumer_db.ipynb             # Jupyter notebook: DB validation
├── producer.ipynb                # Jupyter notebook: producer exploration
├── flink_jobs/                   # Flink job implementations
│   ├── data_load.py              # Raw data load job
│   └── aggregation.py            # Windowed aggregation job
└── README.md                     # This file
```

---

## 🛠️ Configuration Guide

### Flink Configuration (flink-config.yaml)

Key settings you can adjust:

```yaml
# JobManager Configuration
jobmanager.memory.process.size: 1600m  # Increase for large datasets
jobmanager.rpc.address: jobmanager

# TaskManager Configuration
taskmanager.memory.process.size: 1728m
taskmanager.numberOfTaskSlots: 15      # Parallelism
parallelism.default: 3                 # Default parallelism

# State Backend (for checkpointing)
state.backend: rocksdb
state.checkpoints.dir: file:///opt/flink/checkpoints
```

### Producer Configuration (producer_realtime.py)

Customize data generation:

```python
# Line 72: Kafka bootstrap server
server = 'localhost:9092'  # Change if using remote Kafka

# Line 80: Topic name
topic_name = 'rides'  # Use different topic if needed

# Line 102: Event frequency
time.sleep(0.5)  # Adjust: lower = faster, higher = slower
```

---

## 🐛 Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| Port 9092 already in use | Change Redpanda port in docker-compose.yaml or kill the process: `lsof -ti:9092 \| xargs kill -9` |
| Flink jobs fail to start | Check logs: `docker-compose logs jobmanager` |
| PostgreSQL connection refused | Ensure postgres container is running: `docker-compose logs postgres` |
| Data not flowing to Postgres | Verify Flink job status in UI; check database credentials in job config |
| Memory errors | Increase memory in docker-compose.yaml: `taskmanager.memory.process.size: 2048m` |

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f jobmanager

# Real-time logs (last 50 lines)
docker-compose logs --tail=50 -f
```

---

## 📈 Performance Tuning

### For High Throughput

1. Increase parallelism in `flink-config.yaml`:
   ```yaml
   parallelism.default: 6  # From 3
   ```

2. Increase producer event rate:
   ```python
   time.sleep(0.1)  # From 0.5
   ```

3. Increase TaskManager resources:
   ```yaml
   taskmanager.numberOfTaskSlots: 30  # From 15
   ```

### For Late Data Handling

Adjust grace period in aggregation job (default: 10 seconds):
```python
# In aggregation.py
.allowedLateness(Time.seconds(30))  # Increase if needed
```

---

## 🔒 Security Considerations

- **Redpanda**: Currently running without authentication (development only)
- **PostgreSQL**: Using default credentials (change in production)
- **Flink**: No authentication enabled (configure RBAC for production)

For production deployment:
- Enable Kerberos/SAML authentication
- Use SSL/TLS for all connections
- Rotate database credentials regularly
- Implement network segmentation

---

## 📚 Additional Resources

### Notebooks for Exploration

- **`producer.ipynb`**: Explore and test the producer logic interactively
- **`consumer.ipynb`**: Consumer patterns and topic exploration
- **`consumer_db.ipynb`**: Database query examples and validations

### Documentation

- [Apache Flink Documentation](https://nightlies.apache.org/flink/flink-docs-release-2.2/)
- [Redpanda Documentation](https://docs.redpanda.com/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📝 License

This project is open source. See the LICENSE file for details.

---

## 🎯 Next Steps

After completing the setup:

1. ✅ Experiment with different window sizes in aggregation job
2. ✅ Add custom metrics and monitoring
3. ✅ Implement checkpointing for fault tolerance
4. ✅ Scale up with larger producer event rates
5. ✅ Deploy to Kubernetes for production use

---

## 💡 Tips & Best Practices

- **Monitor backpressure**: Use Flink UI to ensure tasks aren't falling behind
- **Set checkpoints**: Enable state checkpointing for recovery (see flink-config.yaml)
- **Test late events**: The producer generates 20% late events by design; this tests your watermarking
- **Use Jupyter notebooks**: Great for exploratory data analysis before building jobs
- **Review logs frequently**: Docker logs reveal most issues before they become problems

---

**Last Updated**: April 2026  
**Author**: ViinayKumaarMamidi
```

---

## 📌 How to Use This Guide

To implement this README in your repository:

1. **Copy the content above** into your `README.md` file
2. **Push to GitHub** using:
   ```bash
   git add README.md
   git commit -m "docs: Add comprehensive step-by-step README guide"
   git push origin main
   ```

This guide provides:
- ✅ Prerequisites and environment setup
- ✅ Step-by-step execution instructions
- ✅ Data validation procedures
- ✅ Configuration options
- ✅ Troubleshooting guide
- ✅ Performance tuning recommendations
- ✅ Architecture overview
- ✅ Project structure documentation

The guide is beginner-friendly while providing depth for advanced users!
