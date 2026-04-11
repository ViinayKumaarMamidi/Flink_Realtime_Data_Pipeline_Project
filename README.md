# Flink_Realtime_Data_Pipeline_Project
This Repo contains details about Flink Real Time processing pipeline with Redpanda, Flink and Postgres Database Table


# Flink Real-time Data Pipeline Project

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
