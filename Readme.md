# Minimalist E-Commerce Data Warehouse ETL Pipeline

A resilient, standalone transactional data extraction, transformation, and database normalization ecosystem written natively in Python 3. Ingests file variants, filters bad/corrupt transactional parameters, handles multi-currency adjustments, and loads analytical structures into SQLite.

## 🛠️ Step-by-step Quickstart Guide

### 1. Prerequisites
Ensure you have a modern installation configuration of Python 3.9+ running on your shell console.

### 2. Dependency Setup
Dependencies are minimal to enable fast execution on zero-footprint servers. Install the testing components using pip:

```bash
pip install pytest