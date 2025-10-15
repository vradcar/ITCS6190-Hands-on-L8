# Ride Sharing Analytics Using Spark Streaming and Spark SQL.

## Project Report & Results Summary

### Overview
This project implements a real-time analytics pipeline for a ride-sharing platform using Apache Spark Structured Streaming. The pipeline ingests simulated ride data, performs real-time aggregations, and analyzes trends over time. All tasks were successfully executed, and outputs were generated as required.

---

## Explanations, Approach, and Results

### Data Generator
**Approach:**
- The `data_generator.py` script simulates real-time ride-sharing data and streams it to a socket on `localhost:9999`.
- The data includes fields: `trip_id`, `driver_id`, `distance_km`, `fare_amount`, and `timestamp`.

---

### Task 1: Ingestion and Parsing
**Explanation & Approach:**
- Used Spark Structured Streaming to read data from the socket.
- Parsed incoming JSON messages into a DataFrame with the correct schema.
- Wrote each micro-batch as a CSV file in `outputs/task_1/`.

**Results:**
- Each CSV contains raw ride events with all columns present.
- Multiple files confirm successful streaming and batch processing.

---

### Task 2: Real-Time Aggregations
**Explanation & Approach:**
- Reused the parsed DataFrame from Task 1.
- Grouped by `driver_id` to compute total fare and average distance in real time.
- Used `foreachBatch` to write each batch as a CSV in `outputs/task_2/` (due to Spark CSV sink limitations).

**Results:**
- Each batch folder contains a CSV with `driver_id`, `total_fare`, and `avg_distance`.
- Shows real-time aggregation per driver, updating as new data arrives.

---

### Task 3: Windowed Time-Based Analytics
**Explanation & Approach:**
- Converted the timestamp column to a proper TimestampType and added a watermark.
- Used Spark’s window function to perform a 5-minute windowed aggregation (sliding by 1 minute) on `fare_amount`.
- Wrote results to CSV files in `outputs/task_3/`.

**Results:**
- Each CSV contains windowed aggregations with `window_start`, `window_end`, and `total_fare`.
- Results reflect total fare trends over rolling 5-minute windows.

---

### Output Analysis
- **Task 1:**
  - Each CSV in `outputs/task_1/` contains raw, parsed ride events with columns: `trip_id`, `driver_id`, `distance_km`, `fare_amount`, `timestamp`.
  - Multiple files indicate successful streaming ingestion and batch processing.
- **Task 2:**
  - Each batch folder in `outputs/task_2/` contains a CSV with columns: `driver_id`, `total_fare`, `avg_distance`.
  - Shows real-time aggregation per driver, updating as new data arrives.
- **Task 3:**
  - Each CSV in `outputs/task_3/` contains windowed aggregations with columns: `window_start`, `window_end`, `total_fare`.
  - Results reflect total fare trends over rolling 5-minute windows.

### Notes
- All output files are included for transparency and reproducibility.
- Checkpoint folders are omitted from the repository as instructed.
- The project structure and code follow the assignment requirements and best practices for Spark Structured Streaming.

----

## Problems Faced During Execution

- **CSV Sink Output Modes:**
  - Spark Structured Streaming does not support `complete` or `update` output modes for the CSV sink. This required switching to the `foreachBatch` method in Task 2 to write each micro-batch to a separate CSV file.

- **Streaming Environment Warnings:**
  - Several warnings appeared regarding the use of the socket source, Spark UI port binding, and native Hadoop libraries. These are expected in a development environment and did not affect functionality.

- **Windowed Output Delays:**
  - In Task 3, windowed aggregations only produce output after enough data and time have passed, resulting in fewer or delayed output files compared to Tasks 1 and 2.

- **Checkpoints and Output Management:**
  - Care was taken to exclude checkpoint folders from the repository, as they are not needed for reproducibility and can clutter the submission.

---
## **Prerequisites**
Before starting the assignment, ensure you have the following software installed and properly configured on your machine:
1. **Python 3.x**:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. **PySpark**:
   - Install using `pip`:
     ```bash
     pip install pyspark
     ```

3. **Faker**:
   - Install using `pip`:
     ```bash
     pip install faker
     ```

---

## **Setup Instructions**

### **1. Project Structure**

Ensure your project directory follows the structure below:

```
ride-sharing-analytics/
├── outputs/
│   ├── task_1
│   |    └── CSV files of task 1.
|   ├── task_2
│   |    └── CSV files of task 2.
|   └── task_3
│       └── CSV files of task 3.
├── task1.py
├── task2.py
├── task3.py
├── data_generator.py
└── README.md
```

- **data_generator.py/**: generates a constant stream of input data of the schema (trip_id, driver_id, distance_km, fare_amount, timestamp)  
- **outputs/**: CSV files of processed data of each task stored in respective folders.
- **README.md**: Assignment instructions and guidelines.
  
---

### **2. Running the Analysis Tasks**

You can run the analysis tasks either locally.

1. **Execute Each Task **: The data_generator.py should be continuosly running on a terminal. open a new terminal to execute each of the tasks.
   ```bash
     python data_generator.py
     python task1.py
     python task2.py
     python task3.py
   ```

2. **Verify the Outputs**:
   Check the `outputs/` directory for the resulting files:
   ```bash
   ls outputs/
   ```

---

## **Overview**

In this assignment, we will build a real-time analytics pipeline for a ride-sharing platform using Apache Spark Structured Streaming. we will process streaming data, perform real-time aggregations, and analyze trends over time.

## **Objectives**

By the end of this assignment, you should be able to:

1. Task 1: Ingest and parse real-time ride data.
2. Task 2: Perform real-time aggregations on driver earnings and trip distances.
3. Task 3: Analyze trends over time using a sliding time window.

---

## **Task 1: Basic Streaming Ingestion and Parsing**

1. Ingest streaming data from the provided socket (e.g., localhost:9999) using Spark Structured Streaming.
2. Parse the incoming JSON messages into a Spark DataFrame with proper columns (trip_id, driver_id, distance_km, fare_amount, timestamp).

## **Instructions:**
1. Create a Spark session.
2. Use spark.readStream.format("socket") to read from localhost:9999.
3. Parse the JSON payload into columns.
4. Print the parsed data to the console (using .writeStream.format("console")).

---

## **Task 2: Real-Time Aggregations (Driver-Level)**

1. Aggregate the data in real time to answer the following questions:
  • Total fare amount grouped by driver_id.
  • Average distance (distance_km) grouped by driver_id.
2. Output these aggregations to the console in real time.

## **Instructions:**
1. Reuse the parsed DataFrame from Task 1.
2. Group by driver_id and compute:
3. SUM(fare_amount) as total_fare
4. AVG(distance_km) as avg_distance
5. Store the result in csv

---

## **Task 3: Windowed Time-Based Analytics**

1. Convert the timestamp column to a proper TimestampType.
2. Perform a 5-minute windowed aggregation on fare_amount (sliding by 1 minute and watermarking by 1 minute).

## **Instructions:**

1. Convert the string-based timestamp column to a TimestampType column (e.g., event_time).
2. Use Spark’s window function to aggregate over a 5-minute window, sliding by 1 minute, for the sum of fare_amount.
3. Output the windowed results to csv.

---

## 📬 Submission Checklist

- [ ] Python scripts 
- [ ] Output files in the `outputs/` directory  
- [ ] Completed `README.md`  
- [ ] Commit everything to GitHub Classroom  
- [ ] Submit your GitHub repo link on canvas

---

