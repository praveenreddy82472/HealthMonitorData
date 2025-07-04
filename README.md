# HealthMonitorData

The Beginning — Why This Project?
In modern healthcare, data is everywhere — from patient admission records to live vitals streaming from sensors. But the real challenge is not just collecting data; it’s making sense of it in real time to support timely decisions.

I embarked on this project to simulate a scalable health monitoring system that could handle both historical hospital data and real-time patient vitals. The goal? To create a platform where doctors could get insights fast, backed by machine learning predictions — all on a cloud-native infrastructure.

The Challenge — Bridging Batch and Streaming Worlds
Healthcare data comes in many shapes and speeds:

Batch data like patient info, costs, and outcomes stored in CSV files.

Streaming data like temperature, blood pressure, and heart rate coming live from sensors.

How do you unify these two worlds? How do you process streaming and batch data without losing consistency or causing bottlenecks?

The Approach — Taking the GCP Route
I chose Google Cloud Platform for its rich data ecosystem:

Cloud Storage to hold batch CSV files.

Cloud Spanner for reliable transactional batch data storage.

Pub/Sub as a messaging backbone for streaming vitals.

Bigtable optimized for fast writes and queries on time-series sensor data.

Dataflow to orchestrate and process both batch and streaming pipelines.

BigQuery as the analytics hub where batch and stream data unite.

BigQuery ML to build predictive models right inside the warehouse.

Looker Studio for easy-to-build, real-time dashboards.

The Journey — Lessons Learned and Obstacles Overcome
Complexity in Joining Data
Initially, I tried joining batch and stream data directly inside Dataflow using Apache Beam’s GroupByKey. It failed because global window joins on unbounded data are tricky and error-prone.
Solution: I wrote batch and streaming data separately to different BigQuery tables, then joined them in BigQuery itself — this made things simple and scalable.

Handling Nested Data
Blood pressure was a single field with two numbers (systolic/diastolic). Feeding it raw to ML models caused confusion.
Solution: I parsed and split these into two separate numeric columns, which improved model quality drastically.

Vertex AI Training Failures
I wanted to leverage Vertex AI for automated training but repeatedly ran into quota and configuration issues that blocked success.
Solution: I switched to BigQuery ML, which provided fast, integrated, and reliable model training within BigQuery, bypassing the complexity of external training.

Data Schema and Consistency
Ensuring all data schemas matched across batch and stream sources took careful planning and incremental debugging.

The Outcome — What We Achieved
By the end of this project:

The system can process batch hospital data and real-time patient vitals simultaneously.

A unified patient view is created in BigQuery by joining batch and streaming tables.

A logistic regression model predicts patient recovery outcomes with near-perfect accuracy.

A dashboard shows live insights to healthcare staff, enabling timely interventions.

