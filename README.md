# 📈 Real-Time Stock Price ETL Pipeline with Apache Airflow and PostgreSQL

This project is a real-time ETL (Extract, Transform, Load) pipeline that fetches stock prices using Yahoo Finance, computes 7-day moving averages, and loads the processed data into a PostgreSQL database. The entire workflow is orchestrated using Apache Airflow and runs every 15 minutes.

---

## 🧱 Tech Stack

- **Apache Airflow** – DAG orchestration
- **Python** – Core logic
- **yfinance** – Real-time stock price extraction
- **pandas** – Data transformation (moving averages)
- **PostgreSQL** – Structured storage
- **Bash** – Setup and automation scripts
- **(Optional)** Oracle Cloud Free VM – Hosting environment

---

## 🚀 Features

- Real-time scheduled data ingestion
- Moving average computation
- Automatic scheduling & retries via Airflow
- Easy to expand (more tickers, metrics, destinations)

---

## 🧠 Architecture Overview
![image](https://github.com/user-attachments/assets/a03fddc0-e5b5-4213-bb19-114da5c10319)

Airflow DAGs:
![image](https://github.com/user-attachments/assets/2665ce47-bbfe-46e0-8a3e-4353154f07fd)

DAG RUN STATUS:
![image](https://github.com/user-attachments/assets/8036b5dc-20d0-4e13-be6a-d71f81ef50fb)

Data Loaded in Postgres 
![image](https://github.com/user-attachments/assets/9df50206-253c-4728-b536-567f8801126c)


