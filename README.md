# Financial Investment Analysis Pipeline

## Project Overview
This project implements a robust financial data analysis pipeline designed to process, analyze, and visualize investment data from an augmented financial dataset. It helps uncover insights about investment preferences across various demographics, such as age, gender, and risk profiles, providing clear visual dashboards for better decision-making.

## Purpose
The purpose of this project is to develop a scalable ETL (Extract, Transform, Load) pipeline using Apache Spark and pandas to process large financial datasets efficiently. The processed data is then visualized using Matplotlib and Seaborn to aid financial analysts and investors in understanding investment patterns.

## Technologies and Tools Used
- **Python 3.x**: Programming language used to develop the pipeline.
- **Apache Spark (PySpark)**: For scalable ETL processing of large datasets.
- **pandas**: Fallback library for ETL and data manipulation for smaller datasets or limited systems.
- **Matplotlib**: For creating static, animated, and interactive visualizations.
- **Seaborn**: Based on Matplotlib, it provides a high-level interface for drawing attractive and informative statistical graphics.
- **NumPy**: For numerical computations and handling of arrays.

## Project Structure

| File                | Description                                                            |
|---------------------|------------------------------------------------------------------------|
| `etl.py`            | ETL pipeline for data cleaning, transformation, and aggregation         |
| `visualization.py`  | Visualization functions to generate charts and dashboards              |
| `run.py`            | Main script that runs the ETL and visualization workflows               |
| `requirements.txt`  | Required dependencies for the project                                  |
| `README.md`         | Project guide and documentation                                         |

## Python Library Installation

Install the required Python libraries using:

```sh
pip install -r requirements.txt
````

### Required libraries:

* `pyspark`
* `pandas`
* `matplotlib`
* `seaborn`
* `numpy`

## Installation and Execution Steps

1. **Ensure Python 3.x is installed.**

2. **Install Java (version 8 or above)** and configure the `JAVA_HOME` environment variable for PySpark support.

3. **Create and activate a virtual environment:**

   ```sh
   python -m venv venv
   venv\Scripts\activate   # Windows
   source venv/bin/activate  # macOS/Linux
   ```

4. **Install dependencies:**

   ```sh
   pip install -r requirements.txt
   ```

5. **Place the augmented financial dataset CSV** (e.g., `Finance_data_augmented.csv`) in the project directory.

6. **Run the pipeline:**

   ```sh
   python run.py
   ```

## Key Features

* **Efficient large-scale data handling** using Apache Spark (for large datasets) with a fallback to pandas for smaller datasets or limited systems.
* **Automated and optimized visual dashboards** that include:

  * Bar charts
  * Comprehensive multi-chart dashboards
  * Clean single-page dashboards with improved spacing
* **Aggregations by**:

  * Investment avenues
  * Age groups
  * Gender categories
  * Risk behavior factors

## Notes

* **Apache Spark** is required for handling large datasets, but the project works without Spark through pandas fallback for smaller datasets or less powerful systems.
* **Visual dashboards** are optimized for readability, with a focus on clear and accessible presentation of data.
* **Ensure Java installation is functional** if Spark ETL is used.



---
## Project Explaination

This project implements a Financial Investment Analysis Pipeline that systematically extracts, transforms, analyzes, and visualizes investment-related data to provide actionable insights. The execution begins with loading the augmented financial dataset (Finance_data_augmented.csv) using either Apache Spark for large-scale processing or pandas for smaller datasets. The ETL pipeline (etl.py) performs data cleaning, handling missing values, transforming categorical variables, and aggregating data across key dimensions such as age groups, gender, risk profiles, and investment avenues. Once the data is processed, the visualization module (visualization.py) generates multiple dashboards and charts—including bar charts, single-page clean dashboards, and comprehensive multi-chart dashboards—using Matplotlib and Seaborn, highlighting investment trends and demographic patterns. The entire workflow is orchestrated through the main script (run.py), which ensures the ETL and visualization steps execute sequentially. The project supports efficient large-scale computation with Spark, while maintaining flexibility for smaller datasets with pandas, and produces optimized, reader-friendly visual outputs to aid financial analysts and investors in understanding patterns, making informed decisions, and identifying investment preferences across different demographics.