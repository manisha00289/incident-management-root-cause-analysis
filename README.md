# Incident Management & Root Cause Analysis Platform

**A Python-based incident analytics project featuring application log processing, incident detection, HDFS benchmark log analysis, and experimental machine learning-based anomaly detection. The project aims to evolve into an evidence-driven incident investigation platform.**

![Python](https://img.shields.io/badge/Python-3.14-blue)
![Machine Learning](https://img.shields.io/badge/Machine%20Learning-Scikit--learn-orange)
![Testing](https://img.shields.io/badge/Tests-32%20Passing-success)
![License](https://img.shields.io/badge/License-MIT-green)
![Status](https://img.shields.io/badge/Project-In%20Development-yellow)

## Project Overview

Modern distributed systems generate millions of log records across applications, databases, servers, and data pipelines. Identifying abnormal behavior and investigating incidents across these logs can be time-consuming and complex.

The **Incident Management & Root Cause Analysis Platform** explores how data engineering, log analytics, machine learning, and incident management can help engineering teams identify and investigate operational issues.

The current implementation consists of two independent Python-based workflows:

- An application log incident detection pipeline with JSON reporting and SQLite persistence.
- An HDFS benchmark log processing and experimental machine learning anomaly detection pipeline.

The long-term goal is to bring these capabilities together into a unified incident investigation platform.

## Problem Statement

Engineering teams often face challenges such as:

- Processing large volumes of application and infrastructure logs.
- Identifying abnormal system behavior.
- Correlating related log events.
- Understanding the evidence behind detected anomalies.
- Tracking incidents and their investigation status.

This project explores how data engineering and machine learning techniques can help address these challenges.

## Architecture

The project currently contains two independently implemented workflows.

```text
             INCIDENT MANAGEMENT PROJECT
                         |
          +--------------+--------------+
          |                             |
          v                             v
  WORKFLOW 1:                    WORKFLOW 2:
  APPLICATION LOGS               HDFS LOG ANALYTICS
          |                             |
          v                             v
  Python Log Parser              HDFS Log Parser
          |                             |
          v                             v
  Error Grouping                 Event Template Matching
          |                             |
          v                             v
  Severity Assignment            Block Feature Aggregation
          |                             |
          v                             v
  JSON + SQLite                  Dataset Preparation
  (Implemented)                        |
                                        v
                                 Random Forest Model
                                 (Experimental)

          FUTURE INTEGRATION
                  |
                  v
       Evidence-Based Investigation
                  |
                  v
          REST API / Dashboard
                  |
                  v
            AWS Deployment
```

The two implemented workflows currently operate independently. Integration into a unified incident investigation platform is part of the development roadmap.

## Technology Stack

| Category | Technologies | Status |
|---|---|---|
| Programming | Python 3.14 | Implemented |
| Data Processing | Python, CSV, NumPy | Implemented |
| Machine Learning | Scikit-learn, Random Forest | Implemented |
| Data Validation | NumPy, CSV-based validation | Implemented |
| Database | SQLite | Implemented |
| Testing | Pytest | Implemented |
| Version Control | Git, GitHub | Implemented |
| Distributed Processing | Apache Spark, PySpark | Planned |
| Cloud Storage | Amazon S3 | Planned |
| Cloud ETL | AWS Glue | Planned |
| Cloud Monitoring | Amazon CloudWatch | Planned |
| Containerization | Docker | Planned |
| CI/CD | GitHub Actions | Planned |
| API Development | FastAPI | Future Enhancement |
| Dashboard | Streamlit | Future Enhancement |

## Implemented Features

### 1. Incident Detection

- Parses structured application log records.
- Groups repeated errors by pipeline and error code.
- Identifies recurring errors.
- Assigns incident severity.
- Generates incident reports in JSON format.

### 2. SQLite Persistence

- Stores incident information in SQLite.
- Maintains incident identifiers, severity, status, and timestamps.
- Persists supporting evidence.

### 3. HDFS Log Processing

Uses the publicly available HDFS benchmark dataset to explore large-scale log analytics.

Implemented components:

- HDFS log parser.
- Event template matching.
- Block-level event aggregation.
- Event frequency feature extraction.
- Dataset validation and preparation.

### 4. Machine Learning Anomaly Detection

A Random Forest classifier has been implemented using Scikit-learn to identify anomalous HDFS blocks based on extracted log-event features.

Implemented capabilities:

- 32 numerical features per HDFS block.
- Normal and anomaly label encoding.
- Stratified train/test split preparation.
- Random Forest model training.
- Class-weight balancing to account for class imbalance.
- Model prediction and probability scoring using the trained classifier.

The current implementation provides the model training foundation. A reproducible model evaluation and reporting workflow is planned.

### 5. Automated Testing

The project currently includes 32 automated tests covering:

- Incident detection.
- HDFS log parsing.
- Event template matching.
- Block feature aggregation.
- Dataset preparation.

Latest verified result: **32 tests passed.**

## Model Evaluation and Validation

The Random Forest classifier was evaluated through exploratory experiments using the HDFS benchmark dataset.

### Initial Random Train/Test Split

An initial stratified 80/20 train/test split produced exceptionally high classification performance:

| Metric | Anomaly Class Result |
|---|---:|
| Precision | 99.59% |
| Recall | 100.00% |
| F1-score | 99.79% |
| ROC-AUC | 1.0000 |
| Average Precision | 0.9993 |

However, further investigation identified substantial overlap in feature patterns between the training and test datasets. Approximately **99.95% of test records contained feature patterns already observed during training.**

This raised concerns about whether the random split adequately measured the model's ability to generalize to previously unseen patterns.

### Pattern-Separated Validation

To investigate generalization, additional five-fold validation experiments were conducted using `StratifiedGroupKFold`, grouping records by their unique numerical feature patterns.

The grouped validation experiments revealed substantial variation in anomaly-class precision across folds:

- Lowest fold precision: approximately 1.06%.
- Highest fold precision: approximately 98.77%.
- Mean fold ROC-AUC: approximately 0.9699.
- Mean fold Average Precision: approximately 0.9604.

The results demonstrate that model performance was sensitive to the distribution of feature patterns and anomaly prevalence across validation folds.

The fold sizes and class distributions also varied considerably because of the distribution of feature-pattern groups.

### Evaluation Conclusion

The initial random split produced excellent metrics, but the pattern-separated experiments revealed important generalization limitations.

The Random Forest classifier remains experimental. Further work is needed on feature engineering, validation strategies, and model evaluation before considering production use.

**Reproducibility note:** These evaluation metrics are from exploratory experiments performed during development. A complete, reproducible evaluation script is not yet included in the repository.

## HDFS Benchmark Dataset

This project uses the HDFS benchmark dataset from [LogHub](https://github.com/logpai/loghub).

The benchmark contains distributed-system logs generated in a private cloud environment and labeled for normal and anomalous behavior.

Dataset characteristics used in the current experiment:

| Metric | Value |
|---|---:|
| Log records | 11,175,629 |
| HDFS blocks | 575,061 |
| Event templates | 29 |
| Normal blocks | 558,223 |
| Anomalous blocks | 16,838 |

### Dataset Files

The following source files are required for the current HDFS processing workflow:

- `HDFS.log` – Raw HDFS system log records.
- `HDFS.log_templates.csv` – Event templates used for log event matching.
- `anomaly_label.csv` – Ground-truth normal and anomaly labels.

The block-level feature file (`block_features.csv`) is generated during processing and is not included in the repository.

The original dataset is not bundled with this project. Download and prepare it separately from LogHub. A complete one-command dataset processing and model evaluation workflow is a future development item.

The raw dataset and generated feature files are maintained outside this GitHub repository.

## Project Structure

```text
incident-management-root-cause-analysis/
│
├── backend/
│   ├── __init__.py
│   ├── main.py
│   ├── database.py
│   ├── hdfs_parser.py
│   ├── event_matcher.py
│   ├── block_processor.py
│   ├── dataset_preparation.py
│   └── anomaly_detector.py
│
├── tests/
│   ├── test_incidents.py
│   ├── test_hdfs_parser.py
│   ├── test_event_matcher.py
│   ├── test_block_processor.py
│   └── test_dataset_preparation.py
│
├── requirements.txt
├── .gitignore
├── LICENSE
└── README.md
```

Local runtime data, downloaded datasets, and generated processing outputs are excluded from Git.

## Getting Started

### Prerequisites

- Python 3.14 or a compatible Python environment.
- Git.
- GitHub repository cloned locally.

### 1. Clone the repository

```bash
git clone https://github.com/manisha00289/incident-management-root-cause-analysis.git

cd incident-management-root-cause-analysis
```

### 2. Create a virtual environment

Windows PowerShell:

```powershell
python -m venv .venv
```

### 3. Install dependencies

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

### 4. Run automated tests

```powershell
.\.venv\Scripts\python.exe -m pytest -v
```

**Note:** The Getting Started instructions install the project dependencies and run the automated tests. They do not currently execute the complete HDFS dataset processing and model evaluation workflow. The HDFS dataset must be obtained and prepared separately.

## Development Roadmap

The project is being developed incrementally across four phases.

### Phase 1: Core Processing and Detection

- [x] Initial incident detection pipeline.
- [x] SQLite incident persistence.
- [x] HDFS log parser.
- [x] Event template matcher.
- [x] Block-level feature aggregation.
- [x] Dataset preparation.
- [x] Initial Random Forest anomaly detector.
- [x] Automated unit tests.

### Phase 2: Investigation Intelligence

- [ ] Evidence-based root cause investigation engine.
- [ ] Incident correlation and enrichment.
- [ ] Evidence generation and investigation recommendations.

### Phase 3: Application Development

- [ ] REST API development.
- [ ] Interactive incident management dashboard.
- [ ] End-to-end integration testing.

### Phase 4: Cloud and Distributed Engineering

- [ ] PySpark-based distributed processing.
- [ ] AWS cloud integration.
- [ ] Docker-based deployment.
- [ ] CI/CD automation using GitHub Actions.
- [ ] Cloud monitoring with Amazon CloudWatch.

## Engineering Approach

The project follows an incremental engineering approach:

1. Build modular and reusable components.
2. Validate data quality before model training.
3. Use automated tests to protect existing functionality.
4. Evaluate machine learning models using appropriate validation strategies.
5. Develop evidence-based investigation capabilities.
6. Progress toward a cloud-deployable application.

## Research References

- Wei Xu, Ling Huang, Armando Fox, David Patterson, and Michael Jordan. *Detecting Large-Scale System Problems by Mining Console Logs*. SOSP 2009.
- Jieming Zhu, Shilin He, Pinjia He, Jinyang Liu, and Michael R. Lyu. *Loghub: A Large Collection of System Log Datasets for AI-driven Log Analytics*. ISSRE 2023.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

---

**Project Status:** Active Development

Built to explore the intersection of Data Engineering, Machine Learning, and Software Engineering for intelligent incident investigation.
