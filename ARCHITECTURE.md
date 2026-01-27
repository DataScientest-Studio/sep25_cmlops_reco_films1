## Architecture du pipeline MLOps

                ┌──────────────────────────┐
                │        Airflow DAG 1      │
                │   ingestion_ratings_dag   │
                └──────────────┬───────────┘
                               │
                               ▼
                        detect_new_files
                               │
                               ▼
                        POST /load_ratings
                               │
                               ▼
                ┌──────────────────────────┐
                │   FastAPI /load_ratings   │
                │  → charge CSV dans MySQL  │
                │  → détection de dérive    │
                │  → métriques Prometheus   │
                └──────────────┬───────────┘
                               │
                               ▼
                    TriggerDagRunOperator
                               │
                               ▼
                ┌──────────────────────────┐
                │        Airflow DAG 2      │
                │     training_svd_dag      │
                └──────────────┬───────────┘
                               │
                               ▼
                        POST /training
                               │
                               ▼
                ┌──────────────────────────┐
                │   FastAPI /training       │
                │  → train_svd_model        │
                │  → logging MLflow         │
                │  → alias best_model       │
                └──────────────────────────┘
