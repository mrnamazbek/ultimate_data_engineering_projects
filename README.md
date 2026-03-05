<img src="https://capsule-render.vercel.app/api?type=waving&color=gradient&customColorList=0,2,3,5,6&height=200&section=header&text=NAMAZBEK%20BEKZHANOV&fontSize=50&fontColor=fff&animation=fadeIn&fontAlignY=35&desc=Big%20Data%20%E2%80%A2%20Machine%20Learning%20%E2%80%A2%20Software%20Engineering&descSize=16&descAlignY=53">

<br>

<img src="https://user-images.githubusercontent.com/73097560/115834477-dbab4500-a447-11eb-908a-139a6edaec5c.gif">

<br>

## 🧬 `$ python3 profile.py --execute`

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════╗
║  PROFESSIONAL PROFILE: Namazbek Bekzhanov                    ║
║  Role: Big Data Engineer | ML Specialist                     ║
║  Organization: Freedom Finance Insurance                     ║
╚══════════════════════════════════════════════════════════════╝
"""

from typing import List, Dict
from dataclasses import dataclass
from datetime import datetime


@dataclass
class Engineer:
    """Data-driven professional focused on scalable solutions."""

    name: str = "Namazbek Bekzhanov"
    role: str = "Big Data Engineer & ML Enthusiast"
    location: str = "Almaty, Kazakhstan 🇰🇿"
    company: str = "Freedom Finance Insurance"

    education: Dict[str, str] = None
    languages: List[str] = None
    stack: Dict[str, List[str]] = None

    def __post_init__(self):
        self.education = {
            "🎓 Current":   "MSc in Data Science @ KBTU (2025-2027)",
            "🎓 Completed": "BSc in Computer Science @ SDU (2021-2025)"
        }

        self.languages = ["Python", "Go", "Java", "SQL", "Bash"]

        self.stack = {
            "big_data":   ["Spark", "Hadoop", "Airflow", "Kafka"],
            "ml_ai":      ["Scikit-Learn", "TensorFlow", "PyTorch", "Keras"],
            "databases":  ["PostgreSQL", "Oracle", "MySQL", "MSSQL"],
            "devops":     ["Docker", "Git", "Linux", "CI/CD"],
            "frameworks": ["FastAPI", "Pandas", "NumPy"]
        }

    def current_mission(self) -> List[str]:
        """My daily engineering adventures."""
        return [
            "🔥 Architecting scalable ETL/ELT data pipelines",
            "🧠 Building production ML models for real impact",
            "🐘 Orchestrating big data workflows (Airflow + Spark)",
            "⚡ Optimizing database performance & queries",
            "⚽ Leading teammates to victory on & off the field"
        ]

    def get_expertise_matrix(self) -> Dict[str, int]:
        """Skill proficiency levels (out of 10)."""
        return {
            "Python Development":    9,
            "Data Engineering":      9,
            "Machine Learning":      8,
            "Big Data (Spark/Hadoop)": 8,
            "Database Design":       9,
            "Cloud Architecture":    7,
            "Football Strategy":    10  # ⚽
        }

    def philosophy(self) -> str:
        return "Code is poetry. Data tells stories. I write both."


def main():
    me = Engineer()

    print(f"\n{'='*60}")
    print(f"👋 Hello, World! I'm {me.name}")
    print(f"📍 Based in {me.location}")
    print(f"💼 {me.role} @ {me.company}")
    print(f"{'='*60}\n")

    print("🎯 CURRENT MISSION:")
    for mission in me.current_mission():
        print(f"   {mission}")

    print(f"\n💡 PHILOSOPHY: {me.philosophy()}\n")


if __name__ == "__main__":
    main()
```

<br>

<img src="https://user-images.githubusercontent.com/73097560/115834477-dbab4500-a447-11eb-908a-139a6edaec5c.gif">

## 🏆 Achievement Showcase

<div align="center">

| 🎯 Area | 🚀 Highlight |
|---------|-------------|
| 🔄 Streaming Pipeline | Real-time Kafka → Spark → MinIO + Postgres |
| 🏗️ Data Warehouse | dbt-powered ELT with Airflow 3.1.6 orchestration |
| 🤖 ML Deploy | FastAPI model service, Dockerised, K8s-ready |
| 📊 BI Integration | Apache Superset dashboards for every pipeline |
| �� Security | Zero CVEs — all deps tracked against GitHub Advisory DB |
| ⚙️ CI/CD | GitHub Actions: lint → unit tests → Docker build → smoke test |

</div>

<br>

<img src="https://user-images.githubusercontent.com/73097560/115834477-dbab4500-a447-11eb-908a-139a6edaec5c.gif">

## 🎨 Data Engineering Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     de-ml-monorepo  (this repo)                     │
│                                                                     │
│  ┌──────────────┐    ┌──────────────────┐    ┌──────────────────┐  │
│  │  PROJECT 1   │    │   PROJECT 2      │    │   PROJECT 3      │  │
│  │              │    │                  │    │                  │  │
│  │  Kafka       │    │  Airflow 3.1.6   │    │  Trainer         │  │
│  │    ↓         │    │      ↓           │    │  (scikit-learn)  │  │
│  │  PySpark     │    │  dbt-postgres    │    │      ↓           │  │
│  │  Streaming   │───▶│  (ELT models)   │    │  model.pkl       │  │
│  │    ↓    ↓    │    │      ↓           │    │      ↓           │  │
│  │ MinIO Postgres│   │  Postgres DWH    │    │  FastAPI         │  │
│  └──────────────┘    └──────────────────┘    │  /predict        │  │
│                                              └──────────────────┘  │
│                                                                     │
│  ────────────────── Shared Infrastructure ─────────────────────    │
│  Postgres 15 · MinIO · Zookeeper/Kafka · Spark · Superset 6.0.0   │
└─────────────────────────────────────────────────────────────────────┘
```

<br>

<img src="https://user-images.githubusercontent.com/73097560/115834477-dbab4500-a447-11eb-908a-139a6edaec5c.gif">

## ⚡ Technology Arsenal

<div align="center">

**Languages & Core Technologies**

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Go](https://img.shields.io/badge/Go-00ADD8?style=for-the-badge&logo=go&logoColor=white)
![Java](https://img.shields.io/badge/Java-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-336791?style=for-the-badge&logo=postgresql&logoColor=white)
![Bash](https://img.shields.io/badge/Bash-4EAA25?style=for-the-badge&logo=gnu-bash&logoColor=white)
![YAML](https://img.shields.io/badge/YAML-CB171E?style=for-the-badge&logo=yaml&logoColor=white)

**Big Data & Streaming**

![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)

**Databases & Storage**

![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO-C72E49?style=for-the-badge&logo=minio&logoColor=white)
![MySQL](https://img.shields.io/badge/MySQL-005C84?style=for-the-badge&logo=mysql&logoColor=white)
![Oracle](https://img.shields.io/badge/Oracle-F80000?style=for-the-badge&logo=oracle&logoColor=white)
![MSSQL](https://img.shields.io/badge/Microsoft_SQL_Server-CC2927?style=for-the-badge&logo=microsoft-sql-server&logoColor=white)

**ML & AI**

![scikit-learn](https://img.shields.io/badge/scikit--learn-F7931E?style=for-the-badge&logo=scikit-learn&logoColor=white)
![TensorFlow](https://img.shields.io/badge/TensorFlow-FF6F00?style=for-the-badge&logo=tensorflow&logoColor=white)
![PyTorch](https://img.shields.io/badge/PyTorch-EE4C2C?style=for-the-badge&logo=pytorch&logoColor=white)
![Keras](https://img.shields.io/badge/Keras-D00000?style=for-the-badge&logo=keras&logoColor=white)
![NumPy](https://img.shields.io/badge/NumPy-013243?style=for-the-badge&logo=numpy&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)

**DevOps & Infrastructure**

![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Kubernetes](https://img.shields.io/badge/Kubernetes-326CE5?style=for-the-badge&logo=kubernetes&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-2088FF?style=for-the-badge&logo=github-actions&logoColor=white)
![Git](https://img.shields.io/badge/Git-F05032?style=for-the-badge&logo=git&logoColor=white)
![Linux](https://img.shields.io/badge/Linux-FCC624?style=for-the-badge&logo=linux&logoColor=black)

**Frameworks & APIs**

![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![Apache Superset](https://img.shields.io/badge/Apache%20Superset-20A6C9?style=for-the-badge&logo=apache&logoColor=white)

</div>

<br>

<img src="https://user-images.githubusercontent.com/73097560/115834477-dbab4500-a447-11eb-908a-139a6edaec5c.gif">

## 📋 Описание проекта / Project Description

### 🇷🇺 Русское описание (Russian Description)

**Ultimate Data Engineering Projects** — это комплексный учебный проект, который показывает, как работают современные системы обработки данных. Проект состоит из трех частей, которые работают вместе и показывают разные аспекты data engineering.

**Что делает этот проект:**
- Обрабатывает данные в реальном времени с помощью Apache Kafka и Spark
- Строит хранилище данных (data warehouse) с помощью dbt и Airflow  
- Создает и развертывает модели машинного обучения через FastAPI
- Показывает, как все компоненты работают вместе в одной системе

**Зачем это нужно:**
Этот проект помогает понять, как строятся современные системы данных в больших компаниях. Вы увидите, как данные проходят путь от сбора до анализа и как разные инструменты работают вместе.

**Кто может использовать:**
- Студенты, изучающие data engineering
- Начинающие дата-инженеры
- Разработчики, которые хотят понять системы больших данных
- Команды, которым нужен пример современной архитектуры

### 🇬🇧 English Description

**Ultimate Data Engineering Projects** is a comprehensive learning project that demonstrates how modern data processing systems work. The project has three parts that work together to show different aspects of data engineering.

**What this project does:**
- Processes real-time data using Apache Kafka and Spark
- Builds a data warehouse using dbt and Airflow
- Creates and deploys machine learning models via FastAPI
- Shows how all components work together in one system

**Why you need this:**
This project helps you understand how modern data systems are built in large companies. You will see how data travels from collection to analysis and how different tools work together.

**Who can use this:**
- Students learning data engineering
- Beginning data engineers
- Developers who want to understand big data systems
- Teams that need an example of modern architecture

<br>

<img src="https://user-images.githubusercontent.com/73097560/115834477-dbab4500-a447-11eb-908a-139a6edaec5c.gif">

## 🚀 Signature Projects

<div align="center">

### [`de-ml-monorepo`](./de-ml-monorepo) — Production-grade DE/ML Monorepo

> Three end-to-end data engineering & ML projects sharing a single Docker Compose stack

| Project | Description | Stack | Guide |
|---------|-------------|-------|-------|
| [**1 · Kafka + Spark Streaming**](./de-ml-monorepo/projects/project1-kafka-spark) | Real-time event pipeline: produce → stream → store | Kafka · PySpark · MinIO · Postgres | [DEVELOPMENT.md](./de-ml-monorepo/projects/project1-kafka-spark/DEVELOPMENT.md) |
| [**2 · DW + dbt + Airflow**](./de-ml-monorepo/projects/project2-dw-dbt-airflow) | ELT data warehouse with daily orchestration | Airflow 3.1.6 · dbt · Postgres · Superset | [DEVELOPMENT.md](./de-ml-monorepo/projects/project2-dw-dbt-airflow/DEVELOPMENT.md) |
| [**3 · ML Deploy**](./de-ml-monorepo/projects/project3-ml-deploy) | Train → serve a recommendation model at scale | scikit-learn · FastAPI · Docker · K8s | [DEVELOPMENT.md](./de-ml-monorepo/projects/project3-ml-deploy/DEVELOPMENT.md) |

**Quick start:**

```bash
git clone https://github.com/mrnamazbek/ultimate_data_engineering_projects.git
cd ultimate_data_engineering_projects/de-ml-monorepo
cp .env.example .env
docker compose up --build
```

Services available after startup:

| 🌐 Service | 🔗 URL | 🔑 Credentials |
|-----------|--------|----------------|
| Airflow UI | http://localhost:8085 | admin / admin |
| Superset BI | http://localhost:8088 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| FastAPI Docs | http://localhost:8000/docs | — |
| Spark UI | http://localhost:8080 | — |

</div>

<br>

<img src="https://user-images.githubusercontent.com/73097560/115834477-dbab4500-a447-11eb-908a-139a6edaec5c.gif">

## 📊 GitHub Analytics Dashboard

<div align="center">

<img height="180em" src="https://github-readme-stats.vercel.app/api?username=mrnamazbek&show_icons=true&theme=tokyonight&include_all_commits=true&count_private=true"/>
<img height="180em" src="https://github-readme-stats.vercel.app/api/top-langs/?username=mrnamazbek&layout=compact&langs_count=8&theme=tokyonight"/>

<img src="https://github-readme-streak-stats.herokuapp.com/?user=mrnamazbek&theme=tokyonight" alt="mrnamazbek streak"/>

</div>

<br>

<img src="https://user-images.githubusercontent.com/73097560/115834477-dbab4500-a447-11eb-908a-139a6edaec5c.gif">

<br>

## 🤝 Connect With Me

<div align="center">

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/mrnamazbek)
[![GitHub](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)](https://github.com/mrnamazbek)
[![Telegram](https://img.shields.io/badge/Telegram-2CA5E0?style=for-the-badge&logo=telegram&logoColor=white)](https://t.me/mrnamazbek)

</div>

<br>

<sub>Engineer of my own destiny. Built different. Chasing greatness.</sub>

<br><br>

<img src="https://capsule-render.vercel.app/api?type=waving&color=gradient&customColorList=0,2,3,5,6&height=120&section=footer">
