"""
INCI 데이터 파이프라인 월간 DAG
매월 1일 01:00 (KST) 에 자동 실행

실행 순서:
  bronze_kcia ─┐
               ├─▶ silver_mapping ─▶ gold_pipeline
  bronze_cosing┘
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# ---------------------------------------------------------------------------
# 설정
# ---------------------------------------------------------------------------

ECR_REGISTRY = os.environ.get("ECR_REGISTRY", "")
IMAGE = f"{ECR_REGISTRY}/evr0/inci-pipeline:latest"

# 우선순위: Airflow Variable > 환경변수 HOST_PROJECT_DIR > 기본값
# EC2 배포 시 HOST_PROJECT_DIR 또는 Airflow Variable 'inci_project_dir' 을 실제 경로로 설정
PROJECT_DIR = Variable.get(
    "inci_project_dir",
    default_var=os.environ.get("HOST_PROJECT_DIR", "/app"),
)

# .env 파일에서 환경변수 로드
def _load_env(env_path: str) -> dict[str, str]:
    env: dict[str, str] = {}
    path = Path(env_path)
    if not path.exists():
        return env
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        env[key.strip()] = value.strip()
    return env

ENV_VARS = _load_env(os.path.join(PROJECT_DIR, ".env"))

# ---------------------------------------------------------------------------
# DockerOperator 공통 설정
# ---------------------------------------------------------------------------

COMMON = dict(
    image=IMAGE,
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge",
    auto_remove="success",
    mount_tmp_dir=False,
    mounts=[
        Mount(source=PROJECT_DIR, target="/app", type="bind"),
    ],
    environment=ENV_VARS,
    working_dir="/app",
    retries=1,
    retry_delay=timedelta(minutes=5),
)

# ---------------------------------------------------------------------------
# DAG 정의
# ---------------------------------------------------------------------------

default_args = {
    "owner": "inci",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="inci_monthly_pipeline",
    description="INCI Bronze → Silver → Gold 월간 파이프라인",
    schedule="0 1 1 * *",   # 매월 1일 01:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["inci", "bronze", "silver", "gold"],
) as dag:

    bronze_kcia = DockerOperator(
        task_id="bronze_kcia",
        command="python -m pipeline.kcia_pipeline.app",
        execution_timeout=timedelta(hours=2),
        **COMMON,
    )

    bronze_cosing = DockerOperator(
        task_id="bronze_cosing",
        command="python -m pipeline.cosing_pipeline.app",
        execution_timeout=timedelta(hours=2),
        **COMMON,
    )

    silver_mapping = DockerOperator(
        task_id="silver_mapping",
        command="python -m pipeline.silver_mapping.kcia_cosing.run_mapping",
        execution_timeout=timedelta(hours=1),
        **COMMON,
    )

    gold_pipeline = DockerOperator(
        task_id="gold_pipeline",
        command="python -m pipeline.gold_pipeline.kcia_cosing.run_gold",
        execution_timeout=timedelta(minutes=30),
        **COMMON,
    )

    # bronze 둘 다 완료돼야 silver 시작
    [bronze_kcia, bronze_cosing] >> silver_mapping >> gold_pipeline
