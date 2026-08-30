import shutil
import subprocess
import sys
from pathlib import Path

from prefect import get_run_logger, task


@task(retries=1, retry_delay_seconds=15)
def executar(comando, full_refresh=False):
    executavel = shutil.which("dbt", path=str(Path(sys.executable).parent)) or "dbt"
    argumentos = [executavel, comando, "--profiles-dir", "."]
    if full_refresh:
        argumentos.append("--full-refresh")
    processo = subprocess.run(
        argumentos,
        cwd=Path("dbt_franq"),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    get_run_logger().info(processo.stdout)
    if processo.returncode != 0:
        raise RuntimeError(f"dbt {comando} falhou:\n{processo.stdout}\n{processo.stderr}")
    return processo.stdout
