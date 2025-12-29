"""Tests for benchmark script helpers."""

from importlib import util
from pathlib import Path

BENCHMARK_PATH = Path(__file__).resolve().parents[2] / "scripts" / "benchmark.py"
SPEC = util.spec_from_file_location("benchmark", BENCHMARK_PATH)
benchmark = util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark)


def test_generate_data_showcase_schema(tmp_path):
    """Showcase schema includes promotion-focused columns."""
    path = tmp_path / "showcase.csv"

    benchmark.generate_data(str(path), rows=5, profile="showcase")

    header = path.read_text().splitlines()[0]
    columns = header.split(",")

    assert columns[:4] == ["a", "b", "c", "group_key"]
    assert "email" in columns
    assert "age" in columns
    assert "income" in columns
    assert "city" in columns


def test_build_validation_pipeline_includes_quarantine():
    """Validation pipeline should run in quarantine mode."""
    yaml = benchmark.build_validation_pipeline("input.csv", "output.parquet")

    assert "type: validate" in yaml
    assert "mode: quarantine" in yaml
    assert "unique: true" in yaml
    assert "range: [0, 120]" in yaml


def test_build_features_pipeline_includes_state_path():
    """Feature pipeline should include state path and transforms."""
    yaml = benchmark.build_features_pipeline(
        "input.csv",
        "output.parquet",
        "state.json",
    )

    assert "type: features" in yaml
    assert 'state_path: "state.json"' in yaml
    assert "standard_scale" in yaml
    assert "one_hot_encode" in yaml


def test_render_showcase_markdown_includes_rows_per_sec():
    """Showcase markdown includes throughput column."""
    results = [
        {
            "task": "Pipeline (Validation)",
            "tool": "mlprep (CLI)",
            "time": 2.0,
            "rows": 100,
            "note": "validation + quarantine",
        }
    ]

    markdown = benchmark.render_showcase_markdown(results)

    assert "| Task | Tool | Time (s) | Rows | Rows/s | Highlights |" in markdown
    assert "validation + quarantine" in markdown
    assert "50.00" in markdown


def test_build_showcase_pipeline_combines_steps():
    """Showcase pipeline should include validation, features, and runtime block."""
    yaml = benchmark.build_showcase_pipeline(
        "input.csv", "output.parquet", "state.json", streaming=True
    )

    assert "type: validate" in yaml
    assert "type: features" in yaml
    assert "runtime:" in yaml
    assert "streaming: true" in yaml
