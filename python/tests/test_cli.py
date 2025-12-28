"""Tests for the Python CLI wrapper."""

import os
import subprocess
import sys
import tempfile

import mlprep


def _write_pipeline(tmpdir, input_path, output_path):
    pipeline_yaml = f"""
inputs:
  - path: "{input_path}"
steps:
  - type: select
    columns: ["a"]
outputs:
  - path: "{output_path}"
    format: csv
"""
    pipeline_path = os.path.join(tmpdir, "pipeline.yaml")
    with open(pipeline_path, "w") as f:
        f.write(pipeline_yaml)
    return pipeline_path


def test_run_pipeline_streaming_option():
    """run_pipeline accepts streaming/memory_limit and produces output."""
    with tempfile.TemporaryDirectory() as tmpdir:
        input_path = os.path.join(tmpdir, "input.csv")
        output_path = os.path.join(tmpdir, "output.csv")
        with open(input_path, "w") as f:
            f.write("a,b\n1,2\n3,4\n")

        pipeline_path = _write_pipeline(tmpdir, input_path, output_path)

        mlprep.run_pipeline(pipeline_path, streaming=True, memory_limit="1GB")

        assert os.path.exists(output_path)


def test_cli_streaming_flag():
    """`python -m mlprep run ... --streaming` works."""
    with tempfile.TemporaryDirectory() as tmpdir:
        input_path = os.path.join(tmpdir, "input.csv")
        output_path = os.path.join(tmpdir, "output.csv")
        with open(input_path, "w") as f:
            f.write("a,b\n1,2\n")

        pipeline_path = _write_pipeline(tmpdir, input_path, output_path)

        cmd = [
            sys.executable,
            "-m",
            "mlprep",
            "run",
            pipeline_path,
            "--streaming",
            "--memory-limit",
            "1GB",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)

        assert result.returncode == 0, result.stderr
        assert os.path.exists(output_path)
