"""Test suite for mlprep Python I/O bindings."""

import os
import tempfile

import mlprep
import polars as pl
import pytest


class TestReadCSV:
    """Tests for mlprep.read_csv function."""

    def test_read_csv_basic(self):
        """Test basic CSV reading."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("a,b,c\n1,2,3\n4,5,6\n")
            csv_path = f.name

        try:
            df = mlprep.read_csv(csv_path)
            assert df is not None
            # Convert to polars to check shape
            pl_df = df.to_polars()
            assert pl_df.shape == (2, 3)
            assert pl_df.columns == ["a", "b", "c"]
        finally:
            os.unlink(csv_path)

    def test_read_csv_with_types(self):
        """Test CSV reading with various data types."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("int_col,float_col,str_col\n1,1.5,hello\n2,2.5,world\n")
            csv_path = f.name

        try:
            df = mlprep.read_csv(csv_path)
            pl_df = df.to_polars()
            assert pl_df.shape == (2, 3)
            assert pl_df["str_col"].to_list() == ["hello", "world"]
        finally:
            os.unlink(csv_path)

    def test_read_csv_file_not_found(self):
        """Test that reading a non-existent file raises an error."""
        with pytest.raises(OSError):
            mlprep.read_csv("nonexistent_file.csv")


class TestReadParquet:
    """Tests for mlprep.read_parquet function."""

    def test_read_parquet_basic(self):
        """Test basic Parquet reading."""
        with tempfile.TemporaryDirectory() as tmpdir:
            parquet_path = os.path.join(tmpdir, "test.parquet")
            # Create a parquet file using polars
            pl_df = pl.DataFrame({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
            pl_df.write_parquet(parquet_path)

            df = mlprep.read_parquet(parquet_path)
            result = df.to_polars()
            assert result.shape == (3, 2)
            assert result.columns == ["a", "b"]

    def test_read_parquet_file_not_found(self):
        """Test that reading a non-existent file raises an error."""
        with pytest.raises(OSError):
            mlprep.read_parquet("nonexistent_file.parquet")


class TestWriteParquet:
    """Tests for mlprep.write_parquet function."""

    def test_write_parquet_basic(self):
        """Test basic Parquet writing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "test.csv")
            parquet_path = os.path.join(tmpdir, "output.parquet")

            # Create a CSV file
            with open(csv_path, "w") as f:
                f.write("x,y,z\n10,20,30\n40,50,60\n")

            # Read CSV and write to Parquet
            df = mlprep.read_csv(csv_path)
            mlprep.write_parquet(df, parquet_path)

            # Verify the parquet file was created and can be read
            assert os.path.exists(parquet_path)
            result = pl.read_parquet(parquet_path)
            assert result.shape == (2, 3)


class TestRoundtrip:
    """Tests for CSV -> DataFrame -> Parquet -> DataFrame roundtrip."""

    def test_csv_parquet_roundtrip(self):
        """Test full roundtrip: CSV -> read -> write Parquet -> read."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "input.csv")
            parquet_path = os.path.join(tmpdir, "output.parquet")

            # Create a CSV file
            with open(csv_path, "w") as f:
                f.write("id,value,name\n1,100,alice\n2,200,bob\n3,300,charlie\n")

            # Read CSV
            df1 = mlprep.read_csv(csv_path)

            # Write to Parquet
            mlprep.write_parquet(df1, parquet_path)

            # Read back from Parquet
            df2 = mlprep.read_parquet(parquet_path)

            # Compare
            pl_df1 = df1.to_polars()
            pl_df2 = df2.to_polars()

            assert pl_df1.shape == pl_df2.shape
            assert pl_df1.columns == pl_df2.columns


class TestToPolars:
    """Tests for PyDataFrame.to_polars() method."""

    def test_to_polars_returns_polars_dataframe(self):
        """Test that to_polars() returns a proper Polars DataFrame."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("col1,col2\n1,2\n3,4\n")
            csv_path = f.name

        try:
            df = mlprep.read_csv(csv_path)
            pl_df = df.to_polars()

            # Verify it's a Polars DataFrame
            assert isinstance(pl_df, pl.DataFrame)
            assert pl_df.shape == (2, 2)

            # Verify we can use Polars operations
            filtered = pl_df.filter(pl.col("col1") > 1)
            assert filtered.shape == (1, 2)
        finally:
            os.unlink(csv_path)
