# Getting Started

## 1. Create a Pipeline YAML

Create a file named `pipeline.yaml`:

```yaml
inputs:
  - path: data/input.csv
    format: csv

steps:
  - filter:
      column: age
      op: ">="
      value: 18
  - select:
      columns: [id, name, age, score]

outputs:
  - path: data/output.parquet
    format: parquet
```

## 2. Prepare Sample Data

Create `data/input.csv`:

```csv
id,name,age,score
1,Alice,25,85
2,Bob,17,90
3,Charlie,30,75
4,Diana,22,88
```

## 3. Run the Pipeline

```bash
mlprep run pipeline.yaml
```

## 4. Check the Output

The filtered data (age >= 18) will be saved to `data/output.parquet`.

## Next Steps

- Learn about [Pipeline YAML Reference](pipeline-reference.md)
- Explore [Validation & Quarantine Mode](validation.md)
- Try [Feature Engineering](feature-engineering.md)
