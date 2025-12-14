# 1. mlprep 製品概要

## 1.1 目的

* AI/MLの学習・推論パイプラインで支配的になりがちな前処理コスト（読み込み、型推定、欠損、結合、集計、特徴量生成、データ検証）をRust実装で高速化する。
* Pythonからは通常のライブラリとして使える一方、巨大データはCLIでも処理できるようにする。
* 速度だけでなく「再現性」「データ品質」「監査可能性」を同時に満たす。

## 1.2 スコープ

* 対象データ：表形式（CSV/TSV/Parquet/Arrow/JSONL）を中心。画像・音声はメタデータ処理を主対象（拡張で対応）。
* 対象処理：ETL/ELT、特徴量生成、データ検証、出力最適化、データプロファイル、スキーマ管理。

## 1.3 成果物（配布形態）

* Pythonパッケージ：`pip install mlprep`
  * プロジェクト名：`mlprep`
  * パッケージ名：`mlprep`
* 同梱CLI：`mlprep ...`
* ドキュメント：APIリファレンス、チュートリアル、ベンチマーク、移行ガイド（pandas→mlprep）

---

# 2. ユーザー像と主要ユースケース

## 2.1 ペルソナ

* MLエンジニア：特徴量生成・データ検証が本業。前処理が遅くて反復が回らない。
* データエンジニア：バッチ/パイプライン運用。再現性・監視・障害耐性が重要。
* MLOps：学習/推論のデータ整合と監査を担う。スキーマ変化に敏感。
* 研究者/分析者：ローカルPCでも大きめデータを扱いたい。

## 2.2 主要ユースケース

1. 巨大CSV/JSONLを高速に読み込み、型推定→欠損処理→集計→Parquet出力
2. 学習用特徴量を同一定義で毎回生成（再現性）
3. 推論時に入力データをスキーマ検証し、逸脱を検知・隔離
4. データ品質レポート（欠損率、分布、外れ値、重複、リーク疑い）を自動生成
5. ローカル/CIで前処理を高速実行し、成果物をアーティファクトとして保存

---

# 3. 機能要件（完全一覧）

## 3.1 データ入出力（I/O）

### 3.1.1 入力フォーマット

* CSV/TSV（圧縮：gzip/zstd対応）
* Parquet（圧縮：snappy/gzip/zstd対応）
* Arrow IPC / Feather
* JSONL（行JSON）
* Optional（Phase拡張）：Avro、ORC

### 3.1.2 入力ソース

* ローカルファイル
* ディレクトリ（glob/ワイルドカード）
* stdin（CLI）
* Optional：HTTP(S) / S3 / GCS（企業/有償機能や拡張モジュールに分離）

### 3.1.3 出力フォーマット

* Parquet（デフォルト推奨）
* Arrow IPC / Feather
* CSV（必要時のみ）
* “Feature pack”形式（後述：特徴量＋メタ情報をまとめて出力）

### 3.1.4 出力最適化

* Parquet row group サイズ調整
* 列指向最適化（dictionary encoding、圧縮選択）
* ソート/クラスタリング（指定列による並べ替え、range partition）

---

## 3.2 スキーマと型（Schema & Types）

### 3.2.1 型システム（内部）

* Arrow互換の型を中核（int/float/bool/string/binary/date/time/timestamp/decimal/list/struct/categorical）

### 3.2.2 型推定

* サンプル推定（行数/割合指定）
* カラムごとの推定ルール優先順位（例：int→float→string）
* 日付/日時パターン推定（複数フォーマット候補）
* カテゴリ判定（ユニーク比率・カーディナリティ閾値）

### 3.2.3 スキーマ固定とバージョニング

* スキーマをYAML/JSONで保存・読み込み
* スキーマの互換性チェック（追加列/欠落列/型変更）
* 破壊的変更の検知とエラー/警告ポリシー

---

## 3.3 前処理変換（Transform）

### 3.3.1 基本演算

* select/rename/drop
* filter（式/ブール条件）
* sort
* distinct / drop_duplicates（列指定、keepポリシー）
* join（inner/left/right/full、キー複数、衝突列のsuffix）
* concat/union
* groupby-agg（sum/mean/min/max/std/count/nunique/quantile）
* window（rolling/expanding、partition by、order by）
* pivot/unpivot（Phase拡張でも可）

### 3.3.2 欠損処理

* 欠損定義：null/NaN/空文字/特定トークン（"NA","-" 等）
* strategy：
  * drop row/col
  * fill constant
  * fill mean/median/mode（数値/カテゴリ別）
  * forward/backward fill（時系列）
  * group-wise fill（キーごと統計で補完）

### 3.3.3 外れ値/クリップ

* z-score / IQR / パーセンタイルクリップ
* winsorize
* ルールベース除外（閾値、範囲）

### 3.3.4 文字列処理

* trim/lower/upper
* regex extract/replace（安全制限あり：タイムアウト/最大長）
* tokenize（簡易：split、n-gram）
* カテゴリ正規化（辞書マッピング）

### 3.3.5 日時処理

* parse/format
* timezone付与/変換
* date parts（year/month/day/hour/weekday）
* 期間特徴量（差分、ラグ、リード）
* resampling（Phase拡張：時系列集約）

### 3.3.6 数値処理

* cast、round、clip
* log/exp/sqrt
* binning（equal width/equal freq、指定境界）
* scaling（min-max、standard、robust：Phase 1で提供）

---

## 3.4 特徴量生成（Feature Engineering）

### 3.4.1 FeatureSet（特徴量定義）

* 特徴量を「宣言的に」定義し、同一定義から学習・推論で再生成可能にする。
* FeatureSetはファイル保存可能（YAML/JSON）。ハッシュで同一性担保。

### 3.4.2 主要特徴量

* 数値：スケーリング、ビニング、相互作用（pairwise）、比率、差分
* カテゴリ：
  * frequency encoding
  * target encoding（Phase拡張：リーク防止オプション必須）
  * one-hot（高次元は上限・まとめカテゴリ）
* テキスト（軽量）：
  * length、token count
  * hashing trick（固定次元）
* 時系列：
  * lag、rolling統計（mean/std/min/max）
  * 直近N件の集約（entityごと）
* 集計特徴：
  * entityキーごとの集計（例：user_id単位の購入回数）

### 3.4.3 学習/推論の一貫性

* fit/transform API（学習時に統計量を保存、推論時に再利用）
* 例：スケーラーの平均・分散、カテゴリ語彙、頻度表などを「FeatureState」として保存

### 3.4.4 Feature pack（出力）

* `features.parquet`：特徴量テーブル
* `feature_state.json`：fit情報（統計量/語彙）
* `schema.json`：最終スキーマ
* `lineage.json`：処理履歴（後述）

---

## 3.5 データ品質・検証（Data Quality & Validation）

### 3.5.1 ルール定義（Checks）

* not_null（列別）
* unique（キー）
* range（min/max）
* regex（文字列）
* enum（許容集合）
* distribution drift（学習基準と比較：PSI/KSなどはPhase拡張可）
* row count / duplicate rate / missing rate 上限

### 3.5.2 実行モード

* strict：違反で失敗（exit code != 0）
* warn：警告出力し処理継続
* quarantine：違反行を隔離出力（violations.parquet）

### 3.5.3 プロファイリング

* 基本統計（min/max/mean/std/quantiles）
* 欠損率、ユニーク数、トップカテゴリ
* 相関（Phase拡張：計算コスト制御）

### 3.5.4 レポート出力

* JSON（機械可読）
* HTML（人間可読：Phase拡張）
* Markdown（CI向け）

---

## 3.6 パイプライン定義と再現性（Pipeline & Reproducibility）

### 3.6.1 宣言的パイプライン

* YAML/JSONで処理手順を定義可能（CLI/Python両対応）
* 例：read → cast → fillna → checks → features → write

### 3.6.2 lineage（処理系譜）

* 入力ファイル一覧、ハッシュ
* 実行環境（OS/CPU/バージョン）
* 変換ステップ一覧（パラメータ含む）
* 出力ファイルハッシュ
* seed（乱数が絡む処理用）

### 3.6.3 キャッシュ（オプション）

* ステップ単位キャッシュ（入力＋ステップ定義のハッシュで再利用）
* CLIでは `.mlprep_cache/` に保存
* Python APIではユーザーが明示ON/OFF可能

---

## 3.7 Python API（必須）

### 3.7.1 コアAPI

* `mlprep.read_csv(path, schema=None, infer_rows=10000, null_values=[...]) -> DataFrame`
* `DataFrame.filter(expr: str) -> DataFrame`
* `DataFrame.groupby(keys).agg(spec) -> DataFrame`
* `DataFrame.join(other, on=..., how="left") -> DataFrame`
* `mlprep.FeatureSet.from_yaml(path)`
* `features.fit(df) -> FeatureState`
* `features.transform(df, state) -> DataFrame`
* `mlprep.validate(df, checks) -> ValidationResult`

### 3.7.2 pandas/Polars連携

* `to_pandas()` / `from_pandas(df)`
* `to_polars()` / `from_polars(df)`（実装可能なら）
* Arrow経由のゼロコピー優先（可能範囲で）

### 3.7.3 エラーモデル

* Rust起因の例外をPython例外へマッピング
* 例外種別：
  * `IOError`：ファイル読み書きエラー
  * `SchemaError`：スキーマ不整合（型不一致、欠落列）
  * `ValidationError`：データ品質チェック違反
  * `ComputeError`：計算処理エラー（ゼロ除算、オーバーフロー）
  * `MemoryError`：メモリ不足
  * `ConfigError`：YAML/JSON設定ファイルの構文・構造エラー
  * `FeatureStateError`：fit/transform状態の不整合（未fit、バージョン不一致）
* エラーメッセージフォーマット：
  * 構造化形式（エラーコード + メッセージ + コンテキスト）
  * 例：`MLPREP-E001: Schema mismatch - column 'age' expected int64, got string`

---

## 3.8 CLI（同梱）

### 3.8.1 基本

* `mlprep run pipeline.yaml`
* `mlprep profile data.csv --out report.json`
* `mlprep validate checks.yaml data.parquet`
* `mlprep features fit config.yaml --in train.parquet --out feature_pack/`
* `mlprep features transform --state feature_pack/feature_state.json --in test.parquet --out out.parquet`

### 3.8.2 運用向け

* `--threads N`（自動/手動）
* `--memory-limit`（ソフトリミット）
* `--log json|text`
* exit code設計（0=成功、2=検証失敗、3=IO、4=OOM等）

---

## 3.9 性能・並列（Performance）

### 3.9.1 目標

* pandas比：典型前処理で 3〜30倍（データ・処理による）
* メモリ：中間コピー削減、Arrowベースで抑制

**具体的ベンチマーク目標（8スレッド、SSD環境）：**

| 処理 | データサイズ | 目標時間 |
|------|-------------|----------|
| CSV読み込み | 1GB | < 5秒 |
| Parquet読み込み | 1GB | < 1秒 |
| GroupBy集計 | 10M行, 100Kグループ | < 2秒 |
| Join | 10M x 1M行, 単一キー | < 3秒 |
| 欠損補完（mean fill） | 10M行, 100列 | < 1秒 |
| Parquet書き出し | 10M行 | < 3秒 |

* メモリ：ピーク使用量 < 入力データサイズの2倍

### 3.9.2 高速化手段（仕様として担保）

* スレッド並列（rayon等）
* ベクトル化（SIMD、Arrow kernels）
* predicate/projection pushdown（対応フォーマットで）
* streaming実行（メモリに乗らないデータの段階処理：Phase拡張でも可）

### 3.9.3 ベンチマークスイート

* 代表タスクを同梱（読み込み、groupby、join、欠損補完、parquet書き出し）
* CIで性能退行を検知（閾値は緩め）

---

## 3.10 セキュリティ・安全（Security & Safety）

* 入力ファイルパスのサンドボックス（CLIで任意：許可ディレクトリ制限）
* regex等の高コスト処理に上限（最大入力長・タイムアウト）
* 依存ライブラリ脆弱性スキャン（リリース要件）
* テレメトリ：デフォルトOFF。ON時は匿名メトリクスのみ（企業要件で切替）

---

# 4. 非機能要件

## 4.1 対応環境

* Python：3.10+（目標）
* OS：Linux（manylinux）、macOS（arm64/x86_64）、Windows
* CPU：x86_64/arm64（SIMDは可能なら最適化）

## 4.2 信頼性

* OOM回避：チャンク処理、streaming（対応範囲明記）
* 途中失敗時：部分出力の扱い（atomic write：一時ファイル→rename）

## 4.3 可観測性

* ログ：構造化ログ（JSON）と人間向けログ
* メトリクス：処理時間、行数、I/Oスループット、メモリ推定
* trace id（パイプライン実行単位）

## 4.4 互換性方針

* API安定度：`stable` と `experimental` を明記
* FeatureSetの後方互換：破壊的変更はメジャーバージョンでのみ

---

# 5. アーキテクチャ仕様

## 5.1 全体構成

* **Execution Engine**: Rust (`polars`, `arrow-rs`)
    * Polarsをバックエンドエンジンとして採用し、Core DataFrame操作の高速化とメモリ効率化を実現。
    * 自前実装は「前処理ロジック」「特徴量DSL」「Validation」に集中する。
* **Python bindings**: PyO3 + Maturin
* **CLI**: Rust (`clap`) で実装。Coreロジックを共有。
* **Data Interchange**: Apache Arrow (In-memory zero-copy)

## 5.2 ディレクトリ構造（予定）

```text
mlprep/
├── Cargo.toml        # Rust Workspace
├── pyproject.toml    # Python Project Definition
├── src/              # Rust Source (Core Logic)
│   ├── lib.rs
│   ├── engine.rs     # Polars Wrapper
│   └── ...
├── python/           # Python Bindings (src layout)
│   └── mlprep/
│       ├── __init__.py
│       └── ...
├── tests/            # Integration Tests
└── .github/          # CI/CD
```

## 5.3 モジュール構成

1. `io`: format support (csv, parquet, jsonl) via Polars
2. `schema`: type inference & validation
3. `dsl`: feature engineering DSL parser (YAML -> Expr)
4. `compute`: implementation of DSL using Polars Expressions
5. `validate`: quality checks logic
6. `lineage`: metadata tracking
7. `cli`: command line interface entry points

---

# 6. API仕様（例：最小公開面）

## 6.1 Python（例）

* `mlprep.read_csv(path, schema=None, infer_rows=10000, null_values=[...]) -> DataFrame`
* `DataFrame.filter(expr: str) -> DataFrame`
* `DataFrame.groupby(keys).agg(spec) -> DataFrame`
* `DataFrame.join(other, on=..., how="left") -> DataFrame`
* `mlprep.FeatureSet.from_yaml(path)`
* `features.fit(df) -> FeatureState`
* `features.transform(df, state) -> DataFrame`
* `mlprep.validate(df, checks) -> ValidationResult`

## 6.2 CLI（例）

* `mlprep run pipeline.yaml`
* `mlprep profile input --out report.json`
* `mlprep validate checks.yaml input`
* `mlprep features fit config.yaml --in train --out pack/`
* `mlprep features transform --state pack/feature_state.json --in test --out out.parquet`

---

# 7. 設定ファイル仕様（YAML/JSON）

## 7.1 pipeline.yaml（例）

```yaml
inputs:
  - path: "data/train.csv"
    format: csv
    schema: schemas/train_schema.yaml
    infer_rows: 10000
    null_values: ["NA", "", "-"]

steps:
  - cast:
      columns:
        age: int32
        signup_date: datetime[%Y-%m-%d]
  - fillna:
      strategy: mean
      columns: [age, income]
  - filter: "age >= 18"
  - features:
      config: features/user_features.yaml
  - validate:
      checks: checks/data_quality.yaml
      mode: quarantine

outputs:
  - path: "output/train_features.parquet"
    format: parquet
    compression: zstd
    partition_by: [region]

runtime:
  threads: auto
  cache: true
  memory_limit: 8GB
```

## 7.2 checks.yaml（例）

```yaml
columns:
  - name: user_id
    not_null: true
    unique: true
  - name: age
    not_null: true
    range: [0, 120]
  - name: email
    regex: "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"
  - name: status
    enum: ["active", "inactive", "pending"]

dataset:
  row_count_min: 1000
  row_count_max: 10000000
  duplicate_rate_max: 0.01
  missing_rate_max:
    user_id: 0.0
    age: 0.05
    email: 0.10
```

---

# 8. テスト仕様（品質要件）

* 単体テスト：各カーネル、型推定、I/O
* プロパティテスト：欠損/NaN/境界値、join整合
* ゴールデンテスト：同一入力→同一出力ハッシュ
* Python側：APIラウンドトリップ、例外変換
* 互換テスト：pandas/Polars/duckdbとの一致（対象操作に限定）

---

# 9. リリース/配布仕様

* **Build Tool**: `maturin` (PEP 517 backend for Rust)
* **CI/CD**: GitHub Actions
    * `ubuntu-latest`, `macos-latest`, `windows-latest` でのクロスビルド
    * `cibuildwheel` または `maturin-action` を使用
* **Versioning**: SemVer
* **Artifacts**:
    * PyPI (Wheels: manylinux, macos universal, windows)
    * GitHub Releases (CLI binaries)


---

# 10. 料金/エディション（仕様としての分離案）

* OSS（Core + Python + CLI 基本）
* Pro（例）：
  * S3/GCS入出力
  * HTMLレポート
  * 高度なdrift検知、ターゲットエンコーディング（リーク防止含む）
  * 分散/リモート実行、監査連携

