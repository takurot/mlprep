# タスク実行プロンプト

@docs/PLAN.md を参照して、指定されたPR（例: PR-01）の実装を進めてください。

## 実装フロー

### 1. 開発環境の準備
- 仮想環境の作成と有効化（必須）
  ```bash
  python3 -m venv .venv
  source .venv/bin/activate
  pip install -e ".[dev]" maturin
  ```
- ブランチ作成
  - `feature/<pr番号>-<簡潔な説明>` の形式
  - 例: `feature/pr-02-polars-integration`
  - `main` ブランチから分岐

### 2. TDD（テスト駆動開発）で実装
- **Red**: まず失敗するテストを書く
- **Green**: テストが通る最小限のコードを実装
- **Refactor**: コードを整理・改善
- ユニットテスト（Rust/Python）と統合テストの両方を作成

### 3. テストの実行
* **Rust**: `cargo test`
* **Python**: 
  ```bash
  source .venv/bin/activate
  maturin develop --extras dev
  pytest python/tests
  ```
* **E2E**: `cargo run -- run pipeline.yaml` (CLI実装後)

### 4. ベンチマーク確認
```bash
# Releaseビルドで実行
cargo build --release
python scripts/benchmark.py --format markdown
```
* 目標値（CSV 1GB < 5s）と比較
* 性能が大幅に劣化している場合は原因を調査:
  * `cargo flamegraph` でプロファイル取得
  * Polarsの設定（`n_rows`, streaming）を確認
  * メモリ使用量を `heaptrack` で確認

### 5. コード品質の確認
```bash
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
ruff check python/
ruff format python/
```

### 6. PLAN.md の更新
- 実装したPRのステータスを `[DONE]` に更新
- 必要に応じて、実装中に判明した詳細（技術的負債や次フェーズへのTODO）を追記

### 7. コミット & プッシュ
- コミットメッセージ形式: `<type>(<scope>): <description>`
  - type: `feat`, `fix`, `test`, `docs`, `refactor`, `chore`, `ci`
  - 例: `feat(io): implement read_csv using polars lazyframe`
- 適切な粒度でコミットを分割

### 8. Pull Request 作成
```bash
gh pr create --title "<PR番号>: <タイトル>" --body "<説明>"
```
- PRテンプレートに従って記述
- 関連するIssueやPRをリンク

### 9. CI結果の確認と対応
```bash
gh pr checks   # CIの状態確認
```
- CIが失敗した場合は原因を特定して修正
- 全てのチェックがパスするまで繰り返す

## チェックリスト

- [ ] ブランチを `main` から作成した
- [ ] テストを先に書いた（TDD）
- [ ] 全てのテストがパスする
- [ ] ベンチマーク確認済み（目標値との乖離なし）
- [ ] `clippy` / `ruff` がエラーなし
- [ ] PLAN.md を更新した
- [ ] コミットメッセージが適切
- [ ] PRを作成した
- [ ] CIが全てパス

## 注意事項

- 既存のテストを壊さないこと
- Polarsの依存関係は `Cargo.toml` と `pyproject.toml` で整合性を保つこと
- macOS/Linux を優先
