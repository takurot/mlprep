use crate::errors::{MlPrepError, MlPrepResult};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Default)]
pub struct SecurityConfig {
    pub allowed_paths: Option<Vec<PathBuf>>,
    pub mask_columns: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct SecurityContext {
    allowed_paths: Option<Vec<PathBuf>>,
    masker: Masker,
}

impl SecurityContext {
    pub fn new(config: SecurityConfig) -> MlPrepResult<Self> {
        let allowed_paths = if let Some(paths) = config.allowed_paths {
            let mut canonical_paths = Vec::new();
            for p in paths {
                // If path doesn't exist, this might fail. CLI args should ideally be existing paths?
                // Or we canonicalize as much as possible. For now, strict check: must exist to be an allowed root.
                if let Ok(canonical) = p.canonicalize() {
                    canonical_paths.push(canonical);
                } else {
                    // Warning or Error? Given this is security config, error is safer.
                    return Err(MlPrepError::IoError(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("Allowed path not found: {:?}", p),
                    )));
                }
            }
            Some(canonical_paths)
        } else {
            None
        };

        Ok(Self {
            allowed_paths,
            masker: Masker::new(config.mask_columns.unwrap_or_default()),
        })
    }

    pub fn validate_path<P: AsRef<Path>>(&self, path: P) -> MlPrepResult<()> {
        if let Some(allowed) = &self.allowed_paths {
            let path_ref = path.as_ref();

            // Attempt to resolve target path
            let target = if path_ref.exists() {
                path_ref.canonicalize().map_err(MlPrepError::IoError)?
            } else {
                // For output files or non-existent files
                if let Some(parent) = path_ref.parent() {
                    if parent.exists() {
                        parent
                            .canonicalize()
                            .map_err(MlPrepError::IoError)?
                            .join(path_ref.file_name().unwrap())
                    } else {
                        // Parent doesn't exist either. This is tricky.
                        // But for now, let's assume at least the directory structure should exist or be allowed.
                        // Simplest strict approach: Resolve absolute path and check string prefix?
                        // Canonicalize handles symlinks which is crucial.
                        // If parent doesn't exist, we can't safely canonicalize.
                        // Maybe we fallback to absolute path if not exists, but that's risky with symlinks.
                        // Better to fail if we can't verifying safety.
                        return Err(MlPrepError::Unknown(anyhow::anyhow!("Path or parent directory does not exist, cannot verify security clearance: {:?}", path_ref)));
                    }
                } else {
                    // Path is likely root or empty, which is weird.
                    return Err(MlPrepError::Unknown(anyhow::anyhow!(
                        "Cannot resolve path parent: {:?}",
                        path_ref
                    )));
                }
            };

            let is_allowed = allowed
                .iter()
                .any(|allowed_base| target.starts_with(allowed_base));

            if !is_allowed {
                return Err(MlPrepError::Unknown(anyhow::anyhow!(
                    "Access denied: Path {:?} is not in allowed paths {:?}",
                    target,
                    allowed
                )));
            }
        }
        Ok(())
    }

    pub fn masker(&self) -> &Masker {
        &self.masker
    }
}

#[derive(Debug, Clone)]
pub struct Masker {
    columns: HashSet<String>,
}

impl Masker {
    pub fn new(columns: Vec<String>) -> Self {
        Self {
            columns: columns.into_iter().collect(),
        }
    }

    pub fn is_masked(&self, column: &str) -> bool {
        self.columns.contains(column)
    }

    pub fn mask_value(&self, column: &str, value: &str) -> String {
        if self.is_masked(column) {
            "***".to_string()
        } else {
            value.to_string()
        }
    }
}
