use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};

use super::registration::WorkspaceSeed;
use super::types::{FlowWorkspaceMapping, WorkspaceError};
use super::workspace::WorkspaceStore;

impl WorkspaceStore {
    pub(super) fn project_directory(
        &self,
        execution_root: &Path,
        source_root: &Path,
        imported_paths: &BTreeSet<PathBuf>,
        delete_missing: bool,
    ) -> Result<(), WorkspaceError> {
        let mut files = Vec::new();
        if execution_root
            .try_exists()
            .map_err(|source| WorkspaceError::io(execution_root, source))?
        {
            collect_files(execution_root, execution_root, &mut files)?;
        }
        for (relative, source) in files {
            let destination = source_root.join(checked_relative(&relative)?);
            reject_symlink_components(source_root, &relative)?;
            if let Some(parent) = destination.parent() {
                std::fs::create_dir_all(parent).map_err(|source| WorkspaceError::io(parent, source))?;
            }
            std::fs::copy(source, &destination).map_err(|source| WorkspaceError::io(destination, source))?;
        }
        if delete_missing {
            for relative in imported_paths {
                let source = source_root.join(relative);
                if !execution_root.join(relative).exists() {
                    let Ok(metadata) = std::fs::symlink_metadata(&source) else {
                        continue;
                    };
                    if metadata.file_type().is_symlink() {
                        return Err(WorkspaceError::SymbolicLink { path: source });
                    }
                    reject_symlink_components(source_root, relative)?;
                    std::fs::remove_file(&source).map_err(|source_error| WorkspaceError::io(&source, source_error))?;
                }
            }
        }
        Ok(())
    }

    pub(super) fn read_source_text(&self, root: &Path, relative: &str) -> Result<String, WorkspaceError> {
        let relative = checked_relative(Path::new(relative))?;
        let target = root.join(&relative);
        let mut current = root.to_path_buf();
        for component in relative.components() {
            current.push(component.as_os_str());
            let metadata =
                std::fs::symlink_metadata(&current).map_err(|source| WorkspaceError::io(&current, source))?;
            if metadata.file_type().is_symlink() {
                return Err(WorkspaceError::SymbolicLink { path: current });
            }
        }
        let bytes = std::fs::read(&target).map_err(|source| WorkspaceError::io(&target, source))?;
        String::from_utf8(bytes).map_err(|_| WorkspaceError::InvalidUtf8 { path: target })
    }

    pub(super) fn write_source_text(&self, root: &Path, relative: &str, source: &str) -> Result<(), WorkspaceError> {
        let relative = checked_relative(Path::new(relative))?;
        let target = root.join(&relative);
        reject_symlink_components(root, &relative)?;
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent).map_err(|source| WorkspaceError::io(parent, source))?;
        }
        std::fs::write(&target, source).map_err(|source| WorkspaceError::io(target, source))
    }
}

pub(super) fn collect_files(
    root: &Path,
    current: &Path,
    files: &mut Vec<(PathBuf, PathBuf)>,
) -> Result<(), WorkspaceError> {
    for entry in sorted_entries(current)? {
        let path = entry.path();
        let metadata = std::fs::symlink_metadata(&path).map_err(|source| WorkspaceError::io(&path, source))?;
        if metadata.file_type().is_symlink() {
            return Err(WorkspaceError::SymbolicLink { path });
        }
        if metadata.is_dir() {
            collect_files(root, &path, files)?;
        } else if metadata.is_file() {
            files.push((
                path.strip_prefix(root)
                    .expect("walk remains below root")
                    .to_path_buf(),
                path,
            ));
        } else {
            return Err(WorkspaceError::UnsupportedSource { path });
        }
    }
    Ok(())
}

pub(super) fn collect_artifact_paths(
    execution_root: &Path,
    current: &Path,
    paths: &mut Vec<String>,
) -> Result<(), WorkspaceError> {
    let metadata = match std::fs::symlink_metadata(current) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(WorkspaceError::io(current, source));
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Ok(());
    }
    for entry in sorted_entries(current)? {
        let path = entry.path();
        let metadata = std::fs::symlink_metadata(&path).map_err(|source| WorkspaceError::io(&path, source))?;
        if metadata.file_type().is_symlink() {
            continue;
        }
        if metadata.is_dir() {
            collect_artifact_paths(execution_root, &path, paths)?;
        } else if metadata.is_file() {
            paths.push(
                path.strip_prefix(execution_root)
                    .expect("artifact walk remains below execution root")
                    .to_string_lossy()
                    .replace('\\', "/"),
            );
        }
    }
    Ok(())
}

pub(crate) fn checked_relative(path: &Path) -> Result<PathBuf, WorkspaceError> {
    let mut result = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => result.push(part),
            _ => {
                return Err(WorkspaceError::PathEscapesRoot {
                    path: path.to_path_buf(),
                });
            }
        }
    }
    (!result.as_os_str().is_empty())
        .then_some(result)
        .ok_or_else(|| WorkspaceError::PathEscapesRoot {
            path: path.to_path_buf(),
        })
}

pub(super) fn mapping_destination(destination: &str) -> Result<PathBuf, WorkspaceError> {
    if destination.is_empty() {
        return Ok(PathBuf::new());
    }
    if destination.trim() != destination
        || destination.contains(['\\', ':', '\0'])
        || destination
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return Err(WorkspaceError::PathEscapesRoot {
            path: PathBuf::from(destination),
        });
    }
    checked_relative(Path::new(destination))
}

pub(super) fn validate_mappings(mappings: &[FlowWorkspaceMapping]) -> Result<(), WorkspaceError> {
    let mut destinations: Vec<PathBuf> = Vec::new();
    for mapping in mappings {
        let destination = mapping_destination(&mapping.destination)?;
        for previous in &destinations {
            if destination.starts_with(previous) || previous.starts_with(&destination) {
                return Err(WorkspaceError::OverlappingMappings {
                    first: previous.display().to_string(),
                    second: destination.display().to_string(),
                });
            }
        }
        destinations.push(destination);
    }
    Ok(())
}

pub(super) fn path_name(path: &Path) -> Result<String, WorkspaceError> {
    checked_relative(path)?
        .to_str()
        .map(str::to_owned)
        .ok_or_else(|| WorkspaceError::PathEscapesRoot {
            path: path.to_path_buf(),
        })
}

pub(super) fn mapped_input_path(role: &str, path: &Path) -> PathBuf {
    path.strip_prefix(role)
        .map_or_else(|_| Path::new(role).join(path), Path::to_path_buf)
}

pub(super) fn seed_workspace_seeds(seeds: &[WorkspaceSeed], execution_root: &Path) -> Result<(), WorkspaceError> {
    // The root itself must not redirect writes outside this execution.
    let root_metadata =
        std::fs::symlink_metadata(execution_root).map_err(|source| WorkspaceError::io(execution_root, source))?;
    if root_metadata.file_type().is_symlink() {
        return Err(WorkspaceError::SymbolicLink {
            path: execution_root.to_path_buf(),
        });
    }
    let mut paths_by_root = BTreeMap::<PathBuf, BTreeSet<PathBuf>>::new();
    for seed in seeds {
        paths_by_root
            .entry(seed.source_root.clone())
            .or_default()
            .extend(
                seed.relative_paths
                    .iter()
                    .map(|path| checked_relative(Path::new(path)))
                    .collect::<Result<BTreeSet<_>, _>>()?,
            );
    }
    for (source_root, paths) in paths_by_root {
        match std::fs::symlink_metadata(&source_root) {
            Ok(metadata) if metadata.file_type().is_symlink() => continue,
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => return Err(WorkspaceError::io(&source_root, source)),
        }
        for relative in paths {
            let mut source_path = source_root.clone();
            let mut absent = false;
            for part in relative.components() {
                source_path.push(part);
                match std::fs::symlink_metadata(&source_path) {
                    Ok(metadata) if metadata.file_type().is_symlink() => {
                        absent = true;
                        break;
                    }
                    Ok(_) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        absent = true;
                        break;
                    }
                    Err(source) => return Err(WorkspaceError::io(source_path, source)),
                }
            }
            if !absent {
                copy_workspace_entry(&source_path, &execution_root.join(relative), execution_root)?;
            }
        }
    }
    Ok(())
}

fn sorted_entries(current: &Path) -> Result<Vec<std::fs::DirEntry>, WorkspaceError> {
    let mut entries = std::fs::read_dir(current)
        .map_err(|source| WorkspaceError::io(current, source))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| WorkspaceError::io(current, source))?;
    entries.sort_by_key(std::fs::DirEntry::path);
    Ok(entries)
}

fn copy_workspace_entry(source: &Path, destination: &Path, root: &Path) -> Result<(), WorkspaceError> {
    let metadata = match std::fs::symlink_metadata(source) {
        Ok(metadata) if metadata.file_type().is_symlink() => return Ok(()),
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source_error) => {
            return Err(WorkspaceError::io(source, source_error));
        }
    };
    let parent = destination
        .parent()
        .ok_or_else(|| WorkspaceError::PathEscapesRoot {
            path: destination.to_path_buf(),
        })?;
    let relative_parent = parent
        .strip_prefix(root)
        .map_err(|_| WorkspaceError::PathEscapesRoot {
            path: destination.to_path_buf(),
        })?;
    let mut directory = root.to_path_buf();
    for component in relative_parent.components() {
        directory.push(component);
        match std::fs::symlink_metadata(&directory) {
            Ok(entry) if entry.file_type().is_symlink() => {
                return Err(WorkspaceError::SymbolicLink { path: directory });
            }
            Ok(entry) if !entry.is_dir() => return Err(WorkspaceError::UnsupportedSource { path: directory }),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir(&directory).map_err(|source| WorkspaceError::io(&directory, source))?;
            }
            Err(source) => {
                return Err(WorkspaceError::io(directory, source));
            }
        }
    }
    match std::fs::symlink_metadata(destination) {
        Ok(entry) if entry.file_type().is_symlink() => {
            std::fs::remove_file(destination).map_err(|source| WorkspaceError::io(destination, source))?;
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(WorkspaceError::io(destination, source));
        }
    }
    if metadata.is_dir() {
        std::fs::create_dir_all(destination).map_err(|source| WorkspaceError::io(destination, source))?;
        for entry in std::fs::read_dir(source).map_err(|source_error| WorkspaceError::io(source, source_error))? {
            let entry = entry.map_err(|source_error| WorkspaceError::io(source, source_error))?;
            copy_workspace_entry(&entry.path(), &destination.join(entry.file_name()), root)?;
        }
    } else if metadata.is_file() {
        std::fs::copy(source, destination).map_err(|source_error| WorkspaceError::io(destination, source_error))?;
    } else {
        return Err(WorkspaceError::UnsupportedSource {
            path: source.to_path_buf(),
        });
    }
    Ok(())
}

fn reject_symlink_components(root: &Path, relative: &Path) -> Result<(), WorkspaceError> {
    let mut current = root.to_path_buf();
    for component in relative.components() {
        current.push(component.as_os_str());
        if std::fs::symlink_metadata(&current).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
            return Err(WorkspaceError::SymbolicLink { path: current });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::types::FlowWorkspaceMappingMode;
    use super::*;

    #[test]
    fn mapping_destinations_reject_overlap_and_traversal() {
        let mapping = |destination: &str| FlowWorkspaceMapping {
            directory: "research".to_owned(),
            destination: destination.to_owned(),
            mode: FlowWorkspaceMappingMode::ReadOnly,
        };
        assert!(validate_mappings(&[mapping("notes"), mapping("reports")]).is_ok());
        assert!(validate_mappings(&[mapping("")]).is_ok());
        for destinations in [["", "notes"], ["notes", "notes/sub"], ["notes", "notes"]] {
            assert!(matches!(
                validate_mappings(&destinations.map(mapping)),
                Err(WorkspaceError::OverlappingMappings { .. })
            ));
        }
        for path in ["../notes", "/notes", "notes/../reports", "C:\\notes", "notes//reports"] {
            assert!(matches!(
                validate_mappings(&[mapping(path)]),
                Err(WorkspaceError::PathEscapesRoot { .. })
            ));
        }
    }
}
