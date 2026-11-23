use std::{
    fs,
    path::{Path, PathBuf},
};

use super::super::error::DiscussError;

pub(in crate::discuss) struct RenderedFile {
    pub(in crate::discuss) relative_path: PathBuf,
    pub(in crate::discuss) contents: String,
}

pub(super) fn write_rendered_files(plugin: &str, root: &Path, files: &[RenderedFile]) -> Result<(), DiscussError> {
    for file in files {
        let path = root.join(&file.relative_path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| DiscussError::WorkspaceIo {
                plugin: plugin.to_owned(),
                path: parent.to_string_lossy().into_owned(),
                message: error.to_string(),
            })?;
        }
        fs::write(&path, &file.contents).map_err(|error| DiscussError::WorkspaceIo {
            plugin: plugin.to_owned(),
            path: path.to_string_lossy().into_owned(),
            message: error.to_string(),
        })?;
    }
    Ok(())
}
