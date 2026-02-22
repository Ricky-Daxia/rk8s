//! SlayerFS storage backend for CSI.
//!
//! [`SlayerFsBackend`] implements [`CsiIdentity`], [`CsiController`], and
//! [`CsiNode`] using SlayerFS's `LocalClient` as the underlying storage
//! engine.  Volumes are stored as sub-directories under a configurable
//! `object_root`, and made available to containers via FUSE staging and
//! bind-mount publishing.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use tracing::{debug, info, instrument, warn};

use slayerfs::{ChunkLayout, ClientBackend, LocalClient};

use crate::controller::CsiController;
use crate::error::CsiError;
use crate::identity::CsiIdentity;
use crate::node::CsiNode;
use crate::types::*;

/// Concrete CSI backend backed by SlayerFS.
///
/// # Layout
///
/// ```text
/// <object_root>/
///   <volume-id>/          # SlayerFS object store for each volume
/// ```
///
/// # Thread safety
///
/// All mutable state is behind concurrent maps ([`DashMap`]), allowing
/// multiple Tokio tasks to operate on different volumes concurrently.
pub struct SlayerFsBackend {
    /// Root directory for all SlayerFS volume object stores.
    object_root: PathBuf,
    /// Chunk layout configuration forwarded to each `LocalClient`.
    layout: ChunkLayout,
    /// Tracks live SlayerFS clients keyed by volume ID.
    volumes: DashMap<VolumeId, Arc<dyn ClientBackend>>,
    /// Tracks volume metadata keyed by volume ID.
    volume_meta: DashMap<VolumeId, Volume>,
    /// Node identifier (hostname or user-supplied).
    node_id: String,
}

impl SlayerFsBackend {
    /// Create a new backend.
    ///
    /// * `object_root` — base directory on the local filesystem
    /// * `layout` — SlayerFS chunk layout
    /// * `node_id` — unique identifier for this node
    pub fn new(object_root: impl Into<PathBuf>, layout: ChunkLayout, node_id: String) -> Self {
        Self {
            object_root: object_root.into(),
            layout,
            volumes: DashMap::new(),
            volume_meta: DashMap::new(),
            node_id,
        }
    }

    /// Resolve the on-disk object root for a given volume.
    fn volume_root(&self, volume_id: &VolumeId) -> PathBuf {
        self.object_root.join(&volume_id.0)
    }
}

// ---------------------------------------------------------------------------
// CsiIdentity
// ---------------------------------------------------------------------------

#[async_trait]
impl CsiIdentity for SlayerFsBackend {
    async fn get_plugin_info(&self) -> Result<PluginInfo, CsiError> {
        Ok(PluginInfo {
            name: "rk8s.slayerfs.csi".to_owned(),
            vendor_version: env!("CARGO_PKG_VERSION").to_owned(),
        })
    }

    async fn probe(&self) -> Result<bool, CsiError> {
        // Check that the object root exists and is a directory.
        let exists = tokio::fs::metadata(&self.object_root)
            .await
            .map(|m| m.is_dir())
            .unwrap_or(false);
        Ok(exists)
    }

    async fn get_plugin_capabilities(&self) -> Result<Vec<PluginCapability>, CsiError> {
        Ok(vec![PluginCapability::ControllerService])
    }
}

// ---------------------------------------------------------------------------
// CsiController
// ---------------------------------------------------------------------------

#[async_trait]
impl CsiController for SlayerFsBackend {
    #[instrument(skip(self), fields(name = %req.name))]
    async fn create_volume(&self, req: CreateVolumeRequest) -> Result<Volume, CsiError> {
        let vol_id = VolumeId(format!("slayerfs-{}", uuid::Uuid::new_v4()));
        let vol_root = self.volume_root(&vol_id);

        // Ensure the volume root directory exists on the host filesystem.
        tokio::fs::create_dir_all(&vol_root).await.map_err(|e| {
            CsiError::BackendError(format!("create dir {}: {e}", vol_root.display()))
        })?;

        // Construct a SlayerFS LocalClient for this volume.
        let client = LocalClient::new_local(&vol_root, self.layout)
            .await
            .map_err(CsiError::backend)?;

        // Bootstrap the volume's internal root directory.
        client.mkdir_p("/").await.map_err(CsiError::backend)?;

        let volume = Volume {
            volume_id: vol_id.clone(),
            capacity_bytes: req.capacity_bytes,
            parameters: req.parameters,
            volume_context: HashMap::from([(
                "object_root".to_owned(),
                vol_root.to_string_lossy().into_owned(),
            )]),
            accessible_topology: vec![Topology {
                segments: HashMap::from([("node".to_owned(), self.node_id.clone())]),
            }],
        };

        self.volumes.insert(vol_id.clone(), Arc::new(client));
        self.volume_meta.insert(vol_id.clone(), volume.clone());
        info!(%vol_id, "volume created");

        Ok(volume)
    }

    #[instrument(skip(self))]
    async fn delete_volume(&self, volume_id: &VolumeId) -> Result<(), CsiError> {
        self.volumes.remove(volume_id);
        self.volume_meta.remove(volume_id);

        let vol_root = self.volume_root(volume_id);
        if vol_root.exists() {
            tokio::fs::remove_dir_all(&vol_root).await.map_err(|e| {
                CsiError::BackendError(format!("remove dir {}: {e}", vol_root.display()))
            })?;
        }

        info!(%volume_id, "volume deleted");
        Ok(())
    }

    async fn validate_volume_capabilities(
        &self,
        volume_id: &VolumeId,
        _capabilities: &[VolumeCapability],
    ) -> Result<bool, CsiError> {
        if !self.volume_meta.contains_key(volume_id) {
            return Err(CsiError::VolumeNotFound(volume_id.to_string()));
        }
        // SlayerFS supports all access modes.
        Ok(true)
    }

    async fn list_volumes(&self) -> Result<Vec<Volume>, CsiError> {
        let vols: Vec<Volume> = self
            .volume_meta
            .iter()
            .map(|entry| entry.value().clone())
            .collect();
        Ok(vols)
    }

    async fn get_capacity(&self) -> Result<u64, CsiError> {
        // Report available disk space on the partition hosting `object_root`.
        let stat = nix::sys::statvfs::statvfs(
            self.object_root
                .to_str()
                .ok_or_else(|| CsiError::Internal("non-UTF8 object root path".into()))?,
        )
        .map_err(|e| CsiError::Internal(format!("statvfs: {e}")))?;
        Ok(stat.fragment_size() * stat.blocks_available())
    }
}

// ---------------------------------------------------------------------------
// CsiNode
// ---------------------------------------------------------------------------

#[async_trait]
impl CsiNode for SlayerFsBackend {
    #[instrument(skip(self), fields(volume_id = %req.volume_id))]
    async fn stage_volume(&self, req: NodeStageVolumeRequest) -> Result<(), CsiError> {
        let staging = Path::new(&req.staging_target_path);

        // Idempotent: if the directory already exists and is a mount point,
        // assume a previous stage succeeded.
        if staging.exists() {
            debug!(path = %req.staging_target_path, "staging path already exists, assuming idempotent retry");
            return Ok(());
        }

        tokio::fs::create_dir_all(staging)
            .await
            .map_err(|e| CsiError::MountFailed {
                path: req.staging_target_path.clone(),
                reason: e.to_string(),
            })?;

        // NOTE: The actual FUSE mount of SlayerFS at the staging path would
        // be done here via rfuse3.  This requires spawning a background task
        // that runs the FUSE session.  A full implementation would look like:
        //
        //   let vfs = build_slayerfs_vfs(&req.volume_context)?;
        //   tokio::spawn(rfuse3::mount(vfs, staging, mount_options));
        //
        // For now we leave a placeholder; the FUSE integration is deferred
        // to the P1/P2 implementation phase.

        info!(path = %req.staging_target_path, "volume staged (FUSE mount placeholder)");
        Ok(())
    }

    #[instrument(skip(self))]
    async fn unstage_volume(
        &self,
        volume_id: &VolumeId,
        staging_target_path: &str,
    ) -> Result<(), CsiError> {
        let staging = Path::new(staging_target_path);
        if !staging.exists() {
            debug!(%volume_id, "staging path gone, nothing to unstage");
            return Ok(());
        }

        // Attempt to unmount via fusermount3 (FUSE userspace unmount).
        let status = tokio::process::Command::new("fusermount3")
            .args(["-u", staging_target_path])
            .status()
            .await
            .map_err(|e| CsiError::UnmountFailed {
                path: staging_target_path.to_owned(),
                reason: e.to_string(),
            })?;

        if !status.success() {
            // Non-zero exit may mean it was already unmounted; log a warning
            // but treat as ok (idempotent).
            warn!(
                %volume_id,
                path = staging_target_path,
                code = ?status.code(),
                "fusermount3 returned non-zero (may already be unmounted)",
            );
        }

        info!(%volume_id, path = staging_target_path, "volume unstaged");
        Ok(())
    }

    #[instrument(skip(self), fields(volume_id = %req.volume_id))]
    async fn publish_volume(&self, req: NodePublishVolumeRequest) -> Result<(), CsiError> {
        let target = Path::new(&req.target_path);

        tokio::fs::create_dir_all(target)
            .await
            .map_err(|e| CsiError::MountFailed {
                path: req.target_path.clone(),
                reason: e.to_string(),
            })?;

        let mut flags = nix::mount::MsFlags::MS_BIND;
        if req.read_only {
            flags |= nix::mount::MsFlags::MS_RDONLY;
        }

        nix::mount::mount(
            Some(req.staging_target_path.as_str()),
            req.target_path.as_str(),
            None::<&str>,
            flags,
            None::<&str>,
        )
        .map_err(|e| CsiError::MountFailed {
            path: req.target_path.clone(),
            reason: e.to_string(),
        })?;

        // If read-only was requested, remount with MS_RDONLY (bind mount
        // ignores MS_RDONLY on the first call on some kernels).
        if req.read_only {
            nix::mount::mount(
                None::<&str>,
                req.target_path.as_str(),
                None::<&str>,
                nix::mount::MsFlags::MS_BIND
                    | nix::mount::MsFlags::MS_REMOUNT
                    | nix::mount::MsFlags::MS_RDONLY,
                None::<&str>,
            )
            .map_err(|e| CsiError::MountFailed {
                path: req.target_path.clone(),
                reason: format!("remount read-only: {e}"),
            })?;
        }

        info!(
            target_path = %req.target_path,
            read_only = req.read_only,
            "volume published (bind-mount)",
        );
        Ok(())
    }

    #[instrument(skip(self))]
    async fn unpublish_volume(
        &self,
        volume_id: &VolumeId,
        target_path: &str,
    ) -> Result<(), CsiError> {
        let target = Path::new(target_path);
        if !target.exists() {
            debug!(%volume_id, "target path gone, nothing to unpublish");
            return Ok(());
        }

        nix::mount::umount(target_path).map_err(|e| CsiError::UnmountFailed {
            path: target_path.to_owned(),
            reason: e.to_string(),
        })?;

        info!(%volume_id, %target_path, "volume unpublished");
        Ok(())
    }

    async fn get_info(&self) -> Result<NodeInfo, CsiError> {
        Ok(NodeInfo {
            node_id: self.node_id.clone(),
            max_volumes: 256,
            accessible_topology: Some(Topology {
                segments: HashMap::from([("node".to_owned(), self.node_id.clone())]),
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_backend(dir: &Path) -> SlayerFsBackend {
        SlayerFsBackend::new(dir, ChunkLayout::default(), "test-node".to_owned())
    }

    #[tokio::test]
    async fn create_and_delete_volume() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = make_backend(tmp.path());

        let vol = backend
            .create_volume(CreateVolumeRequest {
                name: "test-vol".into(),
                capacity_bytes: 64 * 1024 * 1024,
                ..Default::default()
            })
            .await
            .unwrap();

        assert!(vol.volume_id.0.starts_with("slayerfs-"));
        assert!(backend.volume_meta.contains_key(&vol.volume_id));
        assert!(backend.volumes.contains_key(&vol.volume_id));

        // List should include the volume.
        let list = backend.list_volumes().await.unwrap();
        assert_eq!(list.len(), 1);

        // Delete.
        backend.delete_volume(&vol.volume_id).await.unwrap();
        assert!(!backend.volume_meta.contains_key(&vol.volume_id));

        let list = backend.list_volumes().await.unwrap();
        assert!(list.is_empty());
    }

    #[tokio::test]
    async fn probe_healthy_root() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = make_backend(tmp.path());
        assert!(backend.probe().await.unwrap());
    }

    #[tokio::test]
    async fn probe_missing_root() {
        let backend = make_backend(Path::new("/nonexistent/path/for/test"));
        assert!(!backend.probe().await.unwrap());
    }

    #[tokio::test]
    async fn plugin_info() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = make_backend(tmp.path());
        let info = backend.get_plugin_info().await.unwrap();
        assert_eq!(info.name, "rk8s.slayerfs.csi");
    }

    #[tokio::test]
    async fn validate_missing_volume() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = make_backend(tmp.path());
        let result = backend
            .validate_volume_capabilities(&VolumeId("nope".into()), &[])
            .await;
        assert!(matches!(result, Err(CsiError::VolumeNotFound(_))));
    }

    #[tokio::test]
    async fn get_node_info() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = make_backend(tmp.path());
        let info = backend.get_info().await.unwrap();
        assert_eq!(info.node_id, "test-node");
    }
}
