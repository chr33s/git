/** Private restore records are never application backup entries. */
export { key as fenceKey, Marker as FenceMarker } from "../src/host/CloudflareBackupFence.ts";
export const checkpointKey = "git-plus:object-import:v1";
export const legacyCheckpointKey = "git-plus:sql-import:v1";
export const restoreKey = "git-plus:backup-restore:v1";
