package flatten

// RequireOwnershipCap previously verified that the process held CAP_CHOWN so
// extracted rootfs entries could be chowned on the host filesystem. Layer
// application now preserves numeric UID/GID, mode bits, and device numbers in
// an in-memory virtual overlay tree without host filesystem chown or mknod
// calls, so rootless execution is supported and this check is a no-op.
func RequireOwnershipCap() error {
	return nil
}
