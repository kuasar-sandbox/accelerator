package image

// ConversionSemantics identifies the generator policy used for conversion
// result caches, not the EROFS Plan schema or artifact content identity.
// This policy applies verified OCI layers using umoci, preserves numeric
// ownership and OCI replacement/link semantics. umoci CleanPath normalizes layer
// paths within the image root; names are made relative before accounting, and
// entries naming the extraction root are skipped, matching containerd/Moby.
// Layer-based exports use a 0755, UID/GID 0:0 root independently of the staging
// directory's host ownership; directory inputs preserve their root metadata.
// It uses go-erofs to emit single-device,
// uncompressed EROFS with 4 KiB full-SHA256 dedup, fixed UUID/time and no
// xattrs (including capabilities). Runtime config and artifact/encryption
// identity rules remain unchanged. The root-entry compatibility fix retains v1
// for existing consumers and conversion-cache compatibility; cached v1 artifacts
// remain reusable and are not regenerated to normalize their root metadata.
// Missing and different values are conversion misses; explicit old artifacts
// and the original compressed-layer cache remain usable.
const ConversionSemantics = "umoci-go-erofs-no-xattr-v1"
