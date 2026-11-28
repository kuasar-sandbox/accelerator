// Package vcdiff implements a pure-Go, cgo-free VCDIFF (RFC 3284) delta encoder
// and decoder, aiming for on-the-wire compatibility with Google's open-vcdiff
// (the standard interchange format: version byte 0x00, the default instruction
// code table, and no secondary compression).
//
// The address space for a COPY instruction is U = S ‖ T, where S is the window's
// source segment (from the source file for a VCD_SOURCE window, from the already
// produced target for a VCD_TARGET window, or empty) and T is the target bytes
// produced so far in the window. A COPY at address a copies forward, byte by
// byte, so legal self-overlap (periodic patterns) reconstructs correctly.
//
// The encoder emits a single window per call using only the default code table's
// single-instruction opcodes (ADD, RUN, COPY) with an explicit size integer —
// the simplest fully standard subset. The decoder is complete: it handles every
// default-table opcode (including the double ADD+COPY / COPY+ADD forms and the
// size-baked variants), all nine COPY address modes, VCD_SOURCE and VCD_TARGET
// windows, an application header, and the open-vcdiff per-window Adler32 checksum
// extension. Secondary compression and custom code tables are rejected with a
// clear error rather than silently mis-decoded.
package vcdiff
