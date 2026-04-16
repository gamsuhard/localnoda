# FullNode config template notes

This directory contains an **overlay fragment**, not a full `java-tron` config.

Why:

- official `config.conf` is large and release-specific
- Block 01 does not yet freeze the exact FullNode artifact/version
- only the event subscription contour is stable enough to template now

Expected later workflow in Block 02/03:

1. obtain official release config
2. copy it to an active local config path
3. merge the overlay fragment from `config.conf.overlay.template`
4. resolve placeholders from the frozen manifests in `artifacts/manifests/`
