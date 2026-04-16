# Provider helper scripts

These scripts bring a **minimal** provider helper contour into the bounded
extractor project without importing the full cleanroom runtime.

Current purpose:

- fetch `trongridApiKey` and `chainbaseApiKey` from AWS Secrets Manager
- materialize a local gitignored runtime env file
- probe provider connectivity when needed

Boundaries:

- provider APIs are helper/validation surfaces only
- they do not replace the primary extraction path:
  `local node -> custom raw sink -> S3 buffer -> Frankfurt loader -> private ClickHouse`
- secrets must stay in runtime-only files, never in tracked source files

