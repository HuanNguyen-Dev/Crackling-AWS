# Layer: `isslCandidateExtractor`

This layer contains the precompiled Linux x86_64 candidate Extractor used by
the Crackling AWS candidate-extraction pipeline. Each invocation hydrates one
raw ISSL bucket for one contiguous global off-target ID interval by combining
the bucket's IDs and occurrence counts with the corresponding signatures from
a catalogue partition.

The source implementation used for this binary is
`isslExtractCandidates.cpp`. Its build command is:

```text
  sudo docker run --rm \
      --platform linux/amd64 \
      --entrypoint /bin/bash \
      -v "$PWD:/src" \
      -w /src \
      public.ecr.aws/lambda/python:3.10 \
      -lc '
        yum install -y gcc-c++ &&
        g++ -o extractor \
          isslExtractCandidates.cpp \
          -O3 \
          -std=c++11 \
          -static-libgcc \
          -static-libstdc++ &&
        chmod 755 extractor &&
        file extractor &&
        ldd extractor
      '
```

The `isslCandidateExtractor` layer was introduced into this AWS repository in
commit `8b4e66d` (`Feature: Added candidate extraction lambda pipeline`). The
precompiled `extractor` binary was produced outside this repository from the
supplied local
`isslExtractCandidates.cpp`; that source directory is:
https://github.com/HuanNguyen-Dev/Crackling/commit/65e5b69736d3f88ee17a5d63dfdeb0f797b38496,
in the commit `65e5b69736d3f88ee17a5d63dfdeb0f797b38496`.

The Extractor writes headerless 16-byte little-endian records matching
`<QII`: a `uint64` signature, `uint32` global off-target ID, and `uint32`
occurrence count. The candidate Mapper consumes these hydrated records during
off-target scoring.
