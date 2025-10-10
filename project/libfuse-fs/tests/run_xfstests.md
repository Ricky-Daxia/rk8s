## What is xfstests

xfstests is a comprehensive filesystem testing suite widely used in Linux development.

It provides hundreds of automated tests to validate correctness, robustness, and compatibility of filesystems by simulating various operations and edge cases.

xfstests helps developers quickly identify regressions and bugs in filesystem implementations.

## How to run xfstests

1. build test program and install it

```bash
cargo build --example overlay --release
sudo install -t /usr/sbin/ -m 700 ../../target/release/examples/overlay 
```

2. execute test script

```bash
chmod +x scripts/xfstests_overlay.sh
sudo ./scripts/xfstests_overlay.sh
```

If you want to execute single test case, go to the directory where `xfstests-dev` is pulled:

```bash
cd /tmp/xfstests-dev
sudo ./check -fuse generic/087
```

## References

See [fuse-backend-rs xfstests results](https://github.com/cloud-hypervisor/fuse-backend-rs/blob/master/tests/scripts/xfstests_overlay.exclude) for reference their test statistics.