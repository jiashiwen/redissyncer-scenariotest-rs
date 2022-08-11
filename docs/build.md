# 构建

## 本机构建

* ubuntu

```shell
sudo apt install pkg-config
sudo apt-get install libudev-dev
cargo build --release
```

## 交叉编译

[参考文档]

* https://www.cnblogs.com/007sx/p/15191400.html
* https://kerkour.com/rust-cross-compilation

### 环境配置

* os ubuntu 20.04

* 安装Rust

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

* 安装 cross

https://github.com/cross-rs/cross

```shell
cargo install -f cross
```

* 编译 window 可执行文件

```shell
brew install filosottile/musl-cross/musl-cross
```