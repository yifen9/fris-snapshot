set shell := ["bash", "-euo", "pipefail", "-c"]
set windows-shell := ["powershell.exe", "-NoLogo", "-NoProfile", "-Command"]

export CARGO_TARGET_DIR := justfile_directory() / "target"
export CARGO_BUILD_JOBS := env("CARGO_BUILD_JOBS", "6")

run := "cargo run --release --locked --quiet --"

default:
    @just --list

lint:
    cargo fmt --check
    cargo clippy --release --locked --all-targets -- -D warnings
    systemd-analyze --user verify systemd/*

test:
    cargo test --release --locked

check: lint test

build:
    cargo build --release --locked

units: build
    mkdir -p ~/Documents/data/fris/log
    systemctl --user link {{ justfile_directory() }}/systemd/*
    systemctl --user daemon-reload

plan *args:
    {{ run }} plan {{ args }}

paths entity:
    {{ run }} paths {{ entity }}

tables *args:
    {{ run }} build {{ args }}

changes:
    {{ run }} changes

fetch *args:
    {{ run }} fetch {{ args }}

harvest *args:
    {{ run }} harvest {{ args }}

spawn *args:
    {{ run }} spawn {{ args }}

status:
    {{ run }} status

bw rate:
    {{ run }} bandwidth {{ rate }}

pause:
    {{ run }} pause

resume:
    {{ run }} resume

stop:
    {{ run }} stop
