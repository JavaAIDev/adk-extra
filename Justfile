#!/usr/bin/env just --justfile

run-code-format:
    mvn com.spotify.fmt:fmt-maven-plugin:format
