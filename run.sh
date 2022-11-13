#!/usr/bin/env bash

sbt -J-Xmx12G -J-Xms12G -J-XX:+UseG1GC run
