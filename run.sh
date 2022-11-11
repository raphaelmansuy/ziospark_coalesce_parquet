#!/usr/bin/env bash
sbt -J-Xmx8G -J-Xms8G -J-XX:+UseG1GC run
