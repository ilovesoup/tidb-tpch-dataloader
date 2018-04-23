#!/bin/sh

find "$1"* | xargs -I {} echo "" >> {}
find "$1"* | xargs -I {} tail -n +2 {} >> "$1".csv