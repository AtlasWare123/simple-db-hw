#!/bin/sh
git tag -d lab5
git push origin :refs/tags/lab5
git tag -a lab5 -m 'finish lab5'
git push origin master --tags
