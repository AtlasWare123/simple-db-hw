#!/bin/sh
git tag -d lab1
git push origin :refs/tags/lab1
git tag -a lab1 -m 'finish lab1'
git push origin master --tags
