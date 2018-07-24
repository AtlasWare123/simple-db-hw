#!/bin/sh
git tag -d lab2
git push origin :refs/tags/lab2
git tag -a lab2 -m 'finish lab2'
git push origin master --tags
