#!/bin/sh
git tag -d lab6
git push origin :refs/tags/lab6
git tag -a lab6 -m 'finish lab6'
git push origin master --tags
