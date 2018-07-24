#!/bin/sh
git tag -d lab3
git push origin :refs/tags/lab3
git tag -a lab3 -m 'finish lab3'
git push origin master --tags
