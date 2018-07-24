#!/bin/sh
git tag -d lab4
git push origin :refs/tags/lab4
git tag -a lab4 -m 'finish lab4'
git push origin master --tags
