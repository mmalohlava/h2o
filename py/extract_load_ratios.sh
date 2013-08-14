#!/bin/bash

grep "Loaded" sandbox/* | sed -e 's/.*ER: //' | grep -v 100 | cut -d\( -f2 | cut -d\) -f1 | sort

