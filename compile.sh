#!/bin/bash --


if hash mvn 2>/dev/null; then
	cd pinkelephant
	echo "Compiling..."
	mvn assembly:assembly
	cd ..
    else
        echo "Oops! mvn is not installed. You can use the compiled jar found in /target"
    fi
