#!/bin/sh
i=0
while [ $i -lt 100 ]; do
	  echo "Testing ------ " $i
	    go test
	      let i+=1
      done
