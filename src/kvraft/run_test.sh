#!/bin/sh
i=0
while [ $i -lt 10 ]; do
	  echo "Testing ------ " $i
	    go test
	      let i+=1
      done
