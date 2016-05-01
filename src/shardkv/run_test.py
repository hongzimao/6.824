# /usr/bin/env python

import os

num_test = 20

for i in range(num_test):
	os.system('go test')
	#os.system('go test -test.v -run "TestUnreliable2"')
