#!/usr/bin/env bash

ssh -N -L 27017:pcp1.ccr.buffalo.edu:27017 -L 3306:ub-openxdmod-dev-db.ccr.xdmod.org:3306 adkofke@rush.ccr.buffalo.edu
